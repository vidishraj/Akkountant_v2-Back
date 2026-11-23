"""ak-vue6 Phase 1: UNIX-domain-socket IPC between the Kite WS
daemon (producer) and the Flask SSE / single-symbol endpoints
(consumers).

## Overseer-approved spec

Socket path: ``/run/akkountant/kite-ltp.sock`` (mode 0600 opc:opc).
Wire format: JSON lines over ``SOCK_STREAM``.

```
Request:   {"op":"get","sym":"RELIANCE"}\n
Response:  {"sym":"RELIANCE","ltp":1234.5,"ts":1700000000.0}\n
           -- or --
           {"sym":"RELIANCE","error":"not_watched"}\n

Request:   {"op":"get_all"}\n
Response:  {"ticks":{"RELIANCE":{"ltp":1234.5,"ts":...}, ...},
            "state":"ok"}\n

Request:   {"op":"state"}\n
Response:  {"state":"ok"|"reconnect_required"|"init","detail":"..."}\n
```

``SUBSCRIBE`` op is reserved for a future Phase (watchlist /
alert-targets wiring). Today the daemon subscribes to the union
of held instruments on boot + post-sync.

## Why UNIX-socket not Redis

Single-user single-worker runtime (Overseer Q6 ✓) — a broker
adds operational surface without buying anything. The daemon is
the SOLE WRITER to the tick dict; consumers only READ snapshots,
so there is zero backpressure risk by construction (LTP is
latest-per-symbol state, never a stream).

Shape is forward-compatible: if the app ever scales to N
workers, the same client interface can target Redis HSET /
HMGET with ZERO change to callers — the ``KiteLtpClient``
abstraction isolates the protocol detail here.

## Security

0600 opc:opc — only the service user can read. No auth on the
wire protocol itself (relies on filesystem perms). Do NOT expose
over TCP without adding token-auth.

## Blast radius

Zero. New standalone module; no imports elsewhere until the
controller + daemon wire it up.
"""
from __future__ import annotations

import json
import os
import socket
import time
from typing import Optional


# Overseer-approved socket location. Infra (akkountant_infra)
# is responsible for pre-creating ``/run/akkountant/`` with
# opc:opc ownership via tmpfiles.d. Daemon falls back to
# ``$XDG_RUNTIME_DIR`` in dev if the canonical path is
# unwritable.
DEFAULT_SOCKET_PATH = "/run/akkountant/kite-ltp.sock"


# IPC protocol operation names (the ``op`` field value).
OP_GET = "get"
OP_GET_ALL = "get_all"
OP_STATE = "state"

# Response state strings (daemon-reported health).
STATE_OK = "ok"
STATE_INIT = "init"            # daemon warming up (first WS connect pending)
STATE_RECONNECT = "reconnect_required"  # 6 AM IST expiry — needs operator action

# Wire-level knobs. Keep small; one-shot roundtrips.
_RECV_BUFSIZE = 8192
_CLIENT_TIMEOUT_SECONDS = 2.0


class KiteLtpIpcError(Exception):
    """Wraps any non-recoverable IPC failure (socket missing,
    daemon unreachable, protocol mismatch). Callers map this to
    a 503-ish response — the daemon is down / booting."""


class KiteLtpClient:
    """Short-lived connection per-request — mirrors the
    tick-snapshot read pattern. Suitable for the single-symbol
    endpoint and the SSE poll loop.

    A long-lived connection could in theory be more efficient
    for the SSE loop (one TCP-ish setup, many reads), but the
    daemon ships a request/response wire today and SSE ticks at
    1 Hz — the overhead of a per-poll open-close is < 100µs on
    a UNIX-domain socket, dominated by the tick cadence itself.
    """

    def __init__(self, socket_path: str = DEFAULT_SOCKET_PATH):
        self._socket_path = socket_path

    def get(self, sym: str) -> Optional[dict]:
        """Fetch the latest LTP for a single symbol.

        Returns:
            ``{"sym": ..., "ltp": ..., "ts": ...}`` on hit,
            ``None`` if the symbol is not currently watched.

        Raises:
            KiteLtpIpcError on daemon-unreachable / malformed
            response.
        """
        payload = {"op": OP_GET, "sym": sym}
        resp = self._roundtrip(payload)
        if "error" in resp:
            # Explicit 'not_watched' is a known-shape miss, not an IPC
            # error. Return None so callers can 404 cleanly.
            if resp.get("error") == "not_watched":
                return None
            raise KiteLtpIpcError(f"daemon error: {resp.get('error')}")
        return resp

    def get_all(self) -> dict:
        """Fetch the full tick snapshot in one roundtrip.

        Returns:
            ``{"ticks": {sym: {...}, ...}, "state": "ok"|...}``.
            Empty ``ticks`` when the daemon has not received a
            first tick yet (still OK — ``state`` carries that).
        """
        return self._roundtrip({"op": OP_GET_ALL})

    def state(self) -> dict:
        """Return the daemon's health shape without touching ticks.
        Useful for health probes / reconnect-required signals.
        """
        return self._roundtrip({"op": OP_STATE})

    def _roundtrip(self, payload: dict) -> dict:
        """One request / one response. Connection closed after."""
        try:
            sock = socket.socket(socket.AF_UNIX, socket.SOCK_STREAM)
            sock.settimeout(_CLIENT_TIMEOUT_SECONDS)
            sock.connect(self._socket_path)
        except (FileNotFoundError, ConnectionRefusedError) as exc:
            # Daemon is down / not yet started. Controllers turn
            # this into a 503-shaped response.
            raise KiteLtpIpcError(
                f"kite-ltp daemon unreachable at {self._socket_path}: "
                f"{type(exc).__name__}"
            )
        except OSError as exc:
            raise KiteLtpIpcError(
                f"kite-ltp socket error: {type(exc).__name__}"
            )

        try:
            wire = (json.dumps(payload) + "\n").encode("utf-8")
            sock.sendall(wire)
            # Request is a single JSON line; response is a single
            # JSON line. Read until newline arrives — guards against
            # partial recvs on larger get_all payloads.
            chunks = []
            while True:
                chunk = sock.recv(_RECV_BUFSIZE)
                if not chunk:
                    break
                chunks.append(chunk)
                if b"\n" in chunk:
                    break
            data = b"".join(chunks)
            if not data:
                raise KiteLtpIpcError("empty response from daemon")
            # Single JSON line — strip trailing newline, parse.
            line = data.split(b"\n", 1)[0]
            try:
                return json.loads(line.decode("utf-8"))
            except (UnicodeDecodeError, json.JSONDecodeError) as exc:
                raise KiteLtpIpcError(
                    f"malformed daemon response: {type(exc).__name__}"
                )
        except socket.timeout:
            raise KiteLtpIpcError(
                "kite-ltp daemon timed out — slow or stuck"
            )
        finally:
            try:
                sock.close()
            except OSError:
                pass


def encode_response(payload: dict) -> bytes:
    """Daemon-side helper: serialize a response dict into the
    newline-terminated wire format. Keeps protocol details in
    one module so a future protocol change (CBOR / msgpack /
    streaming event framing) is a single-file edit.
    """
    return (json.dumps(payload, separators=(",", ":")) + "\n").encode(
        "utf-8"
    )


def decode_request(line: bytes) -> dict:
    """Daemon-side helper: parse a single request line.

    Raises:
        ValueError on malformed input — daemon responds with
        ``{"error": "bad_request"}`` + closes the connection.
    """
    try:
        text = line.decode("utf-8").rstrip("\r\n")
    except UnicodeDecodeError:
        raise ValueError("non-utf8 request")
    if not text:
        raise ValueError("empty request")
    try:
        obj = json.loads(text)
    except json.JSONDecodeError as exc:
        raise ValueError(f"malformed JSON: {exc}")
    if not isinstance(obj, dict):
        raise ValueError("request must be a JSON object")
    if "op" not in obj:
        raise ValueError("missing 'op' field")
    return obj


def now_ts() -> float:
    """Shared timestamp source so daemon + tests agree on epoch
    seconds (float with sub-second resolution). Re-exported so
    tests don't need to mock datetime.
    """
    return time.time()
