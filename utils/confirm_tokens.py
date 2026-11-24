"""ak-dixt (2026-11-23) SECURITY C-4 — server-signed destructive-
confirm tokens.

Replaces the pre-ak-dixt client-asserted `confirmed_tools: [str]`
list with HMAC-signed tokens that bind EACH confirmation to:
  * tool name
  * args hash (SHA-256 of sorted-keys JSON — same args required on
    the retry call, otherwise the user confirmed a DIFFERENT call)
  * user_id (prevents cross-user replay even in the single-user app
    — hardens the surface for any future multi-user shape)
  * expiry timestamp (default 300s TTL — user has 5 minutes to
    confirm; beyond that, server re-prompts)
  * random nonce (prevents issuing an identical token twice for
    identical (tool, args, user) — enables one-time-use dedup)

Wire format (token string):
    base64urlsafe(payload_json) + "." + hex(hmac_sha256(payload_json))

Payload JSON:
    {
      "tool": "<tool_name>",
      "args_hash": "<sha256 hex>",
      "uid": "<firebase_id>",
      "exp": <unix_ts int>,
      "nonce": "<hex>"
    }

Fail-closed invariants:
  * No AK_CONFIRM_SECRET in env → `issue_confirm_token` returns None,
    `verify_confirm_token` returns False. Legacy `confirmed_tools`
    path continues to work via the dual-accept gate in
    agentService — the destructive confirmation UX is NOT bricked,
    just downgraded to the pre-ak-dixt shape. Loud WARN on both
    issue + verify so operator sees the missing-config state.
  * Any parse/signature/user/args/expiry mismatch → False.
  * One-time-use (ak-erkm): a verified or revoked token's payload
    nonce is recorded with ONE atomic INSERT into the confirmTokenStates
    table (unique nonce; utils/confirm_token_store.py), so a replay
    fails in EVERY gunicorn worker. DB error -> refused (fail-closed).
    Outside a Flask app context (unit tests / CLI) the in-process
    LRU-bounded dict below is used instead.

Related: §23 #5 destructive-gate (confirm-id echo pattern).
Precedent: ak-x74g WARN→verify_only Firebase migration dual-accept
shape mirrored here (legacy name-list path logs with [AK-DIXT-
LEGACY] prefix for the 7-day warn-log-zero window).
"""
from __future__ import annotations

import base64
import hashlib
import hmac
import json
import logging
import os
import secrets
import threading
import time
from collections import OrderedDict
from typing import Any, Dict, Optional


_LOG = logging.getLogger(__name__)

_SECRET_ENV_NAME = "AK_CONFIRM_SECRET"
_DEFAULT_TTL_S = 300
_NONCE_BYTES = 16

# One-time-use dedup — bounded LRU-ish. Keyed on the first 16 hex
# chars of sha256(token). Values are the token's `exp` timestamp.
_CONSUMED_CAP = 10_000
_consumed: "OrderedDict[str, int]" = OrderedDict()
_consumed_lock = threading.Lock()


def _get_secret() -> Optional[bytes]:
    """Read AK_CONFIRM_SECRET from env. Returns None if unset or
    empty — callers must treat None as fail-closed (no mint, no
    verify). The dual-accept legacy path in the gate keeps the UX
    alive while this is unset."""
    raw = os.environ.get(_SECRET_ENV_NAME, "")
    if not raw:
        return None
    return raw.encode("utf-8")


def _args_hash(args: Optional[Dict[str, Any]]) -> str:
    """Stable hash of a tool-call args dict. sort_keys so dict-order
    changes between issue + verify don't tamper the hash. `args=None`
    hashes to a fixed string so a call with explicitly-None args
    binds identically on both sides."""
    if args is None:
        payload = "null"
    else:
        try:
            payload = json.dumps(args, sort_keys=True, default=str)
        except Exception:
            # Unserializable args → treat as hash miss; caller will
            # fail verification + prompt afresh.
            return ""
    return hashlib.sha256(payload.encode("utf-8")).hexdigest()


def _b64url_encode(raw: bytes) -> str:
    return base64.urlsafe_b64encode(raw).rstrip(b"=").decode("ascii")


def _b64url_decode(encoded: str) -> bytes:
    pad = "=" * (-len(encoded) % 4)
    return base64.urlsafe_b64decode(encoded + pad)


def issue_confirm_token(
    tool_name: str,
    args: Optional[Dict[str, Any]],
    user_id: str,
    ttl_s: int = _DEFAULT_TTL_S,
) -> Optional[str]:
    """Mint a server-signed destructive-confirm token.

    Returns None if AK_CONFIRM_SECRET is unset (fail-closed — the
    legacy `confirmed_tools` dual-accept path covers the UX in this
    degraded state). Non-None callers must include the token in the
    `confirm` SSE event payload (key `confirm_id`) so the FE can
    echo it on the retry turn in `confirmed_calls: [token]`.
    """
    secret = _get_secret()
    if secret is None:
        _LOG.warning(
            "[AK-DIXT] AK_CONFIRM_SECRET unset — destructive "
            "confirm tokens cannot be issued (legacy name-list "
            "path still accepted). Set the env var to enable "
            "server-signed confirmation."
        )
        return None
    if not isinstance(tool_name, str) or not tool_name:
        return None
    if not isinstance(user_id, str) or not user_id:
        return None
    if not isinstance(ttl_s, int) or ttl_s <= 0:
        ttl_s = _DEFAULT_TTL_S

    payload = {
        "tool": tool_name,
        "args_hash": _args_hash(args),
        "uid": user_id,
        "exp": int(time.time()) + ttl_s,
        "nonce": secrets.token_hex(_NONCE_BYTES),
    }
    payload_json = json.dumps(payload, sort_keys=True, separators=(",", ":"))
    payload_bytes = payload_json.encode("utf-8")
    sig = hmac.new(secret, payload_bytes, hashlib.sha256).hexdigest()
    return f"{_b64url_encode(payload_bytes)}.{sig}"


def _prune_consumed_locked(now: int) -> None:
    """Drop expired entries + enforce cap. Caller holds the lock."""
    # Expiry sweep
    expired = [k for k, exp in _consumed.items() if exp <= now]
    for k in expired:
        _consumed.pop(k, None)
    # LRU cap — drop oldest if over cap
    while len(_consumed) > _CONSUMED_CAP:
        _consumed.popitem(last=False)


def _mark_used(nonce: str, uid: str, tool: str, exp: int, state: str) -> bool:
    """ak-erkm: atomically move a token from live to used. True iff THIS
    call did it. With an app context the state lives in the DB
    (confirmTokenStates, unique nonce) so it holds across gunicorn
    workers; outside one (unit tests / CLI) the in-process set is used."""
    from utils.confirm_token_store import current_store
    store = current_store()
    if store is not None:
        return store.mark(nonce, uid, tool, exp, state)
    now = int(time.time())
    with _consumed_lock:
        _prune_consumed_locked(now)
        if nonce in _consumed:
            return False
        _consumed[nonce] = exp
    return True


def _is_used(nonce: str):
    """ak-erkm: True / False, or None if the shared store can't be read."""
    from utils.confirm_token_store import current_store
    store = current_store()
    if store is not None:
        return store.is_used(nonce)
    with _consumed_lock:
        _prune_consumed_locked(int(time.time()))
        return nonce in _consumed


def verify_confirm_token(
    token: str,
    expected_tool: str,
    expected_args: Optional[Dict[str, Any]],
    user_id: str,
) -> bool:
    """Verify a confirm_id against expected tool + args + user.

    Returns True iff ALL of:
      * AK_CONFIRM_SECRET is configured
      * token parses as `<b64url-payload>.<hex-sig>`
      * HMAC signature matches (constant-time compare)
      * payload JSON parses
      * payload.tool == expected_tool
      * payload.args_hash == hash(expected_args)
      * payload.uid == user_id
      * payload.exp > now()
      * token-digest not already in the consumed set (within TTL)
    Side-effect: on True, records token-digest in the consumed set
    with the token's `exp` as the TTL anchor. Subsequent verifies
    of the same token return False (replay rejection).
    """
    secret = _get_secret()
    if secret is None:
        _LOG.warning(
            "[AK-DIXT] AK_CONFIRM_SECRET unset — verify_confirm_"
            "token returning False (fail-closed). Legacy "
            "confirmed_tools path still covers the UX."
        )
        return False
    if not isinstance(token, str) or "." not in token:
        return False

    try:
        payload_b64, sig_hex = token.rsplit(".", 1)
    except ValueError:
        return False

    try:
        payload_bytes = _b64url_decode(payload_b64)
    except (ValueError, base64.binascii.Error):
        return False

    expected_sig = hmac.new(
        secret, payload_bytes, hashlib.sha256,
    ).hexdigest()
    if not hmac.compare_digest(sig_hex, expected_sig):
        return False

    try:
        payload = json.loads(payload_bytes.decode("utf-8"))
    except (ValueError, UnicodeDecodeError):
        return False

    if not isinstance(payload, dict):
        return False

    if payload.get("tool") != expected_tool:
        return False
    if payload.get("uid") != user_id:
        return False
    if payload.get("args_hash") != _args_hash(expected_args):
        return False

    exp = payload.get("exp")
    if not isinstance(exp, int):
        return False
    now = int(time.time())
    if exp <= now:
        return False

    # One-time-use: register the token's nonce (canonical, lives
    # INSIDE the HMAC-protected payload).
    #
    # ak-dixt reviewer MAJOR (2026-11-23): keying this on
    # sha256(token) was BYPASSABLE via base64url malleability —
    # when the payload byte-length is not a multiple of 3, the
    # trailing b64url character has unused bits, so multiple
    # distinct token STRINGS decode to the SAME payload bytes +
    # verify under the same HMAC sig. Attacker could issue →
    # consume → re-encode trailing char → bypass the dedup set
    # (digest differs because the token string differs). The
    # nonce lives inside the HMAC-protected payload so it is
    # CANONICAL: any malleable re-encoding of the token-string
    # that still HMAC-verifies decodes to the same payload and
    # therefore the same nonce. Dedup-key material unchanged
    # across any legit attack-shape re-encoding.
    nonce = payload.get("nonce")
    if not isinstance(nonce, str) or not nonce:
        return False
    # ak-erkm: atomic, cross-worker (DB) check-and-mark.
    return _mark_used(nonce, user_id, expected_tool, exp, "consumed")


def _decode_live(token, user_id):
    """ak-2ye9: (tool, nonce, exp) for a token that is signed, for this
    user, unexpired and not yet consumed IN THIS PROCESS; else None.
    Does NOT check tool/args and does not consume. Never raises."""
    try:
        secret = _get_secret()
        if secret is None or not isinstance(token, str) or "." not in token:
            return None
        payload_b64, sig_hex = token.rsplit(".", 1)
        payload_bytes = _b64url_decode(payload_b64)
        expected_sig = hmac.new(secret, payload_bytes, hashlib.sha256).hexdigest()
        if not hmac.compare_digest(sig_hex, expected_sig):
            return None
        payload = json.loads(payload_bytes.decode("utf-8"))
        if not isinstance(payload, dict) or payload.get("uid") != user_id:
            return None
        tool, exp, nonce = payload.get("tool"), payload.get("exp"), payload.get("nonce")
        if not isinstance(tool, str) or not isinstance(exp, int):
            return None
        if not isinstance(nonce, str) or not nonce:
            return None
        now = int(time.time())
        if exp <= now:
            return None
        used = _is_used(nonce)
        if used:
            return None
        # used is None: the shared store couldn't be read. Report the
        # token as live (peek -> the bound-args gate still applies;
        # revoke -> its own mark fails closed).
        return tool, nonce, exp
    except Exception:
        return None


def peek_confirm_tool(token: str, user_id: str) -> Optional[str]:
    """ak-2ye9: tool name of a LIVE token, without consuming it. Used to
    find which tools have an Apply pending this turn, so those calls are
    always held to the token's bound args."""
    live = _decode_live(token, user_id)
    return live[0] if live else None


def revoke_confirm_token(token: str, user_id: str) -> Optional[str]:
    """ak-2ye9: consume a LIVE token without executing anything, so it
    can never be applied later. Used when the user cancels a proposal
    (`cancelled_calls`) and when an Apply turn ends without the bound
    call having run ("not_executed").

    Checks signature + uid + expiry + not-already-consumed (NOT tool or
    args: the caller doesn't have them). Returns the token's tool name
    on success, None otherwise. Never raises.

    ak-erkm: revocation and one-time-use share the DB-backed state, so a
    revoked token is dead in EVERY gunicorn worker.
    """
    live = _decode_live(token, user_id)
    if not live:
        return None
    tool, nonce, exp = live
    if not _mark_used(nonce, user_id, tool, exp, "revoked"):
        return None
    return tool


def _reset_consumed_for_tests() -> None:
    """Test-only hook — flush the in-memory consumed dict. Not
    exported in a wildcard import; only test suites touch this."""
    with _consumed_lock:
        _consumed.clear()


__all__ = [
    "issue_confirm_token",
    "verify_confirm_token",
    "revoke_confirm_token",
    "peek_confirm_tool",
]
