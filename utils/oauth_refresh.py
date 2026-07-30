"""ak-oauth-refresh: Anthropic OAuth refresh-token helper.

BUG (per Lead's dispatch hq-wisp-nnvkzy → GREENLIGHT hq-wisp-*):

  The bundled Claude CLI (Bun-embedded, msal-node cache) refreshes
  its access_token implicitly when it hits a 401. Under interactive
  invocation that works fine. Under our HEADLESS subprocess mode
  (rate.* + mailProcessor systemd units invoking `claude` one-shot)
  the implicit refresh appears NOT to fire — leaving the fleet at
  the mercy of the token cache's `expiresAt` field. When that
  expiresAt lapses, every subsequent `claude` subprocess 401s and
  the ingest pipeline goes dark.

  Report hq-wisp-* documented the token snapshot at read time:
    expiresAt = 1785418187947 = 2026-07-30 13:29:47 UTC
    subscriptionType = max
    refreshToken PRESENT (108 chars, valid)

FIX:

  Deterministic Python-side refresh:
    1. Read credentials.json.
    2. If `(expiresAt - now) < window_seconds`, POST the refresh
       request to the derived TOKEN_URL with the cached
       refresh_token.
    3. Atomic-rewrite credentials.json with the new
       accessToken / refreshToken / expiresAt while preserving
       every other field (scopes, subscriptionType, rateLimitTier,
       …).
    4. Enforce 0o600 file mode across the write (mirrors
     ak-nyd v2 per_file_passwords hardening — same threat model:
       plaintext secret at rest, needs owner-only mode).

  Wire this into systemd via `ExecStartPre=python3 -m
  utils.oauth_refresh` on rate.*/mailProcessor units. A belt-and-
  braces hourly systemd timer catches units that stay up past the
  window.

CONFIG (all overridable via env for tests + prod flexibility):

  AK_CLAUDE_CREDENTIALS_PATH  →  path to credentials.json
                                 (default: ~/.claude/.credentials.json)
  CLAUDE_CODE_CUSTOM_OAUTH_URL →  refresh POST endpoint
                                 (default: platform.claude.com …/oauth/token)
  CLAUDE_CODE_OAUTH_CLIENT_ID →  client_id sent with the refresh
                                 (default: 9d1c250a-…-1962f5e)

  These match the env-var names the bundled CLI itself recognises
  (strings extracted from the Bun bundle during the Q1 investigation)
  so operators wanting to point at a staging OAuth endpoint set the
  same var once and both the CLI and this helper follow.

WIRE-SHAPE PROVENANCE (v2/v3, from Lead verify hq-wisp-*):

  The default URL and POST body were extracted directly from the
  bundled CLI binary (Bun bundle strings). v3 REVISED the URL
  default after infra found version drift between prod and Desktop
  CLIs:

    - PROD CLI (SDK 0.1.11 / CLI 2.0.57, Dec-4-2025 build):
        strings ...claude | grep '/v1/oauth/token'
        → https://console.anthropic.com/v1/oauth/token   [ONLY]
    - Desktop CLI (newer bundle we grepped in v1/v2 verify):
        → https://platform.claude.com/v1/oauth/token     [ONLY]

  Anthropic renamed console.anthropic.com → platform.claude.com in
  a newer CLI. Tokens are host-scoped: a token minted by the older
  CLI (as prod's is — expiresAt=2026-07-30 13:29:47 UTC, minted
  2026-07-24 UTC) MUST be refreshed against console.anthropic.com;
  the newer host won't recognise it. v3 default flipped to match
  prod's actual CLI. Operators bump the default (or set
  CLAUDE_CODE_CUSTOM_OAUTH_URL) once prod SDK is upgraded.

  Body shape (unchanged v2 → v3):
    grant_type:"refresh_token", refresh_token:T,
    client_id:OB().CLIENT_ID, scope:PgR.join(" ")
  (v1 sent the first three fields; v2 adds the scope field to
   match the CLI wire shape verbatim; v3 keeps that shape.)

STATUS ladder (mirror of ak-1rh reprocess_status shape so callers /
systemd log parsers can grep for a single field):

  - "still_valid":     inside the window, no refresh attempted.
  - "refreshed":       POST succeeded, credentials.json rewritten.
  - "failed":          POST attempted but failed (network / 4xx /
                       parse). credentials.json NOT touched (partial
                       writes are forbidden — see _refresh).
  - "no_refresh_token": cache has no refresh_token — nothing we can
                       do; operator must re-run `claude login`.
  - "missing_credentials": credentials.json not readable / not JSON.

Pure-Python (stdlib only) so it's testable without booting flask /
SQLAlchemy AND runs in the minimal systemd ExecStartPre environment
without pip installs.
"""

from __future__ import annotations

import json
import os
import re
import sys
import time
import urllib.error
import urllib.request
from typing import Any, Callable, Optional


# ── Config knobs (env-overridable) ──────────────────────────────────

_ENV_CREDENTIALS_PATH = "AK_CLAUDE_CREDENTIALS_PATH"
_ENV_TOKEN_URL = "CLAUDE_CODE_CUSTOM_OAUTH_URL"
_ENV_CLIENT_ID = "CLAUDE_CODE_OAUTH_CLIENT_ID"

_DEFAULT_CREDENTIALS_REL = os.path.join(".claude", ".credentials.json")

# v3 (per Lead's dispatch): infra found version drift between prod
# and Desktop CLIs. Anthropic renamed console.anthropic.com →
# platform.claude.com in a newer CLI (SDK 0.1.11 / CLI 2.0.57
# Dec-4-2025 has console.anthropic.com; the newer Desktop bundle
# has platform.claude.com). The PROD token was minted 2026-07-24
# by the older CLI so it's a console.anthropic.com-issued token
# and MUST be refreshed against that same host — the newer host
# won't recognise it.
#
# Default = the URL the prod CLI actually uses. Operators / this
# module later flip the default (or set CLAUDE_CODE_CUSTOM_OAUTH_URL)
# once the prod SDK is upgraded.
_DEFAULT_TOKEN_URL = "https://console.anthropic.com/v1/oauth/token"

_DEFAULT_CLIENT_ID = "9d1c250a-e61b-44d9-88ed-5944d1962f5e"

# 5-minute default refresh window. Chosen to give systemd time to
# spawn the next unit before the token would 401 under load.
_DEFAULT_WINDOW_SECONDS = 300

_FILE_MODE = 0o600  # owner rw only — mirrors ak-nyd v2
_DIR_MODE = 0o700   # owner rwx only — v4 Minor A parent-dir chmod

_OAUTH_KEY = "claudeAiOauth"

# v4 Minor B: HTTP error body sanitization.
#
# The prior _default_http_post embedded up to 500 chars of raw
# response body in the OAuthRefreshError message so operators could
# debug refusals from journalctl. That's a leak surface: a
# misconfigured OAuth server (or an attacker-controlled endpoint
# reached via a hostile env var) could echo the request body — which
# contains refresh_token — straight back into the log stream.
#
# v4 replaces raw-body embedding with a two-step scrub:
#   1. Regex-strip any token-shape blob (>=40 chars of alnum/dash/
#      underscore) — catches refresh_token, access_token, JWT-like
#      payloads, opaque bearer tokens.
#   2. Extract just the OAuth-standard `"error":"…"` field via a
#      strict short-label regex (RFC 6749 §5.2 error codes are
#      lowercase snake, ≤32 chars).
# Falls back to a short static "unrecognized" label if no error
# field is present. Truncates to a hard cap as belt-and-braces.
_SECRET_SHAPE_RE = re.compile(r'[A-Za-z0-9_\-]{40,}')
_OAUTH_ERROR_LABEL_RE = re.compile(r'"error"\s*:\s*"([a-z0-9_]{1,32})"')
_ERROR_BODY_CAP = 200


def _sanitize_error_body(raw: str) -> str:
    """v4 Minor B: return a short label safe to embed in a log line.
    Never returns anything that could contain a token-shape secret.

    Priority: known OAuth error code → 'unrecognized:<truncated>'
    with secrets redacted. Empty body → 'no-body'.
    """
    if not raw:
        return "no-body"
    # Look for the standard OAuth error field first — this is what
    # operators actually need to distinguish invalid_grant (bad
    # refresh_token, needs re-login) from invalid_client (config bug).
    match = _OAUTH_ERROR_LABEL_RE.search(raw)
    if match:
        return match.group(1)
    # No OAuth error field — return a scrubbed + truncated fragment
    # so at least the shape of the failure is visible.
    scrubbed = _SECRET_SHAPE_RE.sub("<REDACTED>", raw)
    if len(scrubbed) > _ERROR_BODY_CAP:
        scrubbed = scrubbed[: _ERROR_BODY_CAP] + "…"
    return f"unrecognized:{scrubbed}"


# ── Path / credential loading ───────────────────────────────────────


def _default_credentials_path() -> str:
    return os.path.join(os.path.expanduser("~"), _DEFAULT_CREDENTIALS_REL)


def _resolve_credentials_path(path: Optional[str] = None) -> str:
    if path:
        return path
    env_path = os.environ.get(_ENV_CREDENTIALS_PATH, "").strip()
    if env_path:
        return env_path
    return _default_credentials_path()


def _resolve_token_url() -> str:
    return os.environ.get(_ENV_TOKEN_URL, "").strip() or _DEFAULT_TOKEN_URL


def _resolve_client_id() -> str:
    return os.environ.get(_ENV_CLIENT_ID, "").strip() or _DEFAULT_CLIENT_ID


def _load_credentials(path: str) -> Optional[dict]:
    """Return the parsed credentials dict, or None on missing / unparseable
    file. Never raises — callers switch on None."""
    try:
        # Resolve symlink so we rewrite the underlying file (matches
        # ~/.claude/.credentials.json → ~/.claude-accounts/<x>/… layout
        # observed in prod). os.path.realpath is safe on non-symlinks.
        resolved = os.path.realpath(path) if os.path.exists(path) else path
        with open(resolved, "r", encoding="utf-8") as f:
            data = json.load(f)
        if not isinstance(data, dict):
            return None
        return data
    except (OSError, json.JSONDecodeError):
        return None


def _save_credentials(path: str, data: dict) -> None:
    """Atomic rewrite of credentials.json with 0o600 mode.

    Mirrors the ak-nyd v2 pattern: write to `.tmp`, chmod tmp BEFORE
    rename so the file is 0o600 the instant it becomes visible under
    the target path, atomic rename, belt-and-braces chmod post-rename.
    Follows symlinks so we update the real file (not just the link).

    v4 Minor A: also cascade a 0o700 chmod onto the parent directory
    after the file is in place. Prevents a loose parent (e.g., the
    CLI created it with the process umask) from neutering the 0o600
    file mode — anyone with search permission on the parent can
    open() a file inside regardless of its own mode. Non-fatal
    (wrapped in try) so a parent we don't own doesn't blow up the
    refresh.
    """
    resolved = os.path.realpath(path) if os.path.exists(path) else path
    tmp = f"{resolved}.tmp"
    with open(tmp, "w", encoding="utf-8") as f:
        json.dump(data, f, indent=2, sort_keys=True)
        f.write("\n")
    try:
        os.chmod(tmp, _FILE_MODE)
    except OSError:
        pass  # belt-and-braces chmod below covers it
    os.replace(tmp, resolved)
    try:
        os.chmod(resolved, _FILE_MODE)
    except OSError:
        pass
    # v4 Minor A: parent dir 0o700 — mirrors ak-nyd v2 hardening.
    parent = os.path.dirname(resolved)
    if parent:
        try:
            os.chmod(parent, _DIR_MODE)
        except OSError:
            pass  # non-fatal


# ── Expiry math ─────────────────────────────────────────────────────


def _now_seconds(now: Optional[float]) -> float:
    return float(now) if now is not None else time.time()


def seconds_until_expiry(oauth: dict, *, now: Optional[float] = None) -> float:
    """Return seconds until `expiresAt` (ms since epoch in the credentials
    file). Negative if already expired. Defensive: 0.0 on missing /
    non-numeric expiresAt (treat as expired → force refresh)."""
    raw = oauth.get("expiresAt")
    try:
        expires_ms = float(raw)
    except (TypeError, ValueError):
        return 0.0
    return (expires_ms / 1000.0) - _now_seconds(now)


def is_expiring(oauth: dict, *, window_seconds: int = _DEFAULT_WINDOW_SECONDS,
                now: Optional[float] = None) -> bool:
    """True if the token is inside the refresh window (or already expired)."""
    return seconds_until_expiry(oauth, now=now) < window_seconds


# ── HTTP layer (stdlib urllib; injectable for tests) ────────────────


class OAuthRefreshError(Exception):
    """Raised internally by _default_http_post on failure. Callers see
    it collapsed to status='failed' in the result dict."""


def _default_http_post(url: str, body: dict) -> dict:
    """POST JSON body, parse JSON response. Raises OAuthRefreshError
    on any failure (network, HTTP 4xx/5xx, non-JSON body)."""
    payload = json.dumps(body).encode("utf-8")
    req = urllib.request.Request(
        url,
        data=payload,
        headers={
            "Content-Type": "application/json",
            "Accept": "application/json",
            "User-Agent": "akkountant-oauth-refresh/1",
        },
        method="POST",
    )
    try:
        with urllib.request.urlopen(req, timeout=15) as resp:
            raw = resp.read()
    except urllib.error.HTTPError as e:
        # v4 Minor B: sanitize the body before embedding — never
        # ship raw response bytes into the log stream (see
        # _sanitize_error_body docstring for the threat model).
        raw = ""
        try:
            raw = e.read().decode("utf-8", errors="replace")
        except Exception:  # pragma: no cover — defensive
            pass
        raise OAuthRefreshError(
            f"http_{e.code}: {_sanitize_error_body(raw)}"
        ) from e
    except urllib.error.URLError as e:
        raise OAuthRefreshError(f"network: {e.reason}") from e
    try:
        parsed = json.loads(raw.decode("utf-8"))
    except (UnicodeDecodeError, json.JSONDecodeError) as e:
        raise OAuthRefreshError(f"parse: {e}") from e
    if not isinstance(parsed, dict):
        raise OAuthRefreshError("parse: response is not a JSON object")
    return parsed


# ── The refresh flow ────────────────────────────────────────────────


def _extract_new_tokens(response: dict) -> dict:
    """Pull the three fields we need out of the OAuth response.
    Accepts both snake_case (Anthropic OAuth) and camelCase (defensive —
    some proxies rewrite) forms.

    Returns dict with:
      accessToken (str) — required
      refreshToken (str) — optional; falls back to caller's existing
                            token if server didn't rotate.
      expiresInSeconds (int) — required (used with `now` to compute
                                the new expiresAt in ms).
    """
    access = response.get("access_token") or response.get("accessToken")
    refresh = response.get("refresh_token") or response.get("refreshToken")
    expires_in = response.get("expires_in") or response.get("expiresIn")

    if not access or not isinstance(access, str):
        raise OAuthRefreshError("response missing access_token")
    if expires_in is None:
        raise OAuthRefreshError("response missing expires_in")
    try:
        expires_in_int = int(expires_in)
    except (TypeError, ValueError) as e:
        raise OAuthRefreshError(f"response expires_in not numeric: {e}") from e

    return {
        "accessToken": access,
        "refreshToken": refresh if isinstance(refresh, str) and refresh else None,
        "expiresInSeconds": expires_in_int,
    }


def _apply_new_tokens(oauth: dict, new: dict, *, now: Optional[float]) -> dict:
    """Return a new claudeAiOauth dict with the refreshed fields
    applied. Preserves all other fields (scopes, subscriptionType,
    rateLimitTier, …) verbatim."""
    updated = dict(oauth)  # shallow copy
    updated["accessToken"] = new["accessToken"]
    if new["refreshToken"]:
        updated["refreshToken"] = new["refreshToken"]
    # expiresAt in ms since epoch (matches the CLI's schema).
    updated["expiresAt"] = int(
        (_now_seconds(now) + new["expiresInSeconds"]) * 1000
    )
    return updated


def refresh_if_expiring(
    credentials_path: Optional[str] = None,
    *,
    now: Optional[float] = None,
    window_seconds: int = _DEFAULT_WINDOW_SECONDS,
    http_post: Optional[Callable[[str, dict], dict]] = None,
) -> dict:
    """Read credentials.json. If token is inside the refresh window,
    POST the refresh request and rewrite the file. Return a structured
    status dict either way.

    Structured return (see module docstring for status ladder):
      {
        "status": <str>,
        "expires_in_seconds": <int>,      # after any refresh
        "reason": <str-or-None>,          # populated on failure paths
        "path": <resolved credentials path>,
      }
    """
    path = _resolve_credentials_path(credentials_path)
    data = _load_credentials(path)
    if data is None or _OAUTH_KEY not in data or not isinstance(
        data.get(_OAUTH_KEY), dict
    ):
        return {
            "status": "missing_credentials",
            "expires_in_seconds": 0,
            "reason": f"credentials.json not readable at {path}",
            "path": path,
        }

    oauth = data[_OAUTH_KEY]
    remaining = seconds_until_expiry(oauth, now=now)
    if not is_expiring(oauth, window_seconds=window_seconds, now=now):
        return {
            "status": "still_valid",
            "expires_in_seconds": int(remaining),
            "reason": None,
            "path": path,
        }

    refresh_token = oauth.get("refreshToken")
    if not refresh_token or not isinstance(refresh_token, str):
        return {
            "status": "no_refresh_token",
            "expires_in_seconds": int(remaining),
            "reason": "cache has no refreshToken — re-run `claude login`",
            "path": path,
        }

    post = http_post or _default_http_post
    # ak-oauth-refresh v2 (per Lead's verify): the bundled CLI's
    # refresh POST body carries a `scope` field with the granted
    # scopes joined by spaces. Direct evidence from the Bun bundle:
    #   grant_type:"refresh_token", refresh_token:T,
    #   client_id:OB().CLIENT_ID, scope:PgR.join(" ")
    # Some OAuth servers key rate-limits / policy off the scope
    # string; omitting it risks a 400 on a stricter server-side
    # policy. We echo whatever's in the cache — empty string when
    # the cache has none (defensive; not observed in prod).
    scopes = oauth.get("scopes") if isinstance(oauth.get("scopes"), list) else []
    scope_str = " ".join(s for s in scopes if isinstance(s, str) and s)
    body = {
        "grant_type": "refresh_token",
        "refresh_token": refresh_token,
        "client_id": _resolve_client_id(),
        "scope": scope_str,
    }
    try:
        response = post(_resolve_token_url(), body)
        new = _extract_new_tokens(response)
    except OAuthRefreshError as e:
        # ZERO WRITES on failure — the whole point of the atomic
        # rewrite is that a bad refresh leaves the old credentials
        # intact for the next attempt / manual `claude login`.
        return {
            "status": "failed",
            "expires_in_seconds": int(remaining),
            "reason": str(e),
            "path": path,
        }

    data[_OAUTH_KEY] = _apply_new_tokens(oauth, new, now=now)
    _save_credentials(path, data)
    return {
        "status": "refreshed",
        "expires_in_seconds": new["expiresInSeconds"],
        "reason": None,
        "path": path,
    }


def refresh_now(
    credentials_path: Optional[str] = None,
    *,
    now: Optional[float] = None,
    http_post: Optional[Callable[[str, dict], dict]] = None,
) -> dict:
    """Force refresh regardless of remaining lifetime (systemd timer
    entrypoint). Equivalent to refresh_if_expiring with window_seconds
    = a huge number so the expiring check always fires."""
    return refresh_if_expiring(
        credentials_path,
        now=now,
        window_seconds=10**9,  # ~31 years — effectively "always refresh"
        http_post=http_post,
    )


# ── CLI entrypoint for systemd ExecStartPre ─────────────────────────


def _emit_json(result: dict) -> None:
    """Log the result as a single-line JSON event (grep-friendly, matches
    ak-n44 pipeline_logging emit style)."""
    event = {"event": "oauth_refresh_result", **result}
    print(json.dumps(event, sort_keys=True))


def _cli_main(argv: list) -> int:
    force = "--force" in argv or "--now" in argv
    result = refresh_now() if force else refresh_if_expiring()
    _emit_json(result)
    # Exit 0 on success OR still_valid (systemd shouldn't block the
    # unit from starting when the token is fine). Exit 1 on hard
    # failure so systemd's ExecStartPre policy can decide (usually
    # +/- prefix on the ExecStartPre line — operator's call).
    return 0 if result["status"] in {"refreshed", "still_valid"} else 1


if __name__ == "__main__":  # pragma: no cover
    sys.exit(_cli_main(sys.argv[1:]))
