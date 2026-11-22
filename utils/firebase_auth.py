"""ak-c8l: Firebase ID-token verification for the before_request
auth middleware.

Replaces the pre-ak-c8l raw ``X-Firebase-ID``-header trust with
``firebase_admin.auth.verify_id_token`` so the server validates the
JWT signature + audience + expiry rather than taking the client's
word on who they are. Closes the IDOR surface Infra confirmed
2026-08-21: "Fake header smoke test: HTTP 200 with data returned
for arbitrary X-Firebase-ID value → app treats fake UID as
authenticated".

## Rollout plan (per bead — "backward-compat middle-step")

The auth contract changes: pre-ak-c8l clients send a bare Firebase
UID (e.g. ``AwKnpKEPmEhQnpc2kJAnsEikOBK2``); post-ak-c8l clients
send a signed Firebase ID JWT (long base64 string of
``header.payload.signature``). The two shapes are distinguishable
at a glance:
  * JWT has exactly two ``.`` separators and >= ~500 chars.
  * Bare UID is a 28-char alphanumeric string, no dots.

This module runs in one of three modes (controlled by env vars):

  * ``FIREBASE_AUTH_MODE=verify_only`` (prod final state):
    ONLY accept verified JWTs. Bare-UID fallback is rejected with
    401. Set this after the FE has shipped the token-sending
    version and all clients are on the new contract.
  * ``FIREBASE_AUTH_MODE=verify_with_raw_uid_fallback`` (default,
    rollout cycle):
    Try JWT verify first. If the header doesn't look like a JWT,
    fall back to raw-UID trust with a WARN log so the operator can
    track the rollout. This is the backward-compat middle-step.
  * ``FIREBASE_AUTH_MODE=raw_uid_only`` (LOCAL / test):
    Skip verification entirely; treat the header as a bare UID.
    ONLY intended for LOCAL env where no Firebase credentials are
    plumbed. Never ship to prod with this.

Credentials file path:
  * ``FIREBASE_CREDENTIALS_PATH=/path/to/service-account.json``
  * File permissions enforced by operator (0o600 recommended, per
    the rest of the Google / Kite credential pattern).
  * If unset AND mode is not ``raw_uid_only``, init logs a loud
    WARN and falls back to ``raw_uid_only`` behaviour for this
    process. This prevents a broken deploy from locking every
    user out; the WARN is greppable so the operator sees it.

## Single-user invariant

For Akkountant (single-user by design, per rig CLAUDE.md), the
verified ``uid`` returned by Firebase can only be the Overseer's
account — Firebase itself enforces that. We do NOT hardcode the
Overseer's UID here: that would make credential rotation painful
AND would weaken the trust boundary (Firebase's auth decision
becomes authoritative, not a hardcoded comparison). The fix is
just to trust Firebase's verification instead of the raw header.
"""

from __future__ import annotations

import os
import threading
from typing import Optional, Tuple

from utils.logger import Logger

logger = Logger(__name__).get_logger()


# ─── Env-var contract ──────────────────────────────────────────────

# Auth mode — see module docstring. Default is the rollout-cycle
# shape (backward-compat fallback) so a pre-ak-c8l FE deploy keeps
# working during the deploy window.
_AUTH_MODE_ENV = "FIREBASE_AUTH_MODE"
_MODE_VERIFY_ONLY = "verify_only"
_MODE_FALLBACK = "verify_with_raw_uid_fallback"
_MODE_RAW_UID_ONLY = "raw_uid_only"
_DEFAULT_MODE = _MODE_FALLBACK

_CREDENTIALS_PATH_ENV = "FIREBASE_CREDENTIALS_PATH"

# Heuristic: a Firebase ID JWT has three segments separated by
# dots (header.payload.signature). A bare UID is a 28-char
# alphanumeric string with NO dots. We use dot-count as the
# cheap discriminator before attempting a verify. The exact
# UID length isn't strictly 28 in all Firebase account types
# (some longer); we don't assert on length, just on "no dots".
_JWT_DOT_COUNT = 2


# ─── Admin SDK initialization (lazy + thread-safe) ─────────────────

_init_lock = threading.Lock()
_init_done = False
_init_ok = False
_init_error: Optional[str] = None


def init_firebase_admin(credentials_path: Optional[str] = None) -> bool:
    """Initialize firebase_admin with a service-account credential.

    Lazy + idempotent: safe to call from app startup OR from the
    first auth hit. Returns True on success, False on any failure
    (missing dep, missing credentials file, bad JSON, etc.). Failure
    is logged at WARN — the caller is expected to fall back to the
    operator-chosen mode.

    Thread-safe via a module-level lock — Flask's multi-worker boot
    can race on init otherwise.
    """
    global _init_done, _init_ok, _init_error
    with _init_lock:
        if _init_done:
            return _init_ok

        path = credentials_path or os.getenv(_CREDENTIALS_PATH_ENV)
        if not path:
            _init_done = True
            _init_ok = False
            _init_error = (
                f"{_CREDENTIALS_PATH_ENV} env var unset"
            )
            logger.warning(
                "Firebase admin init: no credentials path configured "
                f"({_CREDENTIALS_PATH_ENV} unset). JWT verification "
                "disabled — mode degrades to raw-UID."
            )
            return False

        if not os.path.isfile(path):
            _init_done = True
            _init_ok = False
            _init_error = f"credentials file not found: {path}"
            logger.warning(
                f"Firebase admin init: credentials file missing at "
                f"{path!r}. JWT verification disabled — mode degrades "
                "to raw-UID."
            )
            return False

        try:
            import firebase_admin
            from firebase_admin import credentials
        except Exception as exc:  # pragma: no cover — dep absent
            _init_done = True
            _init_ok = False
            _init_error = f"firebase-admin import failed: {exc}"
            logger.warning(
                f"Firebase admin init: firebase-admin SDK not "
                f"importable ({exc}). Install via requirements.txt. "
                "JWT verification disabled — mode degrades to raw-UID."
            )
            return False

        try:
            # Idempotent: if an existing app is registered we
            # skip re-init. firebase_admin.get_app() raises
            # ValueError on no-existing-app so wrap in a probe.
            try:
                firebase_admin.get_app()
                _init_done = True
                _init_ok = True
                _init_error = None
                logger.info(
                    "Firebase admin init: reusing existing default "
                    "app."
                )
                return True
            except ValueError:
                pass  # no existing app — proceed with init

            cred = credentials.Certificate(path)
            firebase_admin.initialize_app(cred)
            _init_done = True
            _init_ok = True
            _init_error = None
            logger.info(
                f"Firebase admin init: initialized from {path!r}."
            )
            return True
        except Exception as exc:
            _init_done = True
            _init_ok = False
            _init_error = f"firebase-admin init raised: {exc}"
            logger.warning(
                f"Firebase admin init: initialize_app raised ({exc}). "
                "JWT verification disabled — mode degrades to raw-UID."
            )
            return False


def _reset_for_tests() -> None:
    """Reset module-level init state. ONLY for tests — allows a
    test suite to exercise the init path repeatedly under different
    env-var regimes. NOT thread-safe; tests shouldn't need to be."""
    global _init_done, _init_ok, _init_error
    _init_done = False
    _init_ok = False
    _init_error = None


# ─── Request-level authentication ──────────────────────────────────


class FirebaseAuthError(Exception):
    """Raised when authentication cannot be established for a
    request. Carries a short reason suitable for 401 response.
    Never carries the raw token or sensitive detail."""

    def __init__(self, reason: str):
        super().__init__(reason)
        self.reason = reason


def _get_mode() -> str:
    """Resolve the active auth mode from env var, falling back to
    _DEFAULT_MODE on unknown values."""
    mode = (os.getenv(_AUTH_MODE_ENV) or _DEFAULT_MODE).strip().lower()
    if mode not in {_MODE_VERIFY_ONLY, _MODE_FALLBACK, _MODE_RAW_UID_ONLY}:
        logger.warning(
            f"Unknown {_AUTH_MODE_ENV}={mode!r}; defaulting to "
            f"{_DEFAULT_MODE!r}."
        )
        return _DEFAULT_MODE
    return mode


def _looks_like_jwt(header_value: str) -> bool:
    """Cheap discriminator: a Firebase ID JWT has exactly 2 dots
    (header.payload.signature). A bare Firebase UID has 0 dots.
    Called BEFORE the heavier verify_id_token path so a bare-UID
    rollout-cycle fallback doesn't incur the JWT crypto cost.
    """
    if not header_value or not isinstance(header_value, str):
        return False
    return header_value.count(".") == _JWT_DOT_COUNT


def authenticate_request(
    header_value: Optional[str],
) -> Tuple[Optional[str], str]:
    """Authenticate one request given the raw X-Firebase-ID header.

    Returns ``(verified_uid, auth_mode)``:
      * ``verified_uid`` is the uid to store at ``g.firebase_id``.
      * ``auth_mode`` is one of ``"jwt"``, ``"raw_uid_fallback"``,
        ``"raw_uid_only"``. Logged by the caller for telemetry.

    Raises ``FirebaseAuthError`` with a 401-appropriate reason on
    any failure.

    Decision order:
      1. If the header is missing / empty → FirebaseAuthError.
      2. If ``FIREBASE_AUTH_MODE=raw_uid_only`` (LOCAL / test):
         trust the header as a bare UID. Log at DEBUG.
      3. If the header looks like a JWT (2 dots):
         call verify_id_token. On success, return the verified uid.
         On failure, raise FirebaseAuthError — this is the strict
         path, we don't silently fall back.
      4. If the header does NOT look like a JWT:
         behaviour depends on mode:
           * ``verify_with_raw_uid_fallback`` (default): accept the
             raw UID with WARN log so the operator sees rollout
             state. Mode ``raw_uid_fallback``.
           * ``verify_only``: reject with FirebaseAuthError —
             bare UID is not acceptable in the final-state mode.
    """
    if not header_value or not str(header_value).strip():
        raise FirebaseAuthError("Firebase ID is required")

    header_value = str(header_value).strip()
    mode = _get_mode()

    # Fast-path LOCAL / test mode — never calls firebase_admin.
    if mode == _MODE_RAW_UID_ONLY:
        logger.debug(
            "Firebase auth: raw_uid_only mode — trusting header "
            "without verification."
        )
        return header_value, "raw_uid_only"

    if _looks_like_jwt(header_value):
        # JWT path. Try verify.
        init_ok = init_firebase_admin()
        if not init_ok:
            # SDK or credentials unavailable. Under FALLBACK mode
            # we could still accept the token as a raw UID, but
            # that would be a LIE — a JWT-shaped input isn't a UID.
            # Reject cleanly.
            raise FirebaseAuthError(
                "Firebase verification unavailable (admin SDK not "
                "initialized); contact administrator"
            )
        try:
            from firebase_admin import auth as fb_auth
            decoded = fb_auth.verify_id_token(header_value)
        except Exception as exc:
            # Surface a short reason — never include the raw token
            # in the response or log. Log the exception class name
            # at WARN so operators can see verify failures without
            # leaking token content.
            logger.warning(
                f"Firebase auth: verify_id_token failed "
                f"({type(exc).__name__}): {str(exc)[:200]}"
            )
            raise FirebaseAuthError(
                "Firebase ID token verification failed"
            )
        verified_uid = decoded.get("uid") or decoded.get("user_id")
        if not verified_uid:
            logger.warning(
                "Firebase auth: verify_id_token succeeded but "
                "returned no uid field"
            )
            raise FirebaseAuthError(
                "Firebase ID token payload missing uid"
            )
        return str(verified_uid), "jwt"

    # Bare-UID path (no dots). Behaviour depends on mode.
    if mode == _MODE_VERIFY_ONLY:
        raise FirebaseAuthError(
            "Firebase ID token required (bare UID rejected in "
            "verify_only mode)"
        )
    # FALLBACK (default): accept the raw UID with a WARN log so
    # operator sees rollout state. This is the deploy-cycle middle
    # step. Flip to verify_only once FE is confirmed on the new
    # token contract.
    logger.warning(
        "Firebase auth: accepting raw UID fallback — rollout mode. "
        "Flip FIREBASE_AUTH_MODE=verify_only once FE is on the "
        "token contract. header-shape: no-dots, "
        f"len={len(header_value)}."
    )
    return header_value, "raw_uid_fallback"
