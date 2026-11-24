"""ak-et1b SECURITY C-3 (2026-11-23) — strict firebase_id shape
validation for callsites that use it as a filesystem path
component.

Attack class (pre-ak-et1b):
    * controllers + services dropped `user_id` (= firebase_id,
      sourced from the X-Firebase-ID header or JWT claim) directly
      into `os.path.join(..., user_id, ...)` with NO validation.
    * Legitimate Firebase UIDs are alphanumeric (optionally with
      `-` / `_`), ≤ 128 chars, no filesystem separators / path-
      traversal characters.
    * An attacker-controlled string like `../../../etc/passwd` or
      `..%2F..%2Fother_user` passing through the middleware
      (ak-c8l pre-this-bead did not shape-check the UID, only
      JWT-verified it — but the `verify_with_raw_uid_fallback`
      rollout mode admits ANY string, and a mis-signed JWT during
      the dual-mode window could land arbitrary UIDs) would reach
      the filesystem join unchanged.

Defense:
    * CENTRAL (primary): `app.py` before_request middleware
      validates the verified_uid via `is_valid_firebase_id()`
      before setting `g.firebase_id`. Any UID failing the strict
      pattern → 400 BAD_REQUEST with [ak-et1b]-tagged log. Closes
      the attack class at the single chokepoint.
    * DEFENSE-IN-DEPTH (per-callsite): the 5 cited audit sites
      (`transactionsService` :1130+, :1178, :1201, :1291 /
      `controllers/transactionsEP` :464-469 / `fileStorageService`
      `_get_user_dir` / `signatureService.upload_signature_file` /
      `agentFileAttachmentService` :243) call
      `assert_valid_firebase_id_for_path(user_id)` right before
      the path construction. Catches:
        - any future callsite that bypasses the middleware (test
          harness, CLI utility, backfill script, scheduled task)
        - a middleware refactor that drops the validator
        - a service-method direct call (bypasses HTTP entry)
      Belt-and-brace, tiny perf cost.

Shape rule (whole string, via re.fullmatch):
    [A-Za-z0-9_-]{1,128}

Rejects:
    * empty string
    * non-str
    * `..` traversal segment (fails pattern — `.` not allowed)
    * `/` or `\\` separators (fail pattern)
    * NUL / whitespace / control chars (fail pattern), including a
      trailing newline (ak-qkii: fullmatch, not `$`)
    * URL-encoded path chars (`%2F`, etc. — `%` not allowed)
    * strings > 128 chars (modern Firebase Auth hard-caps at 128)
    * empty / None

Precedent: ak-ptxq host-allowlist pattern (strict single-regex
accept + fail-closed reject + bead-tagged log).
"""
from __future__ import annotations

import re


# Strict shape — alphanumeric + hyphen + underscore, 1-128 chars.
# Firebase Auth UIDs are alphanumeric by spec; legacy UIDs are
# 28 chars; newer custom-auth UIDs can reach 128.
# ak-qkii: matched with fullmatch(), NOT match() + `$` — in Python `$`
# also matches before a trailing newline, so `^...$` admitted 'abc\n'.
_FIREBASE_UID_RE = re.compile(r"[A-Za-z0-9_-]{1,128}")


class InvalidFirebaseId(ValueError):
    """Raised by assert_valid_firebase_id_for_path when the uid
    fails the shape check. Callers let it propagate (ak-qkii): an
    uncaught 500 is the intended defense-in-depth signal."""


def is_valid_firebase_id(uid) -> bool:
    """Return True iff the uid is a non-empty str matching the
    strict shape. Used as a boolean gate (e.g., in middleware
    reject logic).
    """
    if not isinstance(uid, str):
        return False
    return _FIREBASE_UID_RE.fullmatch(uid) is not None


def assert_valid_firebase_id_for_path(uid) -> str:
    """Return the uid unchanged if valid; raise InvalidFirebaseId
    otherwise. Used at path-building callsites where a bad uid
    would be a filesystem-traversal attempt.

    ak-qkii: callsites let InvalidFirebaseId PROPAGATE (uncaught ->
    500). The middleware is the primary gate, so a defense-in-depth
    trip means an unexpected bypass; a 500 is the correct "should
    never happen" signal and stays fail-closed. Do not catch it into
    a graceful error body.

    Returns the validated uid so callsites can chain it:
        user_dir = os.path.join(base, assert_valid_firebase_id_for_path(user_id))

    The exception message does NOT echo the invalid uid back
    (avoid log-injection / reflecting attacker-controlled
    strings into error paths that might feed back to logs).
    """
    if not is_valid_firebase_id(uid):
        raise InvalidFirebaseId(
            "ak-et1b: firebase_id fails strict shape check "
            "(expected [A-Za-z0-9_-]{1,128}); refusing to "
            "construct filesystem path"
        )
    return uid


__all__ = [
    "InvalidFirebaseId",
    "is_valid_firebase_id",
    "assert_valid_firebase_id_for_path",
]
