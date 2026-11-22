"""ak-c8l: tests for utils/firebase_auth.py.

Scope:
  1. Mode dispatch — ``_get_mode`` resolves env var correctly +
     falls back to default on unknown values.
  2. JWT shape heuristic — ``_looks_like_jwt`` discriminates
     JWT-shaped tokens (2 dots) from bare UIDs (0 dots).
  3. ``authenticate_request`` behaviour matrix:
     a. Missing / empty header → FirebaseAuthError.
     b. ``raw_uid_only`` mode → trust header verbatim.
     c. JWT-shaped header + verify success → return verified uid.
     d. JWT-shaped header + verify failure → FirebaseAuthError
        (no silent fallback — JWTs must verify, not be trusted on
        shape alone).
     e. Bare UID + ``verify_only`` mode → FirebaseAuthError.
     f. Bare UID + ``verify_with_raw_uid_fallback`` → trust with
        WARN log.
     g. JWT-shaped header with init failure (no credentials /
        SDK absent) → FirebaseAuthError (reject the token outright
        rather than treat it as a raw UID).
  4. Payload shape: verified uid must come from ``uid`` key with
     ``user_id`` as fallback (Firebase SDK versions differ).
  5. Controller drift guard — every current direct reader of
     X-Firebase-ID header is through g.firebase_id, not the raw
     header. Protects the ak-c8l fix across the controller
     surface.

Baseline discipline: tests patch the firebase_admin surface via
``unittest.mock`` so the real SDK is never imported — keeps the
test file bare-env-safe + fast.
"""

from __future__ import annotations

import os
import sys
import types
import unittest
from unittest.mock import MagicMock, patch

sys.path.insert(0, os.path.dirname(os.path.abspath(__file__)))


# Stub firebase_admin modules so tests can import + patch
# verify_id_token without the real SDK being installed in this
# env. Matches the test_kite_bundle2 claude_agent_sdk stub pattern.
# The stub is INSTALLED at module import time so any lazy import
# inside utils.firebase_auth.authenticate_request resolves to our
# controllable mock.
_FB_ADMIN_STUBBED = "firebase_admin" not in sys.modules
if _FB_ADMIN_STUBBED:
    fb_admin_mod = types.ModuleType("firebase_admin")
    fb_admin_mod.__path__ = []
    fb_auth_mod = types.ModuleType("firebase_admin.auth")
    fb_credentials_mod = types.ModuleType("firebase_admin.credentials")

    # Default: verify_id_token must be patched per-test via the
    # explicit `with patch(...)` context — this default raises so
    # a test that forgot to patch fails loudly rather than silently
    # accepting an under-defined mock.
    def _default_verify_id_token(token, *args, **kwargs):
        raise AssertionError(
            "test used firebase_admin.auth.verify_id_token without "
            "patching it — add `with patch("
            "'firebase_admin.auth.verify_id_token', ...)` to the "
            "test body."
        )
    fb_auth_mod.verify_id_token = _default_verify_id_token
    fb_credentials_mod.Certificate = MagicMock(
        name="firebase_admin.credentials.Certificate"
    )
    fb_admin_mod.auth = fb_auth_mod
    fb_admin_mod.credentials = fb_credentials_mod
    fb_admin_mod.initialize_app = MagicMock(
        name="firebase_admin.initialize_app",
    )
    fb_admin_mod.get_app = MagicMock(
        name="firebase_admin.get_app",
    )
    sys.modules["firebase_admin"] = fb_admin_mod
    sys.modules["firebase_admin.auth"] = fb_auth_mod
    sys.modules["firebase_admin.credentials"] = fb_credentials_mod


from utils.firebase_auth import (  # noqa: E402
    FirebaseAuthError,
    _get_mode,
    _looks_like_jwt,
    _reset_for_tests,
    authenticate_request,
)


# ─── (1) Mode dispatch ────────────────────────────────────────────


class TestGetMode(unittest.TestCase):

    def test_unset_defaults_to_fallback(self):
        with patch.dict(os.environ, {}, clear=False):
            os.environ.pop("FIREBASE_AUTH_MODE", None)
            self.assertEqual(_get_mode(), "verify_with_raw_uid_fallback")

    def test_verify_only_passthrough(self):
        with patch.dict(
            os.environ,
            {"FIREBASE_AUTH_MODE": "verify_only"},
        ):
            self.assertEqual(_get_mode(), "verify_only")

    def test_fallback_mode_passthrough(self):
        with patch.dict(
            os.environ,
            {"FIREBASE_AUTH_MODE": "verify_with_raw_uid_fallback"},
        ):
            self.assertEqual(_get_mode(), "verify_with_raw_uid_fallback")

    def test_raw_uid_only_passthrough(self):
        with patch.dict(
            os.environ,
            {"FIREBASE_AUTH_MODE": "raw_uid_only"},
        ):
            self.assertEqual(_get_mode(), "raw_uid_only")

    def test_unknown_value_falls_back(self):
        with patch.dict(
            os.environ,
            {"FIREBASE_AUTH_MODE": "complete-garbage"},
        ):
            self.assertEqual(_get_mode(), "verify_with_raw_uid_fallback")

    def test_case_insensitive(self):
        with patch.dict(
            os.environ,
            {"FIREBASE_AUTH_MODE": "VERIFY_ONLY"},
        ):
            self.assertEqual(_get_mode(), "verify_only")


# ─── (2) JWT shape heuristic ──────────────────────────────────────


class TestLooksLikeJWT(unittest.TestCase):

    def test_two_dots_is_jwt(self):
        self.assertTrue(_looks_like_jwt("header.payload.signature"))
        self.assertTrue(_looks_like_jwt("a.b.c"))

    def test_bare_uid_is_not_jwt(self):
        self.assertFalse(
            _looks_like_jwt("AwKnpKEPmEhQnpc2kJAnsEikOBK2"),
        )

    def test_empty_or_none_is_not_jwt(self):
        self.assertFalse(_looks_like_jwt(""))
        self.assertFalse(_looks_like_jwt(None))

    def test_one_dot_is_not_jwt(self):
        """A one-dot string is malformed as both UID and JWT —
        treat as not-JWT (will take the fallback-or-reject path
        depending on mode)."""
        self.assertFalse(_looks_like_jwt("foo.bar"))

    def test_three_dots_is_not_jwt(self):
        """Three dots is also malformed — JWT RFC is strict on
        the 3-segment shape (= 2 dots)."""
        self.assertFalse(_looks_like_jwt("a.b.c.d"))


# ─── (3) authenticate_request matrix ──────────────────────────────


class TestAuthenticateRequestMissingHeader(unittest.TestCase):

    def test_none_rejected(self):
        with self.assertRaises(FirebaseAuthError) as ctx:
            authenticate_request(None)
        self.assertIn("required", str(ctx.exception).lower())

    def test_empty_rejected(self):
        with self.assertRaises(FirebaseAuthError):
            authenticate_request("")

    def test_whitespace_only_rejected(self):
        with self.assertRaises(FirebaseAuthError):
            authenticate_request("   ")


class TestAuthenticateRequestRawUidOnlyMode(unittest.TestCase):
    """raw_uid_only mode: trust header verbatim, no SDK call."""

    def setUp(self):
        self._env = patch.dict(
            os.environ,
            {"FIREBASE_AUTH_MODE": "raw_uid_only"},
        )
        self._env.start()

    def tearDown(self):
        self._env.stop()

    def test_bare_uid_accepted_verbatim(self):
        uid, mode = authenticate_request("AwKnpKEPmEhQnpc2kJAnsEikOBK2")
        self.assertEqual(uid, "AwKnpKEPmEhQnpc2kJAnsEikOBK2")
        self.assertEqual(mode, "raw_uid_only")

    def test_jwt_shaped_input_also_accepted_as_raw(self):
        """In raw_uid_only mode, we don't try to verify. If the
        caller sends a JWT-shaped string we treat it as the UID.
        LOCAL env + test convenience — a developer who hands a
        JWT in by accident still gets routed to that string as
        their user_id."""
        uid, mode = authenticate_request("a.b.c")
        self.assertEqual(uid, "a.b.c")
        self.assertEqual(mode, "raw_uid_only")

    def test_strips_whitespace(self):
        uid, mode = authenticate_request("  u1  ")
        self.assertEqual(uid, "u1")


class TestAuthenticateRequestJWTPath(unittest.TestCase):
    """JWT-shaped header → verify_id_token must be called."""

    def setUp(self):
        self._env = patch.dict(
            os.environ,
            {"FIREBASE_AUTH_MODE": "verify_only"},
        )
        self._env.start()
        _reset_for_tests()

    def tearDown(self):
        self._env.stop()
        _reset_for_tests()

    def test_verify_success_returns_verified_uid(self):
        """Happy path: verify_id_token returns {uid: ...}; auth
        returns that uid."""
        with patch(
            "utils.firebase_auth.init_firebase_admin",
            return_value=True,
        ), patch(
            "firebase_admin.auth.verify_id_token",
            return_value={"uid": "verified-user-123"},
            create=True,
        ):
            uid, mode = authenticate_request("h.p.s")
            self.assertEqual(uid, "verified-user-123")
            self.assertEqual(mode, "jwt")

    def test_verify_success_falls_back_to_user_id_key(self):
        """Older Firebase SDK versions used 'user_id' key instead
        of 'uid'. Handle both."""
        with patch(
            "utils.firebase_auth.init_firebase_admin",
            return_value=True,
        ), patch(
            "firebase_admin.auth.verify_id_token",
            return_value={"user_id": "legacy-key-user"},
            create=True,
        ):
            uid, mode = authenticate_request("h.p.s")
            self.assertEqual(uid, "legacy-key-user")

    def test_verify_failure_raises(self):
        """verify_id_token raising ANY exception → auth rejects.
        No silent fallback — we don't want a malformed JWT to
        slide through as a raw UID."""
        with patch(
            "utils.firebase_auth.init_firebase_admin",
            return_value=True,
        ), patch(
            "firebase_admin.auth.verify_id_token",
            side_effect=Exception("bad signature"),
            create=True,
        ):
            with self.assertRaises(FirebaseAuthError) as ctx:
                authenticate_request("h.p.s")
            self.assertIn("verification failed", str(ctx.exception).lower())

    def test_verify_returns_no_uid_rejected(self):
        """Pathological case: verify succeeds but payload has
        neither uid nor user_id. Reject rather than accept a
        synthesized or empty uid."""
        with patch(
            "utils.firebase_auth.init_firebase_admin",
            return_value=True,
        ), patch(
            "firebase_admin.auth.verify_id_token",
            return_value={"some_other_key": "value"},
            create=True,
        ):
            with self.assertRaises(FirebaseAuthError):
                authenticate_request("h.p.s")

    def test_jwt_rejected_when_sdk_init_failed(self):
        """ak-c8l security invariant: if the firebase SDK can't
        initialize (no credentials file, bad JSON, SDK not
        importable), a JWT-shaped input MUST NOT silently fall
        through as a raw UID — that would re-open the IDOR
        surface. Reject with 401.
        """
        with patch(
            "utils.firebase_auth.init_firebase_admin",
            return_value=False,
        ):
            with self.assertRaises(FirebaseAuthError) as ctx:
                authenticate_request("h.p.s")
            self.assertIn(
                "unavailable", str(ctx.exception).lower(),
            )


class TestAuthenticateRequestBareUidPath(unittest.TestCase):
    """Bare UID (no dots) → behaviour depends on mode."""

    def setUp(self):
        _reset_for_tests()

    def tearDown(self):
        _reset_for_tests()

    def test_bare_uid_rejected_in_verify_only_mode(self):
        """verify_only is the final-state mode — bare UID must be
        rejected. This is what closes the IDOR surface."""
        with patch.dict(
            os.environ,
            {"FIREBASE_AUTH_MODE": "verify_only"},
        ):
            with self.assertRaises(FirebaseAuthError):
                authenticate_request("AwKnpKEPmEhQnpc2kJAnsEikOBK2")

    def test_bare_uid_accepted_in_fallback_mode(self):
        """verify_with_raw_uid_fallback: rollout-cycle middle
        step. Bare UID accepted with WARN log."""
        with patch.dict(
            os.environ,
            {"FIREBASE_AUTH_MODE": "verify_with_raw_uid_fallback"},
        ):
            uid, mode = authenticate_request(
                "AwKnpKEPmEhQnpc2kJAnsEikOBK2",
            )
            self.assertEqual(uid, "AwKnpKEPmEhQnpc2kJAnsEikOBK2")
            self.assertEqual(mode, "raw_uid_fallback")

    def test_fallback_mode_is_the_default(self):
        """ak-c8l deploy-cycle plan: default mode is fallback so
        a pre-ak-c8l FE deploy keeps working. Flip to
        verify_only once FE ships the token-sending version."""
        with patch.dict(os.environ, {}, clear=False):
            os.environ.pop("FIREBASE_AUTH_MODE", None)
            uid, mode = authenticate_request(
                "AwKnpKEPmEhQnpc2kJAnsEikOBK2",
            )
            self.assertEqual(uid, "AwKnpKEPmEhQnpc2kJAnsEikOBK2")
            self.assertEqual(mode, "raw_uid_fallback")


# ─── (5) Controller drift guard — no raw-header reads ─────────────


import re


def _read_source(rel_path):
    here = os.path.dirname(os.path.abspath(__file__))
    with open(os.path.join(here, rel_path), "r") as f:
        return f.read()


def test_app_before_request_reads_verified_uid_only():
    """The only place that reads the raw X-Firebase-ID header must
    be the before_request auth middleware. Anywhere ELSE reading
    the raw header re-opens the IDOR surface ak-c8l closes.
    """
    src = _read_source("app.py")
    # In before_request, we read request.headers.get("X-Firebase-
    # ID") once (the auth checkpoint). Post-fix, g.firebase_id is
    # the verified uid everywhere else.
    reads = re.findall(
        r'request\.headers\.get\(\s*["\']X-Firebase-ID["\']\s*\)',
        src,
    )
    assert len(reads) == 1, (
        f"app.py reads X-Firebase-ID raw header {len(reads)} times — "
        f"must be exactly 1 (the before_request auth checkpoint)."
    )


def test_controllers_do_not_read_raw_header():
    """Every controller handler must use g.firebase_id (set by
    before_request to the VERIFIED uid) — NOT the raw header.
    Reading the raw header here re-opens the IDOR surface.

    ak-c8l swept transactionsEP.py + jobEmailEP.py + any other
    controller that was doing this. This guard catches a
    regression where a new handler reads the raw header directly.
    """
    import glob
    here = os.path.dirname(os.path.abspath(__file__))
    controller_paths = glob.glob(
        os.path.join(here, "controllers", "*.py"),
    )
    offenders = []
    for path in controller_paths:
        with open(path, "r") as f:
            src = f.read()
        if re.search(
            r'request\.headers\.get\(\s*["\']X-Firebase-ID["\']\s*\)',
            src,
        ):
            offenders.append(os.path.basename(path))
    assert offenders == [], (
        f"Controllers reading raw X-Firebase-ID header "
        f"(ak-c8l IDOR regression): {offenders}. Switch to "
        f"g.get('firebase_id') — before_request sets it to the "
        f"verified uid."
    )


def test_before_request_sets_g_from_authenticate_request():
    """Drift guard: the auth middleware must call
    ``authenticate_request`` and store the VERIFIED uid in
    ``g.firebase_id`` — not the raw header value."""
    src = _read_source("app.py")
    assert "from utils.firebase_auth import" in src
    assert "authenticate_request" in src
    # Must assign g.firebase_id from the returned verified uid,
    # not from the raw header.
    assert re.search(
        r"g\.firebase_id\s*=\s*verified_uid",
        src,
    ), (
        "before_request must set g.firebase_id = verified_uid "
        "(from authenticate_request), not from the raw header."
    )


def test_firebase_admin_in_requirements():
    """Dep drift guard: firebase-admin must stay in requirements."""
    src = _read_source("requirements.txt")
    assert "firebase-admin" in src, (
        "firebase-admin dep removed from requirements.txt — "
        "ak-c8l depends on it."
    )


if __name__ == "__main__":
    unittest.main()
