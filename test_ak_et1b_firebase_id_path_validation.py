"""ak-et1b SECURITY C-3 (2026-11-23): regression tests for the
strict firebase_id shape validation that closes the arbitrary-
filesystem-read/delete/write surface via X-Firebase-ID header.

Vulnerability class (pre-ak-et1b):
    5 cited audit sites + 11 additional callsites dropped
    `user_id` (= firebase_id, sourced from the X-Firebase-ID
    header or JWT claim) directly into `os.path.join(...,
    user_id, ...)` with NO validation. A UID containing `../`,
    `/`, NUL, or other filesystem-sensitive chars would escape
    the sandbox dir:
      * `..%2F..%2Fother_user` → read/write/delete another
        user's files (not meaningful in single-user app, but
        hardens future shape)
      * `../../../etc/passwd` → /etc read in the uploads/
        statements paths (serving apps don't typically have
        read of /etc, but the base_upload_dir resolution would
        land outside /uploads)
      * Null byte in uid → may truncate path at some C API
        boundary (defense-in-depth)

Fix (ak-et1b, layered):
  * `utils/firebase_id_validator.py`:
      - `_FIREBASE_UID_RE = ^[A-Za-z0-9_-]{1,128}$`
      - `is_valid_firebase_id(uid) -> bool` for middleware gate
      - `assert_valid_firebase_id_for_path(uid) -> str` for
        callsite defense-in-depth (raises InvalidFirebaseId)
  * `app.py` before_request middleware: validates verified_uid
    BEFORE setting g.firebase_id. 400 BAD_REQUEST + [ak-et1b]
    log on fail.
  * 5 cited audit sites + their callsite siblings:
      - services/transactionsService.py :577 (_resolve_local_
        pdf_path), :1140 (fetchLocalStatements), :1189
        (downloadLocalStatement), :1215 (deleteLocalStatement),
        :1240 (fetchProcessedStatements covering :1305)
      - controllers/transactionsEP.py :464-469 (downloadFile
        pe_ path)
      - services/fileStorageService.py `_get_user_dir`
      - services/signatureService.py upload_signature_file
      - services/agentFileAttachmentService.py :243 (per-user
        subdir)
    All call `assert_valid_firebase_id_for_path(user_id)` right
    before the path construction.

Pin-downs:

Pure-unit (validator):
  (1-6)   is_valid_firebase_id accepts legit UIDs
          (28-char alphanumeric, with `-` / `_`, up to 128
          chars).
  (7-14)  Rejects: `..`, `../`, `/path`, backslash, NUL,
          control char, empty, non-str, overlong (>128 chars),
          leading/trailing space, dot-segment, URL-encoded
          `%2F`.
  (15)    assert_valid_firebase_id_for_path returns the uid on
          success.
  (16)    assert_valid_firebase_id_for_path raises
          InvalidFirebaseId on fail.
  (17)    InvalidFirebaseId does NOT echo the invalid uid in
          its message (avoid log-injection reflection).

Middleware integration:
  (18)    app.py before_request runs is_valid_firebase_id
          after authenticate_request succeeds.
  (19)    Reject logs with [ak-et1b] tag + returns 400.

Callsite source-grep (5 cited sites + the clones):
  (20-24) Each of the 5 cited audit sites invokes
          assert_valid_firebase_id_for_path (via explicit call
          OR through a wrapper like _get_user_dir).
"""
from __future__ import annotations

import os
import re
import sys
import unittest


sys.path.insert(0, os.path.dirname(os.path.abspath(__file__)))


def _read_source(rel_path):
    here = os.path.dirname(os.path.abspath(__file__))
    with open(os.path.join(here, rel_path), "r") as f:
        return f.read()


# ─── Pure-unit: is_valid_firebase_id ─────────────────────────


class TestIsValidFirebaseId(unittest.TestCase):

    def setUp(self):
        from utils.firebase_id_validator import is_valid_firebase_id
        self.fn = is_valid_firebase_id

    # (1-6) Legit UIDs accepted.
    def test_accepts_legacy_28_char_alphanumeric(self):
        self.assertTrue(self.fn("AwKnpKEPmEhQnpc2kJAnsEikOBK2"))

    def test_accepts_alphanumeric_with_hyphen(self):
        self.assertTrue(self.fn("user-123-ABC"))

    def test_accepts_alphanumeric_with_underscore(self):
        self.assertTrue(self.fn("user_abc_456"))

    def test_accepts_all_hex(self):
        self.assertTrue(self.fn("0123456789abcdef"))

    def test_accepts_single_char(self):
        self.assertTrue(self.fn("A"))

    def test_accepts_128_char_boundary(self):
        self.assertTrue(self.fn("a" * 128))

    # (7-14) Rejects attacker shapes.
    def test_rejects_dot_dot_traversal(self):
        self.assertFalse(self.fn(".."))

    def test_rejects_dot_dot_slash(self):
        self.assertFalse(self.fn("../"))
        self.assertFalse(self.fn("../etc/passwd"))

    def test_rejects_forward_slash(self):
        self.assertFalse(self.fn("user/sub"))

    def test_rejects_backslash(self):
        self.assertFalse(self.fn("user\\sub"))

    def test_rejects_null_byte(self):
        self.assertFalse(self.fn("user\x00rest"))

    def test_rejects_control_chars(self):
        self.assertFalse(self.fn("user\nrest"))
        self.assertFalse(self.fn("user\trest"))
        self.assertFalse(self.fn("user\rrest"))

    def test_rejects_trailing_newline(self):
        # ak-qkii: `^...$` matched before a trailing newline, so
        # 'abc\n' passed. fullmatch() closes it.
        self.assertFalse(self.fn("abc\n"))
        self.assertFalse(self.fn("a" * 128 + "\n"))
        self.assertFalse(self.fn("abc\r\n"))

    def test_rejects_empty_string(self):
        self.assertFalse(self.fn(""))

    def test_rejects_non_str(self):
        self.assertFalse(self.fn(None))
        self.assertFalse(self.fn(123))
        self.assertFalse(self.fn(["a"]))
        self.assertFalse(self.fn({"uid": "a"}))

    def test_rejects_overlong(self):
        self.assertFalse(self.fn("a" * 129))
        self.assertFalse(self.fn("a" * 10_000))

    def test_rejects_leading_trailing_space(self):
        self.assertFalse(self.fn(" user123"))
        self.assertFalse(self.fn("user123 "))

    def test_rejects_url_encoded_traversal(self):
        # Attacker tries %2F / %2E%2E — `%` not in allowlist.
        self.assertFalse(self.fn("..%2F.."))
        self.assertFalse(self.fn("user%00"))

    def test_rejects_lone_dot(self):
        # Single dot — not in [A-Za-z0-9_-].
        self.assertFalse(self.fn("."))
        self.assertFalse(self.fn("user.name"))


# ─── Pure-unit: assert_valid_firebase_id_for_path ────────────


class TestAssertValidFirebaseIdForPath(unittest.TestCase):

    def setUp(self):
        from utils.firebase_id_validator import (
            assert_valid_firebase_id_for_path, InvalidFirebaseId,
        )
        self.fn = assert_valid_firebase_id_for_path
        self.Exc = InvalidFirebaseId

    # (15) Returns the uid unchanged on success.
    def test_returns_uid_unchanged_on_valid(self):
        self.assertEqual(
            self.fn("AwKnpKEPmEhQnpc2kJAnsEikOBK2"),
            "AwKnpKEPmEhQnpc2kJAnsEikOBK2",
        )

    # (16) Raises on invalid.
    def test_raises_on_invalid(self):
        with self.assertRaises(self.Exc):
            self.fn("../../../etc/passwd")
        with self.assertRaises(self.Exc):
            self.fn("")
        with self.assertRaises(self.Exc):
            self.fn(None)

    # (17) Error message does NOT reflect the invalid uid
    # (avoid log-injection / attacker-controlled strings
    # leaking into error paths that might feed back to logs).
    def test_error_message_no_uid_reflection(self):
        attacker_uid = "ATTACKER_MARKER_12345"
        with self.assertRaises(self.Exc) as cm:
            self.fn(f"../{attacker_uid}")
        self.assertNotIn(
            attacker_uid, str(cm.exception),
            "ak-et1b: error message must NOT echo the invalid "
            "uid — attacker-controlled strings reflected into "
            "error paths can be a log-injection vector",
        )
        self.assertIn(
            "ak-et1b", str(cm.exception),
            "error message should carry the bead tag so prod "
            "triage finds this fail-path",
        )


# ─── Middleware source-grep ──────────────────────────────────


class TestBeforeRequestMiddleware(unittest.TestCase):

    def setUp(self):
        self.src = _read_source("app.py")

    # (18) Middleware validates verified_uid after
    # authenticate_request.
    def test_middleware_shape_checks_verified_uid(self):
        # Slice the _request_interceptor body. Terminator: any
        # `def ` or top-level comment block at the method's end.
        m = re.search(
            r"def _request_interceptor\([\s\S]*?"
            r"g\.firebase_auth_mode = auth_mode",
            self.src,
        )
        self.assertIsNotNone(
            m,
            "_request_interceptor body not found through the "
            "g.firebase_auth_mode assignment — shape change",
        )
        body = m.group(0)
        # is_valid_firebase_id is called inside.
        self.assertIn(
            "is_valid_firebase_id(verified_uid)", body,
            "ak-et1b: middleware must call "
            "is_valid_firebase_id(verified_uid) BEFORE setting "
            "g.firebase_id — the central gate",
        )
        # And the gate comes BEFORE the assignment.
        gate_idx = body.find("is_valid_firebase_id(verified_uid)")
        assign_idx = body.find("g.firebase_id = verified_uid")
        self.assertGreater(gate_idx, -1)
        self.assertGreater(assign_idx, -1)
        self.assertLess(
            gate_idx, assign_idx,
            "ak-et1b: shape gate must PRECEDE the "
            "g.firebase_id assignment — otherwise bad uids are "
            "live in request-scoped g before the gate fires",
        )

    # (19) Reject path logs [ak-et1b] + returns 400.
    def test_middleware_reject_logs_and_returns_400(self):
        m = re.search(
            r"def _request_interceptor\([\s\S]*?"
            r"g\.firebase_auth_mode = auth_mode",
            self.src,
        )
        self.assertIsNotNone(m)
        body = m.group(0)
        self.assertIn(
            "[ak-et1b]", body,
            "ak-et1b: middleware reject must log with the bead "
            "tag for prod triage",
        )
        # 400 is the code (bad request on malformed shape).
        self.assertRegex(
            body,
            r"firebase_id shape invalid[\s\S]{0,100}400",
            "ak-et1b: middleware must return 400 on shape "
            "failure (401 is auth-rejection class, distinct)",
        )


# ─── Callsite source-grep ────────────────────────────────────


class TestCallsiteHardening(unittest.TestCase):

    # (20) transactionsService.py — all 5 userId path sites gated
    # by assert_valid_firebase_id_for_path.
    def test_transactions_service_all_sites_gated(self):
        src = _read_source("services/transactionsService.py")
        # Simple substring count: lines ending with `userId)` or
        # `user_id)` INSIDE claude_statements path joins. The
        # 5 cited sites all use the pattern
        # `os.path.join(os.getcwd(), "claude_statements", <uid>...)`.
        join_lines = [
            line for line in src.splitlines()
            if "claude_statements" in line
            and "os.path.join" in line
        ]
        self.assertGreaterEqual(
            len(join_lines), 5,
            f"Expected ≥5 claude_statements path-join lines in "
            f"transactionsService.py (per bead); found "
            f"{len(join_lines)}. Shape change?",
        )
        # Count validator-call occurrences.
        guard_calls = re.findall(
            r"assert_valid_firebase_id_for_path\(",
            src,
        )
        self.assertGreaterEqual(
            len(guard_calls), 5,
            f"ak-et1b: expected ≥5 "
            f"assert_valid_firebase_id_for_path calls in "
            f"transactionsService.py (one per path site OR "
            f"one per method entry). Found {len(guard_calls)}.",
        )
        self.assertIn(
            "from utils.firebase_id_validator import", src,
        )

    # (21) controllers/transactionsEP.py pe_ branch gated.
    def test_controller_transactions_ep_gated(self):
        src = _read_source("controllers/transactionsEP.py")
        self.assertIn(
            "assert_valid_firebase_id_for_path", src,
            "ak-et1b: transactionsEP must gate userId before "
            "path join in the pe_ file-serve branch",
        )

    # (22) fileStorageService._get_user_dir gated.
    def test_file_storage_service_get_user_dir_gated(self):
        src = _read_source("services/fileStorageService.py")
        # Slice _get_user_dir body.
        m = re.search(
            r"def _get_user_dir\(.*?(?=\n    def [a-zA-Z_])",
            src, re.DOTALL,
        )
        self.assertIsNotNone(
            m, "_get_user_dir body not found",
        )
        body = m.group(0)
        self.assertIn(
            "assert_valid_firebase_id_for_path(user_id)", body,
            "ak-et1b: _get_user_dir must gate user_id — it is "
            "the shared choke-point for file upload dirs",
        )

    # (23) signatureService.upload_signature_file gated.
    def test_signature_service_gated(self):
        src = _read_source("services/signatureService.py")
        self.assertIn(
            "assert_valid_firebase_id_for_path(user_id)", src,
            "ak-et1b: signatureService must gate user_id before "
            "embedding in filename (pre-ak-et1b a `../x` uid "
            "would emit a filename like `../x_sig.png`)",
        )

    # (24) agentFileAttachmentService per-user subdir gated.
    def test_agent_file_attachment_service_gated(self):
        src = _read_source("services/agentFileAttachmentService.py")
        # Slice around the user_dir = os.path.join(_BASE_DIR, user_id).
        m = re.search(
            r"user_dir\s*=\s*os\.path\.join\(_BASE_DIR,\s*user_id\)",
            src,
        )
        self.assertIsNotNone(
            m,
            "user_dir construction not found — shape changed",
        )
        # The guard must precede this line (within ~300 chars).
        guard_idx = src.rfind(
            "assert_valid_firebase_id_for_path(user_id)",
            0, m.start(),
        )
        self.assertGreater(
            guard_idx, -1,
            "ak-et1b: agentFileAttachmentService must call "
            "assert_valid_firebase_id_for_path(user_id) BEFORE "
            "constructing user_dir",
        )
        self.assertLess(
            m.start() - guard_idx, 500,
            "ak-et1b: guard is >500 chars before user_dir "
            "construction — gap suggests a refactor split them. "
            "Review that nothing bypasses the gate.",
        )


class TestDefenseInDepthPropagates(unittest.TestCase):
    """ak-qkii: D-i-D trips propagate as InvalidFirebaseId (uncaught
    -> 500) instead of being swallowed into a graceful error dict."""

    _real_mod = None

    @classmethod
    def _load_real_service_module(cls):
        # Other test files replace sys.modules["services.transactionsService"]
        # with a stub at collection time, so a plain import is
        # order-dependent. Load the real file under a private name, and
        # take InvalidFirebaseId from THAT module's namespace so the
        # class identity matches what its methods raise.
        if cls._real_mod is None:
            import importlib.util
            here = os.path.dirname(os.path.abspath(__file__))
            spec = importlib.util.spec_from_file_location(
                "_ak_qkii_real_transactionsService",
                os.path.join(here, "services", "transactionsService.py"),
            )
            mod = importlib.util.module_from_spec(spec)
            spec.loader.exec_module(mod)
            cls._real_mod = mod
        return cls._real_mod

    def setUp(self):
        from unittest import mock
        try:
            mod = self._load_real_service_module()
        except Exception as e:  # pragma: no cover - env without deps
            self.skipTest(f"transactionsService import failed: {e}")
        self.svc = mock.MagicMock()
        self.cls = mod.TransactionService
        self.Exc = mod.InvalidFirebaseId
        self.assertEqual(self.Exc.__name__, "InvalidFirebaseId")

    def test_fetch_local_statements_propagates(self):
        with self.assertRaises(self.Exc):
            self.cls.fetchLocalStatements(self.svc, "../other")

    def test_download_local_statement_propagates(self):
        with self.assertRaises(self.Exc):
            self.cls.downloadLocalStatement(self.svc, "../other", "b", "f.pdf")

    def test_delete_local_statement_propagates(self):
        with self.assertRaises(self.Exc):
            self.cls.deleteLocalStatement(self.svc, "uid\n", "b", "f.pdf")


if __name__ == "__main__":  # pragma: no cover
    unittest.main(verbosity=2)
