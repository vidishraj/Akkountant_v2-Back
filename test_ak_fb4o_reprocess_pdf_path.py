"""ak-fb4o (2026-11-24): POST /reprocessPdf must not open a request-body
pdf_path that is not the user's own statement PDF.

Before: reprocess_pdf did fitz.open(pdf_path) on whatever the request
body sent (only os.path.exists was checked), and the gmail_id glob
fallback used the raw gmail_id / uid in a glob pattern.

After: reprocess_pdf resolves the path through
utils.mail_tool_paths.resolve_reprocess_pdf, the same realpath pin as
ak-jmbw: a regular .pdf under <cwd>/tmp/ or <cwd>/claude_statements/<uid>/.
All files here are synthetic, created in a temp dir.
"""
from __future__ import annotations

import ast
import os
import shutil
import sys
import tempfile
import unittest

HERE = os.path.dirname(os.path.abspath(__file__))
sys.path.insert(0, HERE)

from utils.mail_tool_paths import ToolPathRejected, resolve_reprocess_pdf  # noqa: E402

UID = "TestUid_123"
OTHER_UID = "OtherUid_456"
GMAIL_ID = "19cd534052ce39a8"


def _make_pdf(path):
    with open(path, "wb") as f:
        f.write(b"%PDF-1.4\n%%EOF\n")


class _Sandbox(unittest.TestCase):

    def setUp(self):
        self.root = os.path.realpath(tempfile.mkdtemp(prefix="akfb4o_"))
        self.app = os.path.join(self.root, "app")
        self.tmp = os.path.join(self.app, "tmp")
        self.stmts = os.path.join(self.app, "claude_statements", UID, "bank_statement")
        self.other = os.path.join(self.app, "claude_statements", OTHER_UID, "bank_statement")
        for d in (self.tmp, self.stmts, self.other):
            os.makedirs(d)
        self.stmt_pdf = os.path.join(self.stmts, f"{GMAIL_ID}_hdfc.pdf")
        _make_pdf(self.stmt_pdf)
        self.tmp_pdf = os.path.join(self.tmp, "run.pdf")
        _make_pdf(self.tmp_pdf)
        self.outside_pdf = os.path.join(self.root, "outside.pdf")
        _make_pdf(self.outside_pdf)
        self.secret = os.path.join(self.root, "secret.env")
        with open(self.secret, "w") as f:
            f.write("SYNTHETIC_SECRET=1\n")
        self._cwd = os.getcwd()
        os.chdir(self.app)

    def tearDown(self):
        os.chdir(self._cwd)
        shutil.rmtree(self.root, ignore_errors=True)

    def _rejects(self, pdf_path, uid=UID, gmail_id=GMAIL_ID):
        with self.assertRaises(ToolPathRejected) as cm:
            resolve_reprocess_pdf(uid, gmail_id, pdf_path)
        self.assertNotIn(self.root, str(cm.exception))


class TestExplicitPdfPath(_Sandbox):

    def test_own_statement_accepted(self):
        self.assertEqual(resolve_reprocess_pdf(UID, GMAIL_ID, self.stmt_pdf), self.stmt_pdf)

    def test_tmp_pdf_accepted(self):
        self.assertEqual(resolve_reprocess_pdf(UID, GMAIL_ID, self.tmp_pdf), self.tmp_pdf)

    def test_relative_path_resolved_against_cwd(self):
        rel = os.path.relpath(self.stmt_pdf, self.app)
        self.assertEqual(resolve_reprocess_pdf(UID, GMAIL_ID, rel), self.stmt_pdf)

    def test_outside_roots_rejected(self):
        self._rejects(self.outside_pdf)
        self._rejects("/etc/passwd")
        self._rejects(self.secret)

    def test_traversal_rejected(self):
        self._rejects(os.path.join(self.stmts, "..", "..", "..", "..", "outside.pdf"))
        self._rejects("claude_statements/" + UID + "/../../../outside.pdf")

    def test_other_users_statement_rejected(self):
        p = os.path.join(self.other, f"{GMAIL_ID}_x.pdf")
        _make_pdf(p)
        self._rejects(p)

    def test_symlink_escape_rejected(self):
        link = os.path.join(self.stmts, "evil.pdf")
        os.symlink(self.secret, link)
        self._rejects(link)
        link2 = os.path.join(self.tmp, "evil2.pdf")
        os.symlink(self.outside_pdf, link2)
        self._rejects(link2)

    def test_non_pdf_and_missing_rejected(self):
        txt = os.path.join(self.stmts, "notes.txt")
        with open(txt, "w") as f:
            f.write("x")
        self._rejects(txt)
        self._rejects(os.path.join(self.stmts, "nope.pdf"))
        os.makedirs(os.path.join(self.tmp, "dir.pdf"))
        self._rejects(os.path.join(self.tmp, "dir.pdf"))

    def test_non_string_rejected(self):
        for bad in (123, ["a.pdf"], {"p": 1}, True):
            self._rejects(bad)

    def test_invalid_uid_rejected(self):
        for bad in ("../" + UID, "a/b", "", None):
            self._rejects(self.stmt_pdf, uid=bad)


class TestGlobFallback(_Sandbox):

    def test_resolves_own_saved_pdf(self):
        for empty in (None, ""):
            self.assertEqual(resolve_reprocess_pdf(UID, GMAIL_ID, empty), self.stmt_pdf)

    def test_no_match_rejected(self):
        self._rejects(None, gmail_id="deadbeef")

    def test_other_users_pdf_not_found(self):
        os.remove(self.stmt_pdf)
        _make_pdf(os.path.join(self.other, f"{GMAIL_ID}_x.pdf"))
        self._rejects(None)

    def test_skips_non_pdf_and_escaping_matches(self):
        os.remove(self.stmt_pdf)
        with open(os.path.join(self.stmts, f"{GMAIL_ID}_a.txt"), "w") as f:
            f.write("x")
        os.symlink(self.outside_pdf, os.path.join(self.stmts, f"{GMAIL_ID}_b.pdf"))
        self._rejects(None)
        good = os.path.join(self.stmts, f"{GMAIL_ID}_c.pdf")
        _make_pdf(good)
        self.assertEqual(resolve_reprocess_pdf(UID, GMAIL_ID, None), good)

    def test_bad_gmail_id_rejected(self):
        for bad in ("../../..", "a/b", "a\\b", "..", "", None, 5):
            self._rejects(None, gmail_id=bad)
        # gmail_id must be a bare id, even if a path-shaped one would
        # land on an in-root PDF.
        self._rejects(None, gmail_id="bank_statement/" + GMAIL_ID)
        self._rejects(None, gmail_id="x/../" + GMAIL_ID)
        self._rejects(None, gmail_id="..x" + GMAIL_ID)

    def test_glob_wildcards_in_gmail_id_are_literal(self):
        self._rejects(None, gmail_id="*")
        self._rejects(None, gmail_id="19cd[0-9]*")

    def test_invalid_uid_rejected(self):
        for bad in ("../" + UID, "a/b", "", None, "*"):
            self._rejects(None, uid=bad)


class TestServiceWiring(unittest.TestCase):
    """reprocess_pdf pins its path before the first fitz.open and no
    longer globs / exists-checks the raw input itself."""

    def setUp(self):
        with open(os.path.join(HERE, "services", "mailProcessorService.py")) as f:
            src = f.read()
        tree = ast.parse(src)
        fn = next(
            n for n in ast.walk(tree)
            if isinstance(n, ast.FunctionDef) and n.name == "reprocess_pdf"
        )
        self.fn = fn
        self.body = ast.get_source_segment(src, fn)

    def test_pin_precedes_first_fitz_open(self):
        pin = self.body.index("resolve_reprocess_pdf(user_id, gmail_id, pdf_path)")
        first_open = self.body.index("_fitz.open(")
        self.assertLess(pin, first_open)
        self.assertIn("pdf_path = resolve_reprocess_pdf(", self.body)

    def test_rejection_returns_error(self):
        i = self.body.index("except ToolPathRejected")
        self.assertIn('return {"error"', self.body[i:i + 300])

    def test_no_raw_glob_or_exists_check(self):
        self.assertNotIn("_glob.glob(", self.body)
        self.assertNotIn("os.path.exists(pdf_path)", self.body)

    def test_unlocked_path_uses_splitext(self):
        self.assertIn('os.path.splitext(pdf_path)[0] + "_unlocked.pdf"', self.body)
        self.assertNotIn('pdf_path.replace(".pdf"', self.body)


if __name__ == "__main__":  # pragma: no cover
    unittest.main(verbosity=2)
