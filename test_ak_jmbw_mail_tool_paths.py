"""ak-jmbw (2026-11-24): mail-processor file tools must not act on
model-chosen paths.

Before: get_pdf_pages did fitz.open(args["pdf_path"]) and save_attachment
did shutil.copy2(args["source_path"], ...) for ANY path the model sent
(the ak-m0f6 review measured fitz opening a .txt and returning its text);
`category` (a directory name) was only enum-checked by the tool schema.

After: the service pins the run's own PDF (`pinned_pdf_path`); the tools
act only on it, and only if its realpath is a regular .pdf under
<cwd>/tmp/ or <cwd>/claude_statements/<uid>/. Model paths are ignored
(mismatch logged as a flag); category is validated server-side; the
save destination is confined to attachments/<uid>/<category>/.
All files here are synthetic, created in a temp dir.
"""
from __future__ import annotations

import importlib.util
import logging
import os
import shutil
import sys
import tempfile
import types
import unittest
from unittest import mock

HERE = os.path.dirname(os.path.abspath(__file__))
sys.path.insert(0, HERE)

from utils.mail_tool_paths import (  # noqa: E402
    SAVE_ATTACHMENT_CATEGORIES, ToolPathRejected, model_path_differs,
    resolve_pinned_pdf, safe_attachment_dest,
)

UID = "TestUid_123"
OTHER_UID = "OtherUid_456"

try:
    import fitz  # noqa: F401
    _HAVE_FITZ = True
except ImportError:  # pragma: no cover
    _HAVE_FITZ = False


def _make_pdf(path, text="synthetic page"):
    if _HAVE_FITZ:
        import fitz
        d = fitz.open()
        d.new_page().insert_text((50, 50), text)
        d.save(path)
        d.close()
    else:  # pragma: no cover - minimal bytes; enough for path tests
        with open(path, "wb") as f:
            f.write(b"%PDF-1.4\n%%EOF\n")


class _Sandbox(unittest.TestCase):
    """A fake app cwd with tmp/, claude_statements/<uid>/, and a
    'secret' file outside both."""

    def setUp(self):
        self.root = os.path.realpath(tempfile.mkdtemp(prefix="akjmbw_"))
        self.app = os.path.join(self.root, "app")
        self.tmp = os.path.join(self.app, "tmp")
        self.stmts = os.path.join(self.app, "claude_statements", UID, "bank_statement")
        self.other = os.path.join(self.app, "claude_statements", OTHER_UID)
        for d in (self.tmp, self.stmts, self.other):
            os.makedirs(d)
        self.run_pdf = os.path.join(self.tmp, "run.pdf")
        _make_pdf(self.run_pdf, "run statement")
        self.stmt_pdf = os.path.join(self.stmts, "s.pdf")
        _make_pdf(self.stmt_pdf, "persisted statement")
        self.secret = os.path.join(self.root, "secret.env")
        with open(self.secret, "w") as f:
            f.write("SYNTHETIC_SECRET=1\n")
        self.secret_pdf = os.path.join(self.root, "outside.pdf")
        _make_pdf(self.secret_pdf, "outside")
        self._cwd = os.getcwd()
        os.chdir(self.app)

    def tearDown(self):
        os.chdir(self._cwd)
        shutil.rmtree(self.root, ignore_errors=True)


class TestResolvePinnedPdf(_Sandbox):

    def test_run_tmp_pdf_accepted(self):
        self.assertEqual(resolve_pinned_pdf(self.run_pdf, UID), self.run_pdf)

    def test_persisted_statement_accepted(self):
        self.assertEqual(resolve_pinned_pdf(self.stmt_pdf, UID), self.stmt_pdf)

    def test_relative_path_resolved_against_cwd(self):
        self.assertEqual(resolve_pinned_pdf("tmp/run.pdf", UID), self.run_pdf)

    def _rejects(self, path, uid=UID):
        with self.assertRaises(ToolPathRejected):
            resolve_pinned_pdf(path, uid)

    def test_no_pinned_pdf_rejected(self):
        self._rejects(None)
        self._rejects("")

    def test_outside_roots_rejected(self):
        self._rejects(self.secret_pdf)

    def test_traversal_rejected(self):
        self._rejects(os.path.join(self.tmp, "..", "..", "outside.pdf"))
        self._rejects("tmp/../../outside.pdf")

    def test_other_users_statements_rejected(self):
        p = os.path.join(self.other, "x.pdf")
        _make_pdf(p)
        self._rejects(p)

    def test_symlink_escaping_root_rejected(self):
        link = os.path.join(self.tmp, "evil.pdf")
        os.symlink(self.secret, link)       # .pdf name, target outside
        self._rejects(link)
        link2 = os.path.join(self.tmp, "evil2.pdf")
        os.symlink(self.secret_pdf, link2)  # even a real PDF outside
        self._rejects(link2)

    def test_symlink_within_root_allowed(self):
        link = os.path.join(self.tmp, "alias.pdf")
        os.symlink(self.stmt_pdf, link)
        self.assertEqual(resolve_pinned_pdf(link, UID), self.stmt_pdf)

    def test_symlinked_root_dir_escape_rejected(self):
        # tmp/sub -> outside dir containing a pdf
        out_dir = os.path.join(self.root, "outdir")
        os.makedirs(out_dir)
        _make_pdf(os.path.join(out_dir, "y.pdf"))
        os.symlink(out_dir, os.path.join(self.tmp, "sub"))
        self._rejects(os.path.join(self.tmp, "sub", "y.pdf"))

    def test_non_pdf_rejected(self):
        txt = os.path.join(self.tmp, "notes.txt")
        with open(txt, "w") as f:
            f.write("x")
        self._rejects(txt)

    def test_missing_or_directory_rejected(self):
        self._rejects(os.path.join(self.tmp, "nope.pdf"))
        d = os.path.join(self.tmp, "dir.pdf")
        os.makedirs(d)
        self._rejects(d)

    def test_invalid_uid_rejected(self):
        for bad in ("../" + UID, "a/b", "", None):
            self._rejects(self.run_pdf, uid=bad)

    def test_rejection_message_does_not_echo_path(self):
        try:
            resolve_pinned_pdf(self.secret_pdf, UID)
        except ToolPathRejected as exc:
            self.assertNotIn(self.root, str(exc))

    def test_model_path_differs(self):
        self.assertFalse(model_path_differs(None, self.run_pdf))
        self.assertFalse(model_path_differs(self.run_pdf, self.run_pdf))
        self.assertTrue(model_path_differs(self.secret, self.run_pdf))


class TestSafeAttachmentDest(_Sandbox):

    def test_valid(self):
        d = safe_attachment_dest(UID, "bank_statement", "jan.pdf")
        self.assertEqual(
            d, os.path.join(self.app, "attachments", UID, "bank_statement", "jan.pdf"))

    def test_category_traversal_rejected(self):
        for cat in ("../../etc", "..", "bank_statement/../../x", "", None, "unknown"):
            with self.assertRaises(ToolPathRejected, msg=cat):
                safe_attachment_dest(UID, cat, "a.pdf")

    def test_filename_traversal_neutralized(self):
        for name in ("../../evil.pdf", "..\\..\\evil.pdf", "/abs/evil.pdf"):
            d = safe_attachment_dest(UID, "bank_statement", name)
            self.assertEqual(os.path.dirname(d),
                             os.path.join(self.app, "attachments", UID, "bank_statement"))

    def test_empty_filename_rejected(self):
        for name in ("", "..", "/", None):
            with self.assertRaises(ToolPathRejected, msg=name):
                safe_attachment_dest(UID, "bank_statement", name)

    def test_categories_match_tool_schema(self):
        import ast
        with open(os.path.join(HERE, "services", "mailProcessorTools.py")) as f:
            src = f.read()
        tree = ast.parse(src)
        enums = []
        for node in ast.walk(tree):
            if isinstance(node, ast.Dict):
                keys = [getattr(k, "value", None) for k in node.keys]
                if "enum" in keys and "description" in keys:
                    v = node.values[keys.index("enum")]
                    vals = [e.value for e in v.elts]
                    if "bank_statement" in vals:
                        enums.append(set(vals))
        self.assertEqual(enums, [set(SAVE_ATTACHMENT_CATEGORIES)])


def _load_executor():
    """Import the executor with its Flask/DB imports stubbed (scoped)."""
    def mod(name, **attrs):
        m = types.ModuleType(name)
        for k, v in attrs.items():
            setattr(m, k, v)
        return m

    class _L:
        def __init__(self, name="x"):
            self._l = logging.getLogger("akjmbw.executor")

        def get_logger(self):
            return self._l

    class _Response:  # distinct type: dict results must not match it
        pass

    stubs = {
        "flask": mod("flask", g=types.SimpleNamespace(), Response=_Response,
                     jsonify=lambda *a, **k: None),
        "enums.EPGEnum": mod("enums.EPGEnum", EPGEnum=object),
        "enums.MsnEnum": mod("enums.MsnEnum", MSNENUM=object),
        "enums.TransactionTypeEnum": mod("enums.TransactionTypeEnum",
                                         TransactionTypeEnum=object),
        "models.transactions": mod("models.transactions",
                                   ProcessingMethod=object),
        "utils.GenericUtils": mod("utils.GenericUtils", GenericUtil=object),
        "utils.logger": mod("utils.logger", Logger=_L),
    }
    patcher = mock.patch.dict(sys.modules, stubs)
    patcher.start()
    spec = importlib.util.spec_from_file_location(
        "akjmbw_executor",
        os.path.join(HERE, "services", "mailProcessorToolExecutor.py"))
    m = importlib.util.module_from_spec(spec)
    sys.modules["akjmbw_executor"] = m
    spec.loader.exec_module(m)
    return m, patcher


@unittest.skipUnless(_HAVE_FITZ, "PyMuPDF not installed")
class TestExecutorHandlers(_Sandbox):

    def setUp(self):
        super().setUp()
        self.ex, self._p = _load_executor()

    def tearDown(self):
        self._p.stop()
        super().tearDown()

    def test_get_pdf_pages_ignores_model_path_and_serves_run_pdf(self):
        opened = []
        real_open = self.ex.fitz.open

        def spy(path, *a, **k):
            opened.append(os.path.realpath(path))
            return real_open(path, *a, **k)
        with mock.patch.object(self.ex.fitz, "open", spy), \
                self.assertLogs("akjmbw.executor", level="WARNING") as cm:
            out = self.ex.execute_mail_tool(
                "get_pdf_pages", {"pdf_path": self.secret, "page_number": 1},
                UID, pinned_pdf_path=self.run_pdf)
        self.assertTrue(out.get("_image_content"), out)
        self.assertEqual(opened, [self.run_pdf])        # never the decoy
        self.assertNotIn(self.secret, opened)
        self.assertIn("different file", "\n".join(cm.output))

    def test_get_pdf_pages_without_pinned_pdf_refuses_and_opens_nothing(self):
        with mock.patch.object(self.ex.fitz, "open") as fo:
            out = self.ex.execute_mail_tool(
                "get_pdf_pages", {"pdf_path": self.secret}, UID)
        fo.assert_not_called()
        self.assertIn("refused", out["error"])

    def test_get_pdf_pages_rejects_pinned_outside_root(self):
        with mock.patch.object(self.ex.fitz, "open") as fo:
            out = self.ex.execute_mail_tool(
                "get_pdf_pages", {"pdf_path": self.secret_pdf}, UID,
                pinned_pdf_path=self.secret_pdf)
        fo.assert_not_called()
        self.assertIn("refused", out["error"])

    def test_save_attachment_copies_run_pdf_not_model_source(self):
        out = self.ex.execute_mail_tool(
            "save_attachment",
            {"source_path": self.secret, "category": "bank_statement",
             "filename": "../../jan.pdf"},
            UID, pinned_pdf_path=self.run_pdf)
        dest = os.path.join(self.app, "attachments", UID, "bank_statement", "jan.pdf")
        self.assertEqual(out.get("saved_path"), dest, out)
        with open(dest, "rb") as a, open(self.run_pdf, "rb") as b:
            self.assertEqual(a.read(), b.read())
        self.assertFalse(os.path.exists(os.path.join(self.root, "jan.pdf")))

    def test_save_attachment_category_traversal_refused(self):
        out = self.ex.execute_mail_tool(
            "save_attachment",
            {"source_path": self.run_pdf, "category": "../../../tmpx",
             "filename": "a.pdf"},
            UID, pinned_pdf_path=self.run_pdf)
        self.assertIn("refused", out["error"])
        self.assertFalse(os.path.exists(os.path.join(self.root, "tmpx")))

    def test_save_attachment_without_pinned_pdf_refused(self):
        out = self.ex.execute_mail_tool(
            "save_attachment",
            {"source_path": self.run_pdf, "category": "bank_statement",
             "filename": "a.pdf"}, UID)
        self.assertIn("refused", out["error"])


class TestServiceWiring(unittest.TestCase):
    """The PDF-chunk runs pass their own pdf_path as the pin; the text
    (no-PDF) run passes none, so the file tools refuse there."""

    def setUp(self):
        with open(os.path.join(HERE, "services", "mailProcessorService.py")) as f:
            self.src = f.read()

    def test_handler_forwards_pin(self):
        start = self.src.index("def _build_sdk_tools(")
        sig = self.src[start:self.src.index("):", start)]
        self.assertIn("pinned_pdf_path=None", sig)
        body = self.src[start:start + 6000]
        self.assertIn("pinned_pdf_path=pinned_pdf_path,", body)

    def test_pdf_chunk_sites_pin_their_pdf(self):
        fn = self.src[self.src.index("async def _run_pdf_chunk_async(self, pdf_path"):]
        fn = fn[:fn.index("\n    def ", 10)] if "\n    def " in fn[10:] else fn
        calls = fn.count("self._build_sdk_tools(")
        self.assertEqual(calls, 3)
        self.assertEqual(fn.count("pinned_pdf_path=pdf_path"), 3)

    def test_text_mail_site_has_no_pin(self):
        # The text-email agent processes no PDF, so it gets no pin (its
        # file tools are also scoped out since ak-n0cg).
        i = self.src.index("tool_names=TEXT_EMAIL_TOOL_NAMES")
        call = self.src[self.src.rindex("self._build_sdk_tools(", 0, i):i + 60]
        self.assertNotIn("pinned_pdf_path", call)

if __name__ == "__main__":  # pragma: no cover
    unittest.main(verbosity=2)
