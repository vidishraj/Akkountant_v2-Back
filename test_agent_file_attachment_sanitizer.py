"""ak-9dz: unit tests for services.agentFileAttachmentService pure guards.

Two boundaries pinned here:

  1. `sanitize_display_name` — user-visible filename traversal + shell
     safety. Every display_name from an LLM tool call flows through
     this before being appended to a disk path.

  2. `validate_source_path` — ak-9dz v2 source-path allowlist + size
     cap. Guards the write path against prompt-injection-driven
     arbitrary-file-read (the CRITICAL from reviewer's v1 audit): a
     compromised LLM told to `attach_file_to_chat("/etc/passwd", ...)`
     must be rejected before any copy happens.

AST-lifts both from services/agentFileAttachmentService.py so the tests
run in bare env without pulling the services.Base_Service /
google.oauth2 / firebase chain. Same pattern as
test_wealth_digest_migration_gap.py + test_wealth_digest_parse_content.py.

Run:
    python3 -m unittest test_agent_file_attachment_sanitizer
    python3 -m pytest test_agent_file_attachment_sanitizer.py
"""

import ast
import os
import sys
import tempfile
import unittest


sys.path.insert(0, os.path.dirname(os.path.abspath(__file__)))


def _ast_lift_service_pure_guards():
    """AST-lift the pure guards from services/agentFileAttachmentService.py.

    Kept nodes (everything else — imports of Base_Service, models,
    utils.logger, and the class body — is dropped so bare env can
    exec the subset):

      * `from __future__ import annotations` — PEP 604 unions in
        signatures don't evaluate at import time on Python 3.9.
      * `import os`, `import re` — runtime deps of the guards.
      * `_SANITIZE_RE`, `_LEADING_DOTS_RE` — sanitizer regexes.
      * `_ALLOWED_SCRATCH_BASES` — allowlist constant (AnnAssign
        because it has a `tuple[str, ...]` type annotation).
      * `_MAX_ATTACHMENT_SIZE_BYTES` — size-cap constant (Assign).
      * `sanitize_display_name` — the sanitizer function.
      * `validate_source_path` — the v2 source-path + size guard.

    Returns (sanitize_display_name, validate_source_path,
             ALLOWED_SCRATCH_BASES, MAX_ATTACHMENT_SIZE_BYTES).
    """
    src_path = os.path.join(
        os.path.dirname(os.path.abspath(__file__)),
        "services", "agentFileAttachmentService.py",
    )
    with open(src_path) as fh:
        src = fh.read()
    tree = ast.parse(src)
    _WANTED_ASSIGN_NAMES = {
        "_SANITIZE_RE", "_LEADING_DOTS_RE",
        "_ALLOWED_SCRATCH_BASES", "_MAX_ATTACHMENT_SIZE_BYTES",
    }
    _WANTED_FUNCS = {"sanitize_display_name", "validate_source_path"}
    kept = []
    for node in tree.body:
        if isinstance(node, ast.ImportFrom) and node.module == "__future__":
            kept.append(node)
            continue
        # Keep `import os` + `import re` — the guards' only runtime deps.
        if isinstance(node, ast.Import):
            if any(alias.name in ("os", "re") for alias in node.names):
                kept.append(node)
            continue
        # Plain assignments (e.g. `_MAX_ATTACHMENT_SIZE_BYTES = ...`).
        if isinstance(node, ast.Assign) and len(node.targets) == 1:
            target = node.targets[0]
            if isinstance(target, ast.Name) and target.id in _WANTED_ASSIGN_NAMES:
                kept.append(node)
                continue
        # Annotated assignments (e.g. `_ALLOWED_SCRATCH_BASES: tuple[str, ...] = ...`).
        if isinstance(node, ast.AnnAssign):
            target = node.target
            if isinstance(target, ast.Name) and target.id in _WANTED_ASSIGN_NAMES:
                kept.append(node)
                continue
        if isinstance(node, ast.FunctionDef) and node.name in _WANTED_FUNCS:
            kept.append(node)
    tree.body = kept
    ns = {"__name__": "test_agent_file_attachment_service_ast_lift"}
    exec(compile(tree, "<service-pure-guards-AST-lift>", "exec"), ns)
    return (
        ns["sanitize_display_name"],
        ns["validate_source_path"],
        ns["_ALLOWED_SCRATCH_BASES"],
        ns["_MAX_ATTACHMENT_SIZE_BYTES"],
    )


try:
    try:
        from services.agentFileAttachmentService import (  # noqa
            sanitize_display_name,
            validate_source_path,
            _ALLOWED_SCRATCH_BASES as ALLOWED_SCRATCH_BASES,
            _MAX_ATTACHMENT_SIZE_BYTES as MAX_ATTACHMENT_SIZE_BYTES,
        )
    except Exception:
        (
            sanitize_display_name,
            validate_source_path,
            ALLOWED_SCRATCH_BASES,
            MAX_ATTACHMENT_SIZE_BYTES,
        ) = _ast_lift_service_pure_guards()
    _IMPORT_OK = True
    _SKIP_REASON = ""
except Exception as _exc:  # pragma: no cover
    _IMPORT_OK = False
    _SKIP_REASON = f"import chain unavailable: {_exc}"


class TestImportChannel(unittest.TestCase):
    """Un-decorated guard — fails LOUD if both real-import + AST-lift break."""

    def test_import_channel_is_live(self):
        self.assertTrue(_IMPORT_OK, _SKIP_REASON)


@unittest.skipUnless(_IMPORT_OK, _SKIP_REASON)
class TestSanitizeDisplayName(unittest.TestCase):
    """Nine cases pinning the traversal + shell-safety boundary."""

    # ── happy path ────────────────────────────────────────────────────

    def test_alphanumeric_passes_through(self):
        self.assertEqual(
            sanitize_display_name("july_2026_invoices.csv"),
            "july_2026_invoices.csv",
        )

    def test_spaces_preserved(self):
        """Spaces are whitelisted so 'July 2026 Invoices.csv' stays
        readable in the download card + Content-Disposition."""
        self.assertEqual(
            sanitize_display_name("July 2026 Invoices.csv"),
            "July 2026 Invoices.csv",
        )

    def test_hyphen_and_dot_preserved(self):
        self.assertEqual(
            sanitize_display_name("portfolio-snapshot.2026-08-21.json"),
            "portfolio-snapshot.2026-08-21.json",
        )

    # ── traversal blocked ─────────────────────────────────────────────

    def test_forward_slash_replaced(self):
        """`/` → replaced with `_`. Once `/` is gone, `..` as a substring
        is just a filename component — no traversal possible because
        the sanitized string is a single basename appended to a fixed
        parent dir. What matters is (a) no `/` chars survive, (b) the
        string doesn't START with `..` (which is stripped explicitly
        by the leading-dots pass)."""
        out = sanitize_display_name("../../etc/passwd")
        self.assertNotIn("/", out)
        self.assertFalse(out.startswith(".."))

    def test_backslash_replaced(self):
        """Windows-style path separators also stripped."""
        out = sanitize_display_name("..\\..\\etc\\passwd")
        self.assertNotIn("\\", out)

    def test_leading_dots_stripped(self):
        """Leading `.` chars → stripped (blocks `..`, hidden-file
        creation like `.env`)."""
        self.assertFalse(sanitize_display_name("...hidden").startswith("."))

    # ── shell metachars ───────────────────────────────────────────────

    def test_shell_metachars_replaced(self):
        """Backtick / $ / ; / | / & / quotes → replaced with `_`."""
        raw = "evil `cmd`; rm -rf $HOME | tee 'x' & \"y\""
        out = sanitize_display_name(raw)
        for bad in ("`", "$", ";", "|", "&", "'", "\""):
            self.assertNotIn(bad, out)

    # ── edge cases ────────────────────────────────────────────────────

    def test_empty_raises(self):
        with self.assertRaises(ValueError):
            sanitize_display_name("")

    def test_whitespace_only_raises(self):
        with self.assertRaises(ValueError):
            sanitize_display_name("   \t\n  ")

    def test_all_illegal_produces_empty_raises(self):
        """Input that sanitizes to empty string (e.g. only leading dots +
        path separators + illegal chars) must raise ValueError, not
        return an empty basename that would collide with the uuid_
        prefix on disk."""
        with self.assertRaises(ValueError):
            sanitize_display_name("....")

    def test_length_capped_to_200(self):
        """Very long display_name capped to 200 chars to stay under
        filesystem NAME_MAX (typically 255) after the uuid_ prefix."""
        raw = "a" * 500
        out = sanitize_display_name(raw)
        self.assertLessEqual(len(out), 200)


@unittest.skipUnless(_IMPORT_OK, _SKIP_REASON)
class TestValidateSourcePath(unittest.TestCase):
    """ak-9dz v2: source-path allowlist + size cap.

    Pins the arbitrary-file-read guard reviewer flagged as CRITICAL on
    da8f21e. Every case here corresponds to a real exploit shape the
    prompt-injection surface can exercise on the Investment / Freelance
    agents (they process untrusted invoice / receipt / customer text).

    Uses `/tmp` fixtures rather than mocking `os.path.*` because
    realpath's symlink resolution IS the security property under test;
    mocking it would test the mock, not the guard.
    """

    # ── reject: paths outside the allowlist ───────────────────────────

    def test_rejects_etc_passwd(self):
        """The canonical exfiltration target — must be rejected before
        any copy or stat happens on the sensitive file."""
        with self.assertRaises(ValueError) as ctx:
            validate_source_path("/etc/passwd")
        self.assertIn("outside allowed scratch dirs", str(ctx.exception))
        # Error surfaces the realpath so a legit-usage misconfig can
        # be diagnosed from the ToolValidationError message the LLM
        # sees on retry.
        self.assertIn("/etc/passwd", str(ctx.exception))

    def test_rejects_home_credentials(self):
        """Firebase service-account JSON / `.env` / repo config all
        live under $HOME on this deployment. Guard must reject even
        though the file is trivially readable by opc."""
        with self.assertRaises(ValueError):
            validate_source_path("/home/opc/.credentials.json")

    def test_rejects_var_log(self):
        """`/var/log/messages` etc. — a prompt-injected agent could
        both exfil server logs AND blow the 50 MB cap in one call."""
        with self.assertRaises(ValueError):
            validate_source_path("/var/log/messages")

    def test_rejects_dot_dot_traversal_from_tmp(self):
        """`/tmp/../etc/passwd` looks scratch-dir at a glance but
        realpath resolves to `/etc/passwd` → rejected."""
        with self.assertRaises(ValueError) as ctx:
            validate_source_path("/tmp/../etc/passwd")
        # Confirm realpath resolved BEFORE the check (the surfaced
        # realpath should be the escaped path, not the raw input).
        self.assertIn("/etc/passwd", str(ctx.exception))

    def test_rejects_lookalike_prefix(self):
        """`/tmpfoo/x` must NOT match `/tmp/` — the trailing slash on
        the allowlist entry is load-bearing. Guard rejects even
        though the file doesn't exist (allowlist check runs first)."""
        with self.assertRaises(ValueError):
            validate_source_path("/tmpfoo/x")

    def test_rejects_symlink_escape(self):
        """A `/tmp/`-scoped symlink that TARGETS `/etc/passwd` must
        be rejected — realpath resolves the symlink before the
        startswith check. Real filesystem symlink, not a mock."""
        with tempfile.TemporaryDirectory(dir="/tmp") as tmpdir:
            link_path = os.path.join(tmpdir, "innocuous_looking_link")
            os.symlink("/etc/passwd", link_path)
            with self.assertRaises(ValueError) as ctx:
                validate_source_path(link_path)
            # realpath followed the symlink → error surfaces the
            # resolved target, not the /tmp-scoped link path.
            self.assertIn("/etc/passwd", str(ctx.exception))

    # ── reject: oversize ──────────────────────────────────────────────

    def test_rejects_oversize_file(self):
        """A `/tmp/`-legit file that exceeds the size cap must raise
        BEFORE any copy. Fake the size by writing sparsely so this
        test doesn't actually allocate 51 MB of disk."""
        with tempfile.NamedTemporaryFile(
            dir="/tmp", suffix=".bin", delete=False
        ) as fh:
            fh.seek(MAX_ATTACHMENT_SIZE_BYTES + 1)
            fh.write(b"x")
            oversize_path = fh.name
        try:
            with self.assertRaises(ValueError) as ctx:
                validate_source_path(oversize_path)
            self.assertIn("exceeds limit", str(ctx.exception))
        finally:
            os.remove(oversize_path)

    # ── reject: missing ───────────────────────────────────────────────

    def test_rejects_missing_file(self):
        """Missing file → FileNotFoundError (distinct from allowlist
        ValueError so the executor's error mapping keeps them apart)."""
        with self.assertRaises(FileNotFoundError):
            validate_source_path("/tmp/does_not_exist_ak9dz_v2.bin")

    # ── accept: legitimate /tmp/ scratch ──────────────────────────────

    def test_accepts_legit_tmp_file(self):
        """Baseline: a normal `/tmp/`-scoped file created by the agent
        (e.g. `tempfile.NamedTemporaryFile`) must pass and return the
        realpath for the caller to copy from."""
        with tempfile.NamedTemporaryFile(
            dir="/tmp", suffix=".csv", delete=False
        ) as fh:
            fh.write(b"amount,vendor\n100,Test\n")
            legit_path = fh.name
        try:
            result = validate_source_path(legit_path)
            self.assertTrue(result.startswith("/tmp/"))
            self.assertTrue(os.path.isfile(result))
        finally:
            os.remove(legit_path)

    def test_accepts_at_exact_size_limit(self):
        """Boundary: file EXACTLY at MAX_ATTACHMENT_SIZE_BYTES must be
        accepted; only strictly-greater triggers the guard. Confirms
        the `>` (not `>=`) comparison shape."""
        with tempfile.NamedTemporaryFile(
            dir="/tmp", suffix=".bin", delete=False
        ) as fh:
            if MAX_ATTACHMENT_SIZE_BYTES > 0:
                fh.seek(MAX_ATTACHMENT_SIZE_BYTES - 1)
                fh.write(b"x")
            boundary_path = fh.name
        try:
            # Exactly at limit → accepted.
            result = validate_source_path(boundary_path)
            self.assertEqual(
                os.path.getsize(result), MAX_ATTACHMENT_SIZE_BYTES
            )
        finally:
            os.remove(boundary_path)


if __name__ == "__main__":  # pragma: no cover
    unittest.main(verbosity=2)
