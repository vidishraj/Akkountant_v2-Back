"""ak-nyd regression tests — per-file PDF password override.

MVP-scope JSON store for per-file password overrides. See
utils/per_file_passwords.py for design. These tests exercise the
pure-Python lookup/upsert/remove path against a temp config file.
"""

import json
import os
import sys
import tempfile
import unittest

sys.path.insert(0, os.path.dirname(os.path.abspath(__file__)))

from utils.per_file_passwords import (
    add_password,
    list_entries,
    lookup_password,
    remove_password,
)


class _TmpStoreCase(unittest.TestCase):
    """Base test case that provides a temp JSON config path for each
    test."""

    def setUp(self):
        fd, self.path = tempfile.mkstemp(suffix=".json")
        os.close(fd)
        os.remove(self.path)  # start clean

    def tearDown(self):
        try:
            os.remove(self.path)
        except FileNotFoundError:
            pass


class TestLookupOnEmpty(_TmpStoreCase):
    """Missing / empty config returns None. Never blows up."""

    def test_missing_file_returns_none(self):
        self.assertIsNone(lookup_password(gmail_id="gm1", path=self.path))
        self.assertIsNone(lookup_password(file_id="f1", path=self.path))

    def test_neither_key_returns_none(self):
        add_password("pw", gmail_id="gm1", path=self.path)
        self.assertIsNone(lookup_password(path=self.path))


class TestUpsert(_TmpStoreCase):
    """add_password writes to disk; lookup finds it."""

    def test_add_by_gmail_id(self):
        add_password("secret1", gmail_id="gm1", path=self.path)
        self.assertEqual(
            lookup_password(gmail_id="gm1", path=self.path), "secret1"
        )

    def test_add_by_file_id(self):
        add_password("secret2", file_id="f1", path=self.path)
        self.assertEqual(
            lookup_password(file_id="f1", path=self.path), "secret2"
        )

    def test_add_by_both_writes_two_entries(self):
        add_password("secret3", gmail_id="gm2", file_id="f2", path=self.path)
        self.assertEqual(
            lookup_password(gmail_id="gm2", path=self.path), "secret3"
        )
        self.assertEqual(
            lookup_password(file_id="f2", path=self.path), "secret3"
        )
        entries = list_entries(path=self.path)
        self.assertEqual(len(entries), 2)
        self.assertIn("gmail_id:gm2", entries)
        self.assertIn("file_id:f2", entries)

    def test_upsert_overwrites(self):
        add_password("oldpw", gmail_id="gm3", path=self.path)
        add_password("newpw", gmail_id="gm3", path=self.path)
        self.assertEqual(
            lookup_password(gmail_id="gm3", path=self.path), "newpw"
        )

    def test_empty_password_raises(self):
        with self.assertRaises(ValueError):
            add_password("", gmail_id="gm4", path=self.path)

    def test_no_key_raises(self):
        with self.assertRaises(ValueError):
            add_password("pw", path=self.path)


class TestLookupPreference(_TmpStoreCase):
    """When both gmail_id and file_id have entries, gmail_id wins in
    the two-arg lookup — matches the reprocess_pdf caller shape."""

    def test_gmail_id_preferred(self):
        add_password("gm_pw", gmail_id="gm5", path=self.path)
        add_password("file_pw", file_id="f5", path=self.path)
        got = lookup_password(gmail_id="gm5", file_id="f5", path=self.path)
        self.assertEqual(got, "gm_pw")

    def test_falls_back_to_file_id_if_no_gmail_id_entry(self):
        add_password("file_pw", file_id="f6", path=self.path)
        got = lookup_password(gmail_id="gm-not-present", file_id="f6",
                              path=self.path)
        self.assertEqual(got, "file_pw")


class TestRemove(_TmpStoreCase):
    """remove_password strips entries idempotently."""

    def test_remove_by_gmail_id(self):
        add_password("pw", gmail_id="gm7", path=self.path)
        self.assertTrue(remove_password(gmail_id="gm7", path=self.path))
        self.assertIsNone(lookup_password(gmail_id="gm7", path=self.path))

    def test_remove_missing_is_noop(self):
        self.assertFalse(remove_password(gmail_id="never", path=self.path))

    def test_remove_by_both(self):
        add_password("pw", gmail_id="gm8", file_id="f8", path=self.path)
        self.assertTrue(
            remove_password(gmail_id="gm8", file_id="f8", path=self.path)
        )
        self.assertIsNone(lookup_password(gmail_id="gm8", path=self.path))
        self.assertIsNone(lookup_password(file_id="f8", path=self.path))


class TestCorruptConfig(_TmpStoreCase):
    """Corrupt / non-dict JSON must not raise — defensive default."""

    def test_non_dict_json_returns_empty(self):
        with open(self.path, "w") as f:
            f.write("[1, 2, 3]")
        self.assertIsNone(
            lookup_password(gmail_id="anything", path=self.path)
        )

    def test_garbage_returns_empty(self):
        with open(self.path, "w") as f:
            f.write("{ this is not valid json")
        self.assertIsNone(
            lookup_password(gmail_id="anything", path=self.path)
        )

    def test_corrupt_config_add_still_works(self):
        """A corrupt file gets overwritten cleanly on the next
        add_password call — we don't want prior corruption to block
        recovery."""
        with open(self.path, "w") as f:
            f.write("{ broken")
        add_password("pw", gmail_id="gm_recovery", path=self.path)
        self.assertEqual(
            lookup_password(gmail_id="gm_recovery", path=self.path), "pw"
        )


class TestEnvPathResolution(unittest.TestCase):
    """AK_PER_FILE_PASSWORDS_PATH env var overrides the default cwd
    lookup."""

    def test_env_var_takes_precedence(self):
        fd, tmp = tempfile.mkstemp(suffix=".json")
        os.close(fd)
        os.remove(tmp)
        original = os.environ.get("AK_PER_FILE_PASSWORDS_PATH")
        try:
            os.environ["AK_PER_FILE_PASSWORDS_PATH"] = tmp
            # Explicit path=None → resolver picks up env var.
            add_password("envpw", gmail_id="gm_env")
            self.assertEqual(lookup_password(gmail_id="gm_env"), "envpw")
        finally:
            if original is not None:
                os.environ["AK_PER_FILE_PASSWORDS_PATH"] = original
            else:
                os.environ.pop("AK_PER_FILE_PASSWORDS_PATH", None)
            try:
                os.remove(tmp)
            except FileNotFoundError:
                pass


# ── ak-nyd v2: security hygiene (file/dir modes + default path) ─────


import stat


class TestFileModeIsOwnerOnly(_TmpStoreCase):
    """ak-nyd v2 MAJOR: passwords file MUST be 0o600 after every
    write. Umask can't override — _save_raw runs an explicit chmod
    BEFORE and AFTER the atomic rename."""

    def test_new_file_is_0o600(self):
        add_password("secret", gmail_id="gm_mode1", path=self.path)
        mode = stat.S_IMODE(os.stat(self.path).st_mode)
        self.assertEqual(
            mode, 0o600, f"expected 0o600, got 0o{mode:o}",
        )

    def test_updated_file_stays_0o600(self):
        """Simulate an operator/attacker widening the mode — the
        next write must tighten it back."""
        add_password("s1", gmail_id="gm_mode2", path=self.path)
        os.chmod(self.path, 0o644)
        add_password("s2", gmail_id="gm_mode2", path=self.path)
        mode = stat.S_IMODE(os.stat(self.path).st_mode)
        self.assertEqual(mode, 0o600)

    def test_remove_write_preserves_0o600(self):
        add_password("s", gmail_id="gm_a", file_id="f_a", path=self.path)
        os.chmod(self.path, 0o666)
        remove_password(gmail_id="gm_a", path=self.path)
        mode = stat.S_IMODE(os.stat(self.path).st_mode)
        self.assertEqual(mode, 0o600)


class TestParentDirCreated0o700(unittest.TestCase):
    """When _save_raw creates the parent dir it must be 0o700."""

    def setUp(self):
        self.tmpdir = tempfile.mkdtemp()
        self.path = os.path.join(
            self.tmpdir, "nested_ak_nyd_v2", "creds.json",
        )

    def tearDown(self):
        import shutil
        shutil.rmtree(self.tmpdir, ignore_errors=True)

    def test_parent_dir_is_0o700(self):
        add_password("s", gmail_id="gm_dir", path=self.path)
        parent = os.path.dirname(self.path)
        self.assertTrue(os.path.isdir(parent))
        mode = stat.S_IMODE(os.stat(parent).st_mode)
        self.assertEqual(
            mode, 0o700,
            f"expected parent dir 0o700, got 0o{mode:o}",
        )


class TestDefaultPathOutsideRepo(unittest.TestCase):
    """ak-nyd v2 MAJOR: v1 defaulted to ./per_file_passwords.json —
    landing plaintext creds next to the source tree with git
    watching. v2 default is ~/.config/akkountant/per_file_passwords.json."""

    def test_default_path_is_under_home_config(self):
        from utils.per_file_passwords import _default_config_path
        home = os.path.expanduser("~")
        expected = os.path.join(
            home, ".config", "akkountant", "per_file_passwords.json",
        )
        self.assertEqual(_default_config_path(), expected)

    def test_default_path_not_under_cwd(self):
        """Belt-and-braces — the default must NOT be a CWD-relative
        path even if the operator runs from an unusual dir."""
        from utils.per_file_passwords import _default_config_path
        default = _default_config_path()
        self.assertTrue(os.path.isabs(default))
        cwd = os.getcwd()
        self.assertFalse(
            default.startswith(cwd + os.sep),
            f"default path {default!r} unexpectedly under cwd {cwd!r}",
        )

    def test_resolve_config_path_env_override(self):
        """The env var still wins over the ak-nyd v2 default."""
        from utils.per_file_passwords import _resolve_config_path
        original = os.environ.get("AK_PER_FILE_PASSWORDS_PATH")
        try:
            os.environ["AK_PER_FILE_PASSWORDS_PATH"] = "/tmp/custom_creds.json"
            self.assertEqual(
                _resolve_config_path(), "/tmp/custom_creds.json"
            )
        finally:
            if original is not None:
                os.environ["AK_PER_FILE_PASSWORDS_PATH"] = original
            else:
                os.environ.pop("AK_PER_FILE_PASSWORDS_PATH", None)

    def test_resolve_config_path_no_env_uses_home_default(self):
        from utils.per_file_passwords import (
            _resolve_config_path, _default_config_path,
        )
        original = os.environ.get("AK_PER_FILE_PASSWORDS_PATH")
        try:
            os.environ.pop("AK_PER_FILE_PASSWORDS_PATH", None)
            self.assertEqual(_resolve_config_path(), _default_config_path())
        finally:
            if original is not None:
                os.environ["AK_PER_FILE_PASSWORDS_PATH"] = original


class TestGitignoreEntry(unittest.TestCase):
    """ak-nyd v2 defense-in-depth: the backend .gitignore blocks
    per_file_passwords.json at any depth so an operator running an
    old (pre-v2) CLI whose default path was CWD-relative can't
    accidentally commit the file."""

    def test_gitignore_blocks_per_file_passwords(self):
        gi = os.path.join(
            os.path.dirname(os.path.abspath(__file__)), ".gitignore",
        )
        self.assertTrue(os.path.exists(gi), ".gitignore missing")
        with open(gi, "r") as f:
            content = f.read()
        # Either bare-name or /-anchored entry is acceptable — reject
        # only if NEITHER is present.
        self.assertTrue(
            "per_file_passwords.json" in content,
            ".gitignore must include per_file_passwords.json entry"
        )


if __name__ == "__main__":
    print("ak-nyd per-file password override tests")
    print("=" * 60)
    unittest.main(verbosity=2, exit=False)
    print("=" * 60)
