"""ak-oauth-refresh regression tests.

Pure-Python coverage of utils.oauth_refresh. Covers:
  - Expiry window math (boundary cases)
  - credentials.json round-trip preserves non-token fields
  - Refresh POST failure paths leave credentials.json untouched
  - Missing / malformed credentials return structured errors
  - 0o600 mode after write
  - Env-var overrides for URL / client_id
  - refresh_now vs refresh_if_expiring window semantics
  - Response field aliasing (snake_case vs camelCase)
"""

import json
import os
import stat
import sys
import tempfile
import unittest

sys.path.insert(0, os.path.dirname(os.path.abspath(__file__)))

from utils import oauth_refresh
from utils.oauth_refresh import (
    OAuthRefreshError,
    _apply_new_tokens,
    _extract_new_tokens,
    _sanitize_error_body,
    is_expiring,
    refresh_if_expiring,
    refresh_now,
    seconds_until_expiry,
)


# Fixed reference time. `_EXPIRES_AT_MS` is the Q2-observed expiresAt
# (2026-07-30 13:29:47 UTC). `_NOW` is set exactly 2h before that so
# a 3h window guarantees is_expiring() → True and refresh fires,
# while a 5-minute window guarantees is_expiring() → False (still_valid).
_EXPIRES_AT_MS = 1785418187947             # ms
_NOW = (_EXPIRES_AT_MS / 1000.0) - 7200.0  # 2h before expiry


def _make_creds() -> dict:
    """Fresh credentials.json shape matching Q2 findings."""
    return {
        "claudeAiOauth": {
            "accessToken": "old-access-token",
            "refreshToken": "valid-refresh-token",
            "expiresAt": _EXPIRES_AT_MS,
            "scopes": [
                "user:file_upload",
                "user:inference",
                "user:mcp_servers",
                "user:profile",
                "user:sessions:claude_code",
            ],
            "subscriptionType": "max",
            "rateLimitTier": "default_claude_max_20x",
        }
    }


def _write_creds(dir_: str, data: dict) -> str:
    path = os.path.join(dir_, ".credentials.json")
    with open(path, "w", encoding="utf-8") as f:
        json.dump(data, f)
    os.chmod(path, 0o600)
    return path


# ── Expiry math ─────────────────────────────────────────────────────


class TestExpiryMath(unittest.TestCase):
    def test_seconds_until_expiry_positive_when_future(self):
        oauth = _make_creds()["claudeAiOauth"]
        remaining = seconds_until_expiry(oauth, now=_NOW)
        # Exactly 2h remaining by fixture construction.
        self.assertAlmostEqual(remaining, 7200.0, places=0)

    def test_seconds_until_expiry_negative_when_past(self):
        oauth = _make_creds()["claudeAiOauth"]
        # Way in the future — token expired long ago.
        remaining = seconds_until_expiry(oauth, now=_NOW + 3600 * 24 * 365)
        self.assertLess(remaining, 0)

    def test_seconds_until_expiry_missing_field_treated_as_expired(self):
        self.assertEqual(seconds_until_expiry({}, now=_NOW), 0.0)

    def test_seconds_until_expiry_non_numeric_expiresAt(self):
        self.assertEqual(
            seconds_until_expiry({"expiresAt": "not-a-number"}, now=_NOW),
            0.0,
        )

    def test_is_expiring_true_when_inside_window(self):
        oauth = _make_creds()["claudeAiOauth"]
        # 3-hour window covers the 2h remaining.
        self.assertTrue(
            is_expiring(oauth, window_seconds=3 * 3600, now=_NOW)
        )

    def test_is_expiring_false_when_outside_window(self):
        oauth = _make_creds()["claudeAiOauth"]
        # 5-minute default window — token has 2h left, not expiring yet.
        self.assertFalse(
            is_expiring(oauth, window_seconds=300, now=_NOW)
        )


# ── refresh_if_expiring — still_valid path ─────────────────────────


class TestStillValidPath(unittest.TestCase):
    def test_no_post_when_outside_window(self):
        with tempfile.TemporaryDirectory() as d:
            path = _write_creds(d, _make_creds())
            called = []

            def http_post(url, body):
                called.append((url, body))
                return {}

            result = refresh_if_expiring(
                path, now=_NOW, window_seconds=300, http_post=http_post
            )
            self.assertEqual(result["status"], "still_valid")
            self.assertEqual(called, [])
            # File untouched.
            with open(path) as f:
                self.assertEqual(json.load(f), _make_creds())


# ── refresh_if_expiring — refreshed happy path ─────────────────────


class TestRefreshedHappyPath(unittest.TestCase):
    def test_refresh_rewrites_file_and_preserves_other_fields(self):
        with tempfile.TemporaryDirectory() as d:
            path = _write_creds(d, _make_creds())

            def http_post(url, body):
                # Server rotates refresh_token too.
                return {
                    "access_token": "new-access-token",
                    "refresh_token": "new-refresh-token",
                    "expires_in": 3600,
                }

            result = refresh_if_expiring(
                path, now=_NOW, window_seconds=3 * 3600,
                http_post=http_post,
            )
            self.assertEqual(result["status"], "refreshed")
            self.assertEqual(result["expires_in_seconds"], 3600)

            with open(path) as f:
                new_data = json.load(f)
            oauth = new_data["claudeAiOauth"]
            self.assertEqual(oauth["accessToken"], "new-access-token")
            self.assertEqual(oauth["refreshToken"], "new-refresh-token")
            # New expiresAt = now + 3600 (ms)
            self.assertEqual(oauth["expiresAt"], int((_NOW + 3600) * 1000))
            # Non-token fields preserved.
            self.assertEqual(oauth["subscriptionType"], "max")
            self.assertEqual(oauth["rateLimitTier"], "default_claude_max_20x")
            self.assertEqual(
                oauth["scopes"],
                _make_creds()["claudeAiOauth"]["scopes"],
            )

    def test_refresh_preserves_old_refresh_token_when_server_omits(self):
        with tempfile.TemporaryDirectory() as d:
            path = _write_creds(d, _make_creds())

            def http_post(url, body):
                # Server does NOT rotate refresh_token.
                return {
                    "access_token": "new-access-token",
                    "expires_in": 3600,
                }

            result = refresh_if_expiring(
                path, now=_NOW, window_seconds=3 * 3600,
                http_post=http_post,
            )
            self.assertEqual(result["status"], "refreshed")

            with open(path) as f:
                oauth = json.load(f)["claudeAiOauth"]
            # Existing refresh token preserved.
            self.assertEqual(oauth["refreshToken"], "valid-refresh-token")

    def test_refresh_accepts_camelCase_response_fields(self):
        """Some proxies rewrite snake_case → camelCase. Helper accepts
        both."""
        with tempfile.TemporaryDirectory() as d:
            path = _write_creds(d, _make_creds())

            def http_post(url, body):
                return {
                    "accessToken": "camel-access",
                    "refreshToken": "camel-refresh",
                    "expiresIn": 1800,
                }

            result = refresh_if_expiring(
                path, now=_NOW, window_seconds=3 * 3600,
                http_post=http_post,
            )
            self.assertEqual(result["status"], "refreshed")
            with open(path) as f:
                oauth = json.load(f)["claudeAiOauth"]
            self.assertEqual(oauth["accessToken"], "camel-access")
            self.assertEqual(oauth["refreshToken"], "camel-refresh")

    def test_refresh_body_carries_correct_grant_and_client(self):
        """Verify the POST body has the right shape (Anthropic OAuth
        contract). v2 adds `scope` per CLI wire-shape observation.
        Direct binary evidence:
          grant_type:"refresh_token", refresh_token:T,
          client_id:OB().CLIENT_ID, scope:PgR.join(" ")
        """
        with tempfile.TemporaryDirectory() as d:
            path = _write_creds(d, _make_creds())
            seen = {}

            def http_post(url, body):
                seen["url"] = url
                seen["body"] = body
                return {"access_token": "x", "expires_in": 3600}

            refresh_if_expiring(
                path, now=_NOW, window_seconds=3 * 3600,
                http_post=http_post,
            )
            # v3: default URL matches PROD CLI (older SDK 0.1.11 /
            # CLI 2.0.57, Dec-4-2025). Newer Desktop CLI uses
            # platform.claude.com — token host is scoped by mint
            # source, so we point at the URL that issued this token.
            self.assertEqual(
                seen["url"], "https://console.anthropic.com/v1/oauth/token"
            )
            self.assertEqual(seen["body"]["grant_type"], "refresh_token")
            self.assertEqual(
                seen["body"]["refresh_token"], "valid-refresh-token"
            )
            self.assertEqual(
                seen["body"]["client_id"],
                "9d1c250a-e61b-44d9-88ed-5944d1962f5e",
            )
            # v2: scope field is space-separated from the cache's
            # granted scopes.
            self.assertIn("scope", seen["body"])
            self.assertEqual(
                seen["body"]["scope"],
                "user:file_upload user:inference user:mcp_servers "
                "user:profile user:sessions:claude_code",
            )


# ── v2 scope field ──────────────────────────────────────────────────


class TestScopeField(unittest.TestCase):
    """ak-oauth-refresh v2: POST body must carry `scope` field with
    granted scopes space-joined. Direct binary evidence from the CLI:
      grant_type:"refresh_token", refresh_token:T,
      client_id:OB().CLIENT_ID, scope:PgR.join(" ")
    """

    def _capture_body(self, creds: dict) -> dict:
        with tempfile.TemporaryDirectory() as d:
            path = _write_creds(d, creds)
            seen = {}

            def http_post(url, body):
                seen["body"] = body
                return {"access_token": "x", "expires_in": 3600}

            refresh_if_expiring(
                path, now=_NOW, window_seconds=3 * 3600,
                http_post=http_post,
            )
            return seen.get("body", {})

    def test_scope_present_and_space_separated(self):
        body = self._capture_body(_make_creds())
        self.assertIn("scope", body)
        parts = body["scope"].split(" ")
        # Must match the input order — join(" ") preserves order.
        self.assertEqual(
            parts,
            [
                "user:file_upload",
                "user:inference",
                "user:mcp_servers",
                "user:profile",
                "user:sessions:claude_code",
            ],
        )

    def test_scope_empty_when_scopes_missing(self):
        creds = _make_creds()
        del creds["claudeAiOauth"]["scopes"]
        body = self._capture_body(creds)
        # Field present but empty — mirrors CLI's join("") on empty
        # array. Server may reject; that's a legit surface for the
        # `failed` status.
        self.assertEqual(body.get("scope"), "")

    def test_scope_empty_when_scopes_empty_list(self):
        creds = _make_creds()
        creds["claudeAiOauth"]["scopes"] = []
        body = self._capture_body(creds)
        self.assertEqual(body.get("scope"), "")

    def test_scope_filters_non_string_entries(self):
        """Defensive: a malformed cache with a non-string scope entry
        shouldn't blow up the join or leak "None" into the wire."""
        creds = _make_creds()
        creds["claudeAiOauth"]["scopes"] = [
            "user:inference", None, 42, "", "user:profile",
        ]
        body = self._capture_body(creds)
        self.assertEqual(body.get("scope"), "user:inference user:profile")

    def test_scope_non_list_treated_as_empty(self):
        """Defensive: `scopes` key present but not a list → empty."""
        creds = _make_creds()
        creds["claudeAiOauth"]["scopes"] = "user:inference"  # str, not list
        body = self._capture_body(creds)
        self.assertEqual(body.get("scope"), "")

    def test_scope_preserved_across_refresh(self):
        """Post-refresh, credentials.json still carries the original
        scopes array (server doesn't rotate scopes)."""
        with tempfile.TemporaryDirectory() as d:
            path = _write_creds(d, _make_creds())

            def http_post(url, body):
                return {"access_token": "x", "expires_in": 3600}

            refresh_if_expiring(
                path, now=_NOW, window_seconds=3 * 3600,
                http_post=http_post,
            )
            with open(path) as f:
                oauth = json.load(f)["claudeAiOauth"]
            self.assertEqual(
                oauth["scopes"],
                _make_creds()["claudeAiOauth"]["scopes"],
            )


# ── refresh_if_expiring — failure paths (zero writes) ──────────────


class TestFailureLeavesFileUntouched(unittest.TestCase):
    def _fail_and_check_untouched(self, http_post):
        with tempfile.TemporaryDirectory() as d:
            path = _write_creds(d, _make_creds())
            original = _make_creds()

            result = refresh_if_expiring(
                path, now=_NOW, window_seconds=3 * 3600,
                http_post=http_post,
            )
            self.assertEqual(result["status"], "failed")
            self.assertIsNotNone(result["reason"])
            with open(path) as f:
                self.assertEqual(json.load(f), original)

    def test_http_exception_leaves_file_untouched(self):
        def http_post(url, body):
            raise OAuthRefreshError("http_401: invalid grant")

        self._fail_and_check_untouched(http_post)

    def test_response_missing_access_token_leaves_file_untouched(self):
        def http_post(url, body):
            return {"expires_in": 3600}  # missing access_token

        self._fail_and_check_untouched(http_post)

    def test_response_missing_expires_in_leaves_file_untouched(self):
        def http_post(url, body):
            return {"access_token": "x"}  # missing expires_in

        self._fail_and_check_untouched(http_post)


# ── Missing / malformed credentials ─────────────────────────────────


class TestMissingCredentials(unittest.TestCase):
    def test_missing_file_returns_structured_error(self):
        result = refresh_if_expiring(
            "/nonexistent/path/creds.json", now=_NOW,
        )
        self.assertEqual(result["status"], "missing_credentials")

    def test_malformed_json_returns_structured_error(self):
        with tempfile.TemporaryDirectory() as d:
            path = os.path.join(d, ".credentials.json")
            with open(path, "w") as f:
                f.write("{{{not json")
            result = refresh_if_expiring(path, now=_NOW)
            self.assertEqual(result["status"], "missing_credentials")

    def test_missing_claudeAiOauth_key(self):
        with tempfile.TemporaryDirectory() as d:
            path = os.path.join(d, ".credentials.json")
            with open(path, "w") as f:
                json.dump({"other_key": {}}, f)
            result = refresh_if_expiring(path, now=_NOW)
            self.assertEqual(result["status"], "missing_credentials")


class TestNoRefreshToken(unittest.TestCase):
    def test_no_refresh_token_returns_structured_error_no_post(self):
        creds = _make_creds()
        del creds["claudeAiOauth"]["refreshToken"]

        with tempfile.TemporaryDirectory() as d:
            path = _write_creds(d, creds)
            called = []

            def http_post(url, body):
                called.append(url)
                return {}

            result = refresh_if_expiring(
                path, now=_NOW, window_seconds=3 * 3600,
                http_post=http_post,
            )
            self.assertEqual(result["status"], "no_refresh_token")
            self.assertEqual(called, [])


# ── File mode enforcement ───────────────────────────────────────────


class TestFileMode(unittest.TestCase):
    def test_refresh_enforces_0o600(self):
        with tempfile.TemporaryDirectory() as d:
            path = _write_creds(d, _make_creds())
            # Loosen it — helper must tighten back.
            os.chmod(path, 0o644)

            def http_post(url, body):
                return {"access_token": "x", "expires_in": 3600}

            refresh_if_expiring(
                path, now=_NOW, window_seconds=3 * 3600,
                http_post=http_post,
            )
            mode = stat.S_IMODE(os.stat(path).st_mode)
            self.assertEqual(mode, 0o600)


# ── refresh_now vs refresh_if_expiring window semantics ────────────


class TestRefreshNowForces(unittest.TestCase):
    def test_refresh_now_fires_even_when_far_from_expiry(self):
        with tempfile.TemporaryDirectory() as d:
            # Token that expires 6 months from _NOW — comfortably outside
            # any reasonable window.
            creds = _make_creds()
            creds["claudeAiOauth"]["expiresAt"] = int(
                (_NOW + 3600 * 24 * 180) * 1000
            )
            path = _write_creds(d, creds)
            called = []

            def http_post(url, body):
                called.append(url)
                return {"access_token": "y", "expires_in": 3600}

            result = refresh_now(path, now=_NOW, http_post=http_post)
            self.assertEqual(result["status"], "refreshed")
            self.assertEqual(len(called), 1)


# ── Env-var overrides ───────────────────────────────────────────────


class TestEnvOverrides(unittest.TestCase):
    def test_env_overrides_url_and_client_id(self):
        with tempfile.TemporaryDirectory() as d:
            path = _write_creds(d, _make_creds())
            seen = {}

            def http_post(url, body):
                seen["url"] = url
                seen["body"] = body
                return {"access_token": "x", "expires_in": 3600}

            os.environ["CLAUDE_CODE_CUSTOM_OAUTH_URL"] = \
                "https://staging.example.com/oauth/token"
            os.environ["CLAUDE_CODE_OAUTH_CLIENT_ID"] = "staging-client-id"
            try:
                refresh_if_expiring(
                    path, now=_NOW, window_seconds=3 * 3600,
                    http_post=http_post,
                )
            finally:
                del os.environ["CLAUDE_CODE_CUSTOM_OAUTH_URL"]
                del os.environ["CLAUDE_CODE_OAUTH_CLIENT_ID"]

            self.assertEqual(
                seen["url"], "https://staging.example.com/oauth/token"
            )
            self.assertEqual(seen["body"]["client_id"], "staging-client-id")

    def test_env_override_supports_platform_claude_com_for_newer_cli(self):
        """v3 version-drift coverage: the default URL matches the PROD
        CLI (console.anthropic.com). When prod SDK is upgraded to the
        newer CLI (whose token host is platform.claude.com), operators
        set CLAUDE_CODE_CUSTOM_OAUTH_URL to migrate without a code
        change. This test locks that env-var pathway in."""
        with tempfile.TemporaryDirectory() as d:
            path = _write_creds(d, _make_creds())
            seen = {}

            def http_post(url, body):
                seen["url"] = url
                return {"access_token": "x", "expires_in": 3600}

            os.environ["CLAUDE_CODE_CUSTOM_OAUTH_URL"] = \
                "https://platform.claude.com/v1/oauth/token"
            try:
                refresh_if_expiring(
                    path, now=_NOW, window_seconds=3 * 3600,
                    http_post=http_post,
                )
            finally:
                del os.environ["CLAUDE_CODE_CUSTOM_OAUTH_URL"]

            self.assertEqual(
                seen["url"], "https://platform.claude.com/v1/oauth/token"
            )

    def test_env_overrides_credentials_path(self):
        with tempfile.TemporaryDirectory() as d:
            path = _write_creds(d, _make_creds())

            def http_post(url, body):
                return {"access_token": "x", "expires_in": 3600}

            os.environ["AK_CLAUDE_CREDENTIALS_PATH"] = path
            try:
                result = refresh_if_expiring(
                    now=_NOW, window_seconds=3 * 3600,
                    http_post=http_post,
                )
            finally:
                del os.environ["AK_CLAUDE_CREDENTIALS_PATH"]
            self.assertEqual(result["status"], "refreshed")
            self.assertEqual(result["path"], path)


# ── Pure-helper unit tests ──────────────────────────────────────────


class TestExtractNewTokens(unittest.TestCase):
    def test_requires_access_token(self):
        with self.assertRaises(OAuthRefreshError):
            _extract_new_tokens({"expires_in": 3600})

    def test_requires_expires_in(self):
        with self.assertRaises(OAuthRefreshError):
            _extract_new_tokens({"access_token": "x"})

    def test_non_numeric_expires_in(self):
        with self.assertRaises(OAuthRefreshError):
            _extract_new_tokens(
                {"access_token": "x", "expires_in": "soon"}
            )

    def test_missing_refresh_returns_None(self):
        got = _extract_new_tokens(
            {"access_token": "x", "expires_in": 3600}
        )
        self.assertIsNone(got["refreshToken"])

    def test_empty_string_refresh_returns_None(self):
        got = _extract_new_tokens(
            {"access_token": "x", "expires_in": 3600,
             "refresh_token": ""}
        )
        self.assertIsNone(got["refreshToken"])


class TestApplyNewTokens(unittest.TestCase):
    def test_shallow_copy_leaves_original_untouched(self):
        oauth = _make_creds()["claudeAiOauth"]
        original_snapshot = json.loads(json.dumps(oauth))

        new = {"accessToken": "new", "refreshToken": "new-r",
               "expiresInSeconds": 3600}
        _apply_new_tokens(oauth, new, now=_NOW)
        self.assertEqual(oauth, original_snapshot)

    def test_computes_expiresAt_as_ms_since_epoch(self):
        oauth = _make_creds()["claudeAiOauth"]
        new = {"accessToken": "new", "refreshToken": None,
               "expiresInSeconds": 60}
        updated = _apply_new_tokens(oauth, new, now=_NOW)
        self.assertEqual(updated["expiresAt"], int((_NOW + 60) * 1000))


# ── v4 Minor A: parent dir 0o700 ───────────────────────────────────


class TestParentDirMode(unittest.TestCase):
    """v4 Minor A: post-refresh, the parent directory of the
    credentials file must be 0o700. A loose parent lets any process
    with search permission open the 0o600 file regardless of its
    own mode."""

    def test_parent_dir_chmodded_to_0o700_after_refresh(self):
        with tempfile.TemporaryDirectory() as parent_of_parent:
            # Nest one level so the parent-of-creds is one we own and
            # can safely tighten without interfering with the outer
            # temp dir cleanup.
            parent = os.path.join(parent_of_parent, ".claude")
            os.makedirs(parent, mode=0o755)  # deliberately loose
            path = _write_creds(parent, _make_creds())

            def http_post(url, body):
                return {"access_token": "x", "expires_in": 3600}

            result = refresh_if_expiring(
                path, now=_NOW, window_seconds=3 * 3600,
                http_post=http_post,
            )
            self.assertEqual(result["status"], "refreshed")
            mode = stat.S_IMODE(os.stat(parent).st_mode)
            self.assertEqual(mode, 0o700)

    def test_parent_dir_chmod_failure_is_non_fatal(self):
        """If the parent dir chmod raises (e.g., not-owner), the
        refresh still succeeds — file mode is already 0o600, parent
        tightening is belt-and-braces."""
        with tempfile.TemporaryDirectory() as d:
            path = _write_creds(d, _make_creds())

            original_chmod = os.chmod
            call_log = []

            def flaky_chmod(target, mode):
                # Fail specifically on the parent-dir chmod (dir path);
                # let the file chmods through.
                if os.path.isdir(target):
                    call_log.append(("dir_chmod_denied", target, mode))
                    raise PermissionError("simulated")
                return original_chmod(target, mode)

            def http_post(url, body):
                return {"access_token": "x", "expires_in": 3600}

            import unittest.mock as mock
            with mock.patch.object(os, "chmod", side_effect=flaky_chmod):
                result = refresh_if_expiring(
                    path, now=_NOW, window_seconds=3 * 3600,
                    http_post=http_post,
                )
            self.assertEqual(result["status"], "refreshed")
            self.assertTrue(
                any("dir_chmod_denied" == c[0] for c in call_log),
                "parent-dir chmod should have been attempted",
            )


# ── v4 Minor B: HTTP error body sanitization ───────────────────────


class TestSanitizeErrorBody(unittest.TestCase):
    """v4 Minor B: never embed raw response bytes in the 'failed'
    reason — a hostile OAuth server (or misconfigured proxy)
    could echo the request body straight back and leak
    refresh_token."""

    def test_empty_body(self):
        self.assertEqual(_sanitize_error_body(""), "no-body")
        self.assertEqual(_sanitize_error_body(None), "no-body")

    def test_oauth_standard_error_extracted(self):
        body = '{"error":"invalid_grant","error_description":"expired"}'
        self.assertEqual(_sanitize_error_body(body), "invalid_grant")

    def test_oauth_error_with_padding_whitespace(self):
        body = '{ "error"  :  "invalid_client" }'
        self.assertEqual(_sanitize_error_body(body), "invalid_client")

    def test_token_shape_string_redacted(self):
        """Any 40+ char blob of alnum/dash/underscore is scrubbed —
        catches refresh_token, access_token, JWT payloads, opaque
        bearer tokens."""
        token = "A" * 108  # matches Q2 refresh_token length
        body = f'{{"detail":"unauthorised: {token}"}}'
        cleaned = _sanitize_error_body(body)
        self.assertNotIn(token, cleaned)
        self.assertIn("<REDACTED>", cleaned)

    def test_multiple_secrets_all_redacted(self):
        body = (
            f'access={"A"*80}, refresh={"B"*108}, jwt={"C"*50}'
        )
        cleaned = _sanitize_error_body(body)
        self.assertNotIn("A" * 80, cleaned)
        self.assertNotIn("B" * 108, cleaned)
        self.assertNotIn("C" * 50, cleaned)

    def test_hard_cap_enforced(self):
        """Even after redaction, cap the fragment length so a
        thousand-char error page doesn't flood the log. Use a body
        of short words separated by whitespace so the secret-shape
        regex (40+ char run) doesn't collapse everything to
        <REDACTED> — that would defeat the cap-check by making the
        fragment short."""
        body = " ".join(["word"] * 500)  # ~2500 chars, no 40-char runs
        cleaned = _sanitize_error_body(body)
        # cleaned = "unrecognized:" + first 200 chars of body + "…"
        self.assertLess(len(cleaned), 250)
        self.assertTrue(cleaned.endswith("…"))

    def test_short_body_without_error_field_kept_readable(self):
        body = "Server temporarily unavailable"
        cleaned = _sanitize_error_body(body)
        self.assertIn("Server", cleaned)
        self.assertTrue(cleaned.startswith("unrecognized:"))

    def test_normal_json_field_names_not_redacted(self):
        """Short field names / values must not trip the secret-shape
        regex — the threshold is 40+ chars for a reason."""
        body = '{"error":"invalid_grant","state":"abc123","code":"xyz"}'
        # invalid_grant path — extracted, nothing else matters.
        self.assertEqual(_sanitize_error_body(body), "invalid_grant")


class TestFailedReasonNeverLeaksToken(unittest.TestCase):
    """v4 end-to-end: even if the OAuth server echoes the refresh_token
    in an error response, the token must NOT appear in the result
    dict's `reason` field."""

    def test_refresh_token_scrubbed_from_failed_reason(self):
        """Use a realistic 108-char refresh token (matches the Q2 prod
        cache observation). The sanitizer threshold is 40 chars, which
        covers every real OAuth token we've seen — short debug fixtures
        below that threshold would not be scrubbed and are not a
        realistic threat model."""
        creds = _make_creds()
        realistic_token = "R" * 108  # matches production shape
        creds["claudeAiOauth"]["refreshToken"] = realistic_token

        with tempfile.TemporaryDirectory() as d:
            path = _write_creds(d, creds)

            # Simulate a hostile / misconfigured OAuth server that
            # echoes the request body in a 400.
            def http_post(url, body):
                echoed = f"Bad request: refresh_token={body['refresh_token']}"
                raise OAuthRefreshError(f"http_400: {_sanitize_error_body(echoed)}")

            result = refresh_if_expiring(
                path, now=_NOW, window_seconds=3 * 3600,
                http_post=http_post,
            )
            self.assertEqual(result["status"], "failed")
            self.assertNotIn(
                realistic_token, result["reason"],
                f"refresh_token leaked into reason: {result['reason']}",
            )
            self.assertIn("<REDACTED>", result["reason"])


if __name__ == "__main__":
    print("ak-oauth-refresh regression tests")
    print("=" * 60)
    unittest.main(verbosity=2, exit=False)
    print("=" * 60)
