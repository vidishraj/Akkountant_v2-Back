"""ak-x5l9 (P0, 2026-11-24): HDFC Smart Statement gate vs requests/urllib3
parser differential.

Bug: the gate checked urlparse(url).hostname, but urllib3 ends the
authority at a backslash. "https://evil.com\\@smartstatements.hdfcbank.com/x"
passed the gate (urlparse host = HDFC) yet requests CONNECTED TO evil.com,
including for the form-action POST carrying the Customer ID password.

Fix (utils/hdfc_smart_statement.py): reject backslash / control chars /
userinfo / non-443 ports / non-https, require urlparse host == urllib3 host,
and send every request to a URL rebuilt from the checked components.
Core property tested here: for ANY string the gate accepts, the host that
requests would actually connect to is allowlisted.
No network.
"""
from __future__ import annotations

import itertools
import os
import shutil
import sys
import tempfile
import unittest
from unittest import mock

HERE = os.path.dirname(os.path.abspath(__file__))
sys.path.insert(0, HERE)

import requests  # noqa: E402
from urllib3.util import parse_url  # noqa: E402

from utils.hdfc_smart_statement import (  # noqa: E402
    HDFC_SMART_STATEMENT_HOSTS, canonical_hdfc_url, follow_hdfc_redirects,
    is_hdfc_smart_statement_url, new_hdfc_session,
)

H = "smartstatements.hdfcbank.com"

ATTACKS = [
    f"https://evil.com\\@{H}/x",                 # the reported bypass
    f"http://169.254.169.254\\@{H}/latest",       # metadata via the same trick
    f"https://evil.com%5C@{H}/x",                 # encoded backslash (userinfo)
    f"https://evil.com%5c@{H}/x",
    f"https://evil.com@{H}/x",                    # userinfo forms
    f"https://user:pw@{H}/x",
    f"https://@{H}/x",
    f"https://:@{H}/x",
    f"https://{H}@evil.com/x",
    f"https://{H}:444/x",                         # ports
    f"https://{H}:80/x",
    f"https://{H}:0/x",
    f"http://{H}/x",                              # https only
    f"HTTP://{H}/x",
    f"https://{H}\\.evil.com/x",
    f"https://{H}/x\\@evil.com",                  # backslash anywhere
    f"https://{H}\t.evil.com/x",                  # control / whitespace
    f"https://{H}\r\n.evil.com/x",
    f"https://{H} /x",
    f"https://{H}\x00/x",
    f" https://{H}/x",
    f"https://{H}./x",                            # trailing-dot host
    f"https://{H}.evil.com/x",
    f"//{H}/x",                                   # scheme-relative
    f"https:{H}/x",
    f"https:///{H}/x",
    "https://[::1]/x",
]


def _connect_host(url):
    """The host requests/urllib3 would actually connect to."""
    prepared = requests.Request("GET", url).prepare().url
    return (parse_url(prepared).host or "").lower()


class TestAttacksRejected(unittest.TestCase):

    def test_attack_table(self):
        for url in ATTACKS:
            self.assertIsNone(canonical_hdfc_url(url), repr(url))
            self.assertFalse(is_hdfc_smart_statement_url(url), repr(url))

    def test_reported_case_really_was_a_differential(self):
        # Documents the bug: the old check would have said HDFC, requests says evil.com.
        from urllib.parse import urlparse
        url = f"https://evil.com\\@{H}/x"
        self.assertEqual(urlparse(url).hostname, H)
        self.assertEqual(_connect_host(url), "evil.com")
        self.assertIsNone(canonical_hdfc_url(url))


class TestAcceptedAndCanonical(unittest.TestCase):

    def test_canonical_rebuild(self):
        self.assertEqual(canonical_hdfc_url(f"https://{H}/page?jobkey=JK1"), f"https://{H}/page?jobkey=JK1")
        self.assertEqual(
            canonical_hdfc_url("HTTPS://SmartStatements.HDFCBank.COM:443/p;x/y?a=1&b=2#frag"),
            f"https://{H}/p;x/y?a=1&b=2",
        )
        self.assertEqual(canonical_hdfc_url(f"https://{H}"), f"https://{H}/")
        self.assertEqual(canonical_hdfc_url("https://smartstatements.hdfc.bank.in/d"),
                         "https://smartstatements.hdfc.bank.in/d")


class TestDifferentialProperty(unittest.TestCase):
    """Whatever the gate accepts, requests connects to an allowlisted
    host, both for the raw string and for the canonical URL."""

    PREFIXES = ["", "evil.com", "169.254.169.254", "user", "user:pw", H, f"{H}:443", f"{H}:444"]
    SEPS = ["", "@", "\\", "\\@", "%5C@", "%40", "/", "?", "#", ";", ":", "\t", "%00", ".", "@@"]
    TAILS = ["", H, f"{H}/x", f"{H}:443/x", "evil.com/x", f"{H}.evil.com/x", f"{H}\\@evil.com/"]
    SCHEMES = ["https://", "http://", "HTTPS://", "https:/", "//"]

    def test_no_accepted_url_connects_off_allowlist(self):
        accepted = 0
        for scheme, pre, sep, tail in itertools.product(self.SCHEMES, self.PREFIXES, self.SEPS, self.TAILS):
            url = f"{scheme}{pre}{sep}{tail}"
            canon = canonical_hdfc_url(url)
            if canon is None:
                continue
            accepted += 1
            self.assertIn(_connect_host(canon), HDFC_SMART_STATEMENT_HOSTS, (url, canon))
            self.assertIn(_connect_host(url), HDFC_SMART_STATEMENT_HOSTS, url)
        self.assertGreater(accepted, 0)  # the property was actually exercised


class _Resp:
    def __init__(self, status=200, location=None, url=""):
        self.status_code = status
        self.headers = {"Location": location} if location else {}
        self.url = url


class _Session:
    def __init__(self, responses=()):
        self.responses = list(responses)
        self.calls = []
        self.headers = {}

    def get(self, url, **kw):
        self.calls.append(("GET", url, kw))
        return self.responses.pop(0)

    def post(self, url, **kw):
        self.calls.append(("POST", url, kw))
        return self.responses.pop(0)


class TestRedirectsAndSession(unittest.TestCase):

    def test_redirect_bypasses_never_leave_allowlist(self):
        # Relative Locations (e.g. "//<H>/x") may legitimately resolve to
        # HDFC; the property is that NO hop ever connects elsewhere.
        for loc in ATTACKS + ["\\\\evil.com/x", "/\\evil.com/x", "//evil.com/x", "https://evil.com/"]:
            s = _Session([_Resp(200)])
            follow_hdfc_redirects(s, _Resp(302, loc, url=f"https://{H}/a"))
            for _m, url, kw in s.calls:
                self.assertIn(_connect_host(url), HDFC_SMART_STATEMENT_HOSTS, (loc, url))
                self.assertIs(kw["allow_redirects"], False)

    def test_absolute_attack_redirects_refused_without_request(self):
        # "https:///<H>/x" resolves to a path ON the HDFC host (safe; covered above).
        for loc in [a for a in ATTACKS if "://" in a and not a.startswith("https:///")]:
            s = _Session()
            self.assertIsNone(follow_hdfc_redirects(s, _Resp(302, loc, url=f"https://{H}/a")), repr(loc))
            self.assertEqual(s.calls, [])

    def test_redirect_hop_uses_canonical_url(self):
        s = _Session([_Resp(200)])
        follow_hdfc_redirects(s, _Resp(302, f"HTTPS://{H.upper()}:443/n?q=1#f", url=f"https://{H}/a"))
        self.assertEqual(s.calls[0][1], f"https://{H}/n?q=1")

    def test_session_ignores_environment(self):
        s = new_hdfc_session("UA")
        self.assertFalse(s.trust_env)
        self.assertEqual(s.headers["User-Agent"], "UA")
        with mock.patch.dict(os.environ, {"HTTPS_PROXY": "http://evil.example:8080"}):
            self.assertEqual(s.merge_environment_settings(f"https://{H}/", {}, None, None, None)["proxies"], {})


class TestServicesSendCanonicalUrls(unittest.TestCase):

    def setUp(self):
        with open(os.path.join(HERE, "services", "mailProcessorService.py")) as fh:
            src = fh.read()
        i = src.index("def _download_from_smart_statement_link(")
        self.fn = src[i:src.index("\n    def ", i + 10)]

    def test_mail_link_and_action_canonicalised_before_requests(self):
        get_i = self.fn.index("session.get(link")
        self.assertLess(self.fn.index("link = canonical_hdfc_url(link)"), get_i)
        post_i = self.fn.index("session.post(action, data=login_data")
        canon_i = self.fn.rindex("action = canonical_hdfc_url(action)", 0, post_i)
        # No reassignment of action between canonicalisation and the POST.
        self.assertNotRegex(self.fn[canon_i + 10:post_i], r"\baction\s*=")
        self.assertIn("session = new_hdfc_session(", self.fn)
        self.assertNotIn("requests.Session(", self.fn)

    def test_mail_pdf_params_encoded(self):
        self.assertIn('params={"jobkey": job_key, "reqid": req_id, "format": "pdf"}', self.fn)
        self.assertNotIn("&reqid={req_id}", self.fn)


try:
    import services.StatementDownloadService as sds_mod
    _SDS_OK, _SDS_SKIP = True, ""
except Exception as _exc:  # pragma: no cover
    _SDS_OK, _SDS_SKIP = False, f"StatementDownloadService import failed: {_exc}"


@unittest.skipUnless(_SDS_OK, _SDS_SKIP)
class TestSds(unittest.TestCase):

    def setUp(self):
        self.svc = sds_mod.StatementDownloadService()
        self.tmp = tempfile.mkdtemp(prefix="akx5l9_")
        self.out = os.path.join(self.tmp, "tmp")
        os.makedirs(self.out)
        self._p = mock.patch.object(sds_mod, "TEMP_DIR", self.out)
        self._p.start()

    def tearDown(self):
        self._p.stop()
        shutil.rmtree(self.tmp, ignore_errors=True)

    def test_parse_href_rejects_attacks_without_request(self):
        made = []
        with mock.patch.object(sds_mod, "new_hdfc_session", side_effect=lambda *a: made.append(1)):
            for url in ATTACKS:
                self.assertIsNone(self.svc._parse_href(url), repr(url))
        self.assertEqual(made, [])

    def test_parse_href_requests_canonical_url(self):
        sess = _Session([_Resp(404)])
        with mock.patch.object(sds_mod, "new_hdfc_session", return_value=sess):
            self.svc._parse_href(f"HTTPS://{H.upper()}:443/page?jobkey=JK1#frag")
        self.assertEqual(sess.calls[0][1], f"https://{H}/page?jobkey=JK1")

    def test_saved_filename_cannot_escape_tmp(self):
        class R:
            status_code = 200
            content = b"%PDF-1.4 synthetic"
            headers = {}
            url = ""
        with mock.patch.object(sds_mod, "new_hdfc_session", side_effect=lambda *a: _Session([R()])):
            files = self.svc._download_hdfc_statements([["../../escape", "../jk"]])
        self.assertEqual(len(files), 1)
        self.assertNotIn("/", files[0])
        self.assertEqual(os.listdir(self.tmp), ["tmp"])          # nothing written outside
        self.assertEqual(os.listdir(self.out), files)


if __name__ == "__main__":  # pragma: no cover
    unittest.main(verbosity=2)
