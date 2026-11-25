"""ak-tspe (2026-11-24): follow-ups to ak-ptxq.

1. StatementDownloadService (operator path, boi_backfill_2026.py) took the
   Smart Statement href from email HTML and did requests.get(link) on it
   with no host check (SSRF). It now uses the same gate as the mail
   pipeline, hoisted into utils/hdfc_smart_statement.py.
2. The dead GmailServiceUtils.downloadFilesInRange wrapper is removed.
3. m1: Smart Statement requests no longer auto-follow redirects; the
   credentialed POST (Customer ID password) can never be replayed to
   another host. Allowlisted redirects are followed as body-less GETs.
4. m2: hostname parsing is inside the gate's fail-closed try.
No network: sessions / responses are fakes.
"""
from __future__ import annotations

import ast
import os
import re
import sys
import unittest
from unittest import mock

HERE = os.path.dirname(os.path.abspath(__file__))
sys.path.insert(0, HERE)

from utils import hdfc_smart_statement as hss  # noqa: E402
from utils.hdfc_smart_statement import (  # noqa: E402
    follow_hdfc_redirects, is_hdfc_smart_statement_url,
)

GOOD = "https://smartstatements.hdfcbank.com/page?jobkey=JK1"

# Same bypass table as the ak-ptxq mail-processor tests.
BYPASSES = [
    "https://smartstatements.hdfcbank.com@evil.example/collect",   # userinfo-at
    "https://evil.example/?q=smartstatements.hdfcbank.com",         # substring (query)
    "https://evil.example/smartstatements.hdfcbank.com/x",          # substring (path)
    "https://evil.hdfcbank.com/collect",                            # look-alike subdomain
    "https://smartstatements.hdfcbank.com.evil.example/",           # look-alike suffix
    "https://hdfcbank.com/",                                        # parent domain
    "file:///etc/passwd",                                           # non-http
    "gopher://smartstatements.hdfcbank.com/0",
    "javascript:alert(1)",
    "ftp://smartstatements.hdfcbank.com/",
    "http://[::1",                                                  # malformed
    "", None, 42,
]


class _Resp:
    def __init__(self, status=200, location=None, url="", text="", content=b""):
        self.status_code = status
        self.headers = {"Location": location} if location else {}
        self.url = url
        self.text = text
        self.content = content


class _Session:
    """Records calls; returns queued responses."""

    def __init__(self, responses):
        self.responses = list(responses)
        self.calls = []
        self.headers = {}

    def _next(self, method, url, kwargs):
        self.calls.append((method, url, kwargs))
        return self.responses.pop(0)

    def get(self, url, **kw):
        return self._next("GET", url, kw)

    def post(self, url, **kw):
        return self._next("POST", url, kw)


# ── gate ────────────────────────────────────────────────────────────

class TestGate(unittest.TestCase):

    def test_allowed(self):
        self.assertTrue(is_hdfc_smart_statement_url(GOOD))
        self.assertTrue(is_hdfc_smart_statement_url("https://SMARTSTATEMENTS.HDFC.BANK.IN/x"))

    def test_bypass_table_rejected(self):
        for url in BYPASSES:
            self.assertFalse(is_hdfc_smart_statement_url(url), url)

    def test_hostname_error_fails_closed(self):
        # m2: an exception from .hostname must return False, not raise.
        class Parsed:
            scheme = "https"

            @property
            def hostname(self):
                raise ValueError("bad netloc")
        with mock.patch.object(hss, "urlsplit", return_value=Parsed()):
            self.assertFalse(is_hdfc_smart_statement_url(GOOD))

    def test_mail_service_delegates_to_shared_gate(self):
        with open(os.path.join(HERE, "services", "mailProcessorService.py")) as fh:
            src = fh.read()
        self.assertIn("_HDFC_SMART_STATEMENT_HOSTS = HDFC_SMART_STATEMENT_HOSTS", src)
        self.assertIn("return is_hdfc_smart_statement_url(url)", src)
        self.assertNotRegex(src, r"_HDFC_SMART_STATEMENT_HOSTS\s*=\s*frozenset")


# ── redirect following ──────────────────────────────────────────────

class TestFollowRedirects(unittest.TestCase):

    def test_non_redirect_passthrough(self):
        s = _Session([])
        r = _Resp(200)
        self.assertIs(follow_hdfc_redirects(s, r), r)
        self.assertEqual(s.calls, [])

    def test_allowlisted_redirect_followed_as_bodyless_get(self):
        final = _Resp(200)
        s = _Session([final])
        out = follow_hdfc_redirects(
            s, _Resp(307, "https://smartstatements.hdfc.bank.in/next", url=GOOD), timeout=7,
        )
        self.assertIs(out, final)
        self.assertEqual(s.calls, [("GET", "https://smartstatements.hdfc.bank.in/next",
                                    {"timeout": 7, "allow_redirects": False})])

    def test_relative_location_resolved_against_response_url(self):
        s = _Session([_Resp(200)])
        follow_hdfc_redirects(s, _Resp(302, "/HDFCRestFulService/x", url=GOOD))
        self.assertEqual(s.calls[0][1], "https://smartstatements.hdfcbank.com/HDFCRestFulService/x")

    def test_off_allowlist_redirect_refused_without_request(self):
        for code in (301, 302, 303, 307, 308):
            for loc in BYPASSES[:6]:
                s = _Session([])
                self.assertIsNone(follow_hdfc_redirects(s, _Resp(code, loc, url=GOOD)), (code, loc))
                self.assertEqual(s.calls, [])

    def test_chain_off_allowlist_midway_refused(self):
        s = _Session([_Resp(302, "https://evil.example/x", url=GOOD)])
        self.assertIsNone(follow_hdfc_redirects(s, _Resp(302, GOOD, url=GOOD)))
        self.assertEqual(len(s.calls), 1)

    def test_redirect_loop_capped(self):
        loop = [_Resp(302, GOOD, url=GOOD) for _ in range(10)]
        s = _Session(loop)
        self.assertIsNone(follow_hdfc_redirects(s, _Resp(302, GOOD, url=GOOD)))
        self.assertEqual(len(s.calls), hss.MAX_REDIRECTS)

    def test_redirect_without_location_returned_as_is(self):
        r = _Resp(302)
        self.assertIs(follow_hdfc_redirects(_Session([]), r), r)


# ── mail pipeline: no auto-redirects on Smart Statement calls ────────

class TestMailServiceRedirectHardening(unittest.TestCase):

    def setUp(self):
        with open(os.path.join(HERE, "services", "mailProcessorService.py")) as fh:
            src = fh.read()
        i = src.index("def _download_from_smart_statement_link(")
        self.fn = src[i:src.index("\n    def ", i + 10)]

    def test_every_portal_request_disables_auto_redirects(self):
        # Real calls only (docstring mentions are "session.get()").
        calls = re.findall(r"session\.(?:get|post)\((?!\))(?:[^()]|\([^()]*\))*\)", self.fn, re.S)
        self.assertEqual(len(calls), 3, calls)
        for c in calls:
            self.assertIn("allow_redirects=False", c)

    def test_credentialed_post_followed_by_safe_follow(self):
        i = self.fn.index("session.post(action, data=login_data")
        post_line = self.fn[i:self.fn.index("\n", i)]
        self.assertIn("allow_redirects=False", post_line)
        nxt = self.fn[i:i + 400]
        self.assertIn("follow_hdfc_redirects(session, login_response", nxt)
        self.assertIn("if login_response is None:", nxt)

    def test_refusals_return_none(self):
        self.assertEqual(self.fn.count("follow_hdfc_redirects("), 3)
        self.assertGreaterEqual(self.fn.count("is None:\n"), 3)


# ── StatementDownloadService (operator path) ─────────────────────────

try:
    import services.StatementDownloadService as sds_mod
    _SDS_OK, _SDS_SKIP = True, ""
except Exception as _exc:  # pragma: no cover
    _SDS_OK, _SDS_SKIP = False, f"StatementDownloadService import failed: {_exc}"

_SEQ_PAGE = '<input type="hidden" name="seqence" id="seqence" value="REQ9">'


@unittest.skipUnless(_SDS_OK, _SDS_SKIP)
class TestSdsParseHref(unittest.TestCase):

    def setUp(self):
        self.svc = sds_mod.StatementDownloadService()
        self.sessions = []

    def _patch(self, responses):
        def factory():
            s = _Session(list(responses))
            self.sessions.append(s)
            return s
        return mock.patch.object(sds_mod.requests, "Session", side_effect=factory)

    def test_bypass_table_refused_before_any_request(self):
        with self._patch([]), \
                mock.patch.object(sds_mod.requests, "get") as g, \
                mock.patch.object(sds_mod.requests, "post") as p:
            for url in BYPASSES:
                self.assertIsNone(self.svc._parse_href(url), url)
        self.assertEqual(self.sessions, [])
        g.assert_not_called()
        p.assert_not_called()

    def test_allowlisted_link_parsed_without_auto_redirect(self):
        # Parser stubbed: other test modules may replace bs4 in sys.modules.
        class _Soup:
            def __init__(self, text, _parser):
                self.text = text

            def find(self, *_a, **_k):
                return type("El", (), {"attrs": {"value": "REQ9"},
                                       "__getitem__": lambda s, k: s.attrs[k]})()
        with self._patch([_Resp(200, text=_SEQ_PAGE)]), \
                mock.patch.object(sds_mod, "BeautifulSoup", _Soup):
            self.assertEqual(self.svc._parse_href(GOOD), ["REQ9", "JK1"])
        (method, url, kw), = self.sessions[0].calls
        self.assertEqual((method, url), ("GET", GOOD))
        self.assertIs(kw.get("allow_redirects"), False)
        self.assertIn("timeout", kw)

    def test_redirect_off_allowlist_refused(self):
        with self._patch([_Resp(302, "https://evil.example/x", url=GOOD)]):
            self.assertIsNone(self.svc._parse_href(GOOD))
        self.assertEqual(len(self.sessions[0].calls), 1)

    def test_missing_jobkey_refused_without_request(self):
        with self._patch([]):
            self.assertIsNone(self.svc._parse_href("https://smartstatements.hdfcbank.com/page"))
        self.assertEqual(self.sessions, [])


@unittest.skipUnless(_SDS_OK, _SDS_SKIP)
class TestSdsDownload(unittest.TestCase):

    def setUp(self):
        self.svc = sds_mod.StatementDownloadService()
        self.tmp = __import__("tempfile").mkdtemp(prefix="aktspe_")
        self._tmp_patch = mock.patch.object(sds_mod, "TEMP_DIR", self.tmp)
        self._tmp_patch.start()

    def tearDown(self):
        self._tmp_patch.stop()
        __import__("shutil").rmtree(self.tmp, ignore_errors=True)

    def test_pdf_post_fixed_host_encoded_params_no_auto_redirect(self):
        sessions = []

        def factory():
            s = _Session([_Resp(200, content=b"%PDF-1.4 synthetic")])
            sessions.append(s)
            return s
        with mock.patch.object(sds_mod.requests, "Session", side_effect=factory):
            files = self.svc._download_hdfc_statements([["R&x=1", "JK1"], None])
        self.assertEqual(len(files), 1)
        (method, url, kw), = sessions[0].calls
        self.assertEqual((method, url), ("POST", "https://smartstatements.hdfcbank.com"
                                                 "/HDFCRestFulService/webresources/app/pdfformat"))
        self.assertEqual(kw["params"], {"jobkey": "JK1", "reqid": "R&x=1", "format": "pdf"})
        self.assertIs(kw.get("allow_redirects"), False)

    def test_pdf_redirect_off_allowlist_not_saved(self):
        with mock.patch.object(sds_mod.requests, "Session",
                               side_effect=lambda: _Session([_Resp(307, "https://evil.example/")])):
            self.assertEqual(self.svc._download_hdfc_statements([["R1", "JK1"]]), [])
        self.assertEqual(os.listdir(self.tmp), [])

    def test_no_raw_requests_calls_left(self):
        with open(os.path.join(HERE, "services", "StatementDownloadService.py")) as fh:
            src = fh.read()
        self.assertNotRegex(src, r"requests\.(get|post)\(")
        for c in re.findall(r"session\.(?:get|post)\((?:[^()]|\([^()]*\))*\)", src, re.S):
            self.assertIn("allow_redirects=False", c)


# ── dead wrapper removed ────────────────────────────────────────────

class TestDeadWrapperRemoved(unittest.TestCase):

    def test_download_files_in_range_gone(self):
        with open(os.path.join(HERE, "utils", "GmailServiceUtils.py")) as fh:
            tree = ast.parse(fh.read())
        cls = next(n for n in tree.body if isinstance(n, ast.ClassDef) and n.name == "GmailServiceUtils")
        names = {n.name for n in cls.body if isinstance(n, ast.FunctionDef)}
        self.assertNotIn("downloadFilesInRange", names)
        self.assertIn("checkStatus", names)  # rest of the class intact
        imported = {a.name for n in ast.walk(tree) if isinstance(n, ast.ImportFrom) for a in n.names}
        self.assertNotIn("StatementDownloadService", imported)

    def test_no_callers_left(self):
        hits = []
        for root, dirs, files in os.walk(HERE):
            dirs[:] = [d for d in dirs if d not in (".git", "venv", "node_modules", "__pycache__")]
            for f in files:
                if f.endswith(".py") and f != os.path.basename(__file__):
                    with open(os.path.join(root, f), errors="ignore") as fh:
                        if "downloadFilesInRange" in fh.read():
                            hits.append(f)
        self.assertEqual(hits, [])


if __name__ == "__main__":  # pragma: no cover
    unittest.main(verbosity=2)
