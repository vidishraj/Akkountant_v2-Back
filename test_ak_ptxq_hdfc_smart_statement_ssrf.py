"""ak-ptxq SECURITY C-8 (2026-11-23): regression tests for the
HDFC Smart Statement SSRF + password-leak hardening in
services/mailProcessorService.py.

Vulnerability class (pre-ak-ptxq):
  * _extract_smart_statement_link chose the download URL via two
    heuristics — (1) brand-colour <td style="...004b8d..."> + ANY
    inner <a> href, or (2) substring check
    `"smartstatements.hdfcbank.com" in href`. An attacker-controlled
    phishing email styled with HDFC branding, OR a URL like
    `https://evil.example/?q=smartstatements.hdfcbank.com`, passed
    both checks.
  * _download_from_smart_statement_link then fired `session.get(link)`
    (SSRF to attacker URL) and `session.post(action, data={...,
    'custid': password})` (password POST'd to whatever absolute URL
    the attacker's page exposed as the form action). Net effect: an
    attacker who can send email to Vidish's inbox could exfiltrate
    the HDFC Customer ID password.

Fix (ak-ptxq):
  * Module-level allowlist of HDFC Smart Statement hosts
    (`_HDFC_SMART_STATEMENT_HOSTS`): exact-match, case-insensitive.
  * Helper `_is_hdfc_smart_statement_url` on
    `MailProcessorService` — the SOLE gate between attacker-chosen
    URLs and `session.get` / `session.post` calls carrying the
    password.
  * Validation applied at 4 points:
    1. `_extract_smart_statement_link` strategy-1 (colour-match) —
       href must pass the host gate.
    2. `_extract_smart_statement_link` strategy-2 (both MIME-parts
       branch and direct-payload branch) — substring check replaced.
    3. `_download_from_smart_statement_link` link entry — belt-and-
       brace: fail closed if upstream gate missed it.
    4. `_download_from_smart_statement_link` form-action resolution
       + final-pre-POST gate — absolute URLs host-validated; final
       gate catches any refactor that breaks base_url derivation.
  * Log hygiene: only `list(login_data.keys())` is logged, never
    the values; password MUST NOT appear in any log line or
    exception message (§23 #4 PII guard class).

What these tests pin:
  (1)  `_is_hdfc_smart_statement_url` accepts allowed hosts
       (http + https, both allowlist entries, case-insensitive).
  (2)  Rejects attacker hosts (exact-match, not substring).
  (3)  Rejects substring-exploit URL `evil.com/?q=smartstatements.
       hdfcbank.com`.
  (4)  Rejects non-http(s) schemes (file://, javascript:, gopher://).
  (5)  Rejects None / empty / non-str.
  (6)  Rejects unparseable URLs without raising.
  (7)  Case-insensitive host match.

Source-grep drift guards (would red if the hardening is removed):
  (8)  `_HDFC_SMART_STATEMENT_HOSTS` constant present with both
       canonical hosts.
  (9)  `_is_hdfc_smart_statement_url` call present at
       strategy-1 colour-match.
  (10) `_is_hdfc_smart_statement_url` call present replacing the
       substring check.
  (11) Pre-POST gate present in `_download_from_smart_statement_
       link`.
  (12) Log-hygiene: no `{password}` / `login_data['custid']` /
       `{login_data}` / f"...custid={...}" shape in the file (the
       password MUST not reach the log line).

Behavior test (end-to-end stubbed Session):
  (13) Attacker link at `_download_from_smart_statement_link` entry
       → return None, zero session.get/.post calls.
"""
from __future__ import annotations

import ast
import os
import re
import sys
import unittest
from unittest import mock
from urllib.parse import urlparse


sys.path.insert(0, os.path.dirname(os.path.abspath(__file__)))


def _read_source(rel_path):
    here = os.path.dirname(os.path.abspath(__file__))
    with open(os.path.join(here, rel_path), "r") as f:
        return f.read()


def _ast_lift_hdfc_url_validator():
    """Return the HDFC Smart Statement URL gate + its allowlist.

    ak-tspe hoisted both into the stdlib-only leaf module
    utils/hdfc_smart_statement.py (shared with
    StatementDownloadService); MailProcessorService keeps
    `_HDFC_SMART_STATEMENT_HOSTS` / `_is_hdfc_smart_statement_url` as
    aliases (pinned by TestHardeningSourceGrep). The leaf module
    imports without the anyio / SDK chain, so no AST lift is needed.
    """
    from utils.hdfc_smart_statement import (
        HDFC_SMART_STATEMENT_HOSTS, is_hdfc_smart_statement_url,
    )
    return is_hdfc_smart_statement_url, HDFC_SMART_STATEMENT_HOSTS


# ─── Pure-unit: _is_hdfc_smart_statement_url ─────────────────


class TestIsHdfcSmartStatementUrl(unittest.TestCase):
    """AST-lift the classmethod + allowlist constant so we can test
    them without triggering the full MailProcessorService module
    import (anyio dep missing in test env; irrelevant to ak-ptxq).
    """

    def setUp(self):
        self.fn, self.hosts = _ast_lift_hdfc_url_validator()

    def test_allowlist_contents(self):
        """Pin the exact allowlist contents — adding a host must be
        a deliberate 2-touch change (constant + this assertion)."""
        self.assertEqual(
            self.hosts,
            frozenset({
                "smartstatements.hdfcbank.com",
                "smartstatements.hdfc.bank.in",
            }),
            "ak-ptxq: _HDFC_SMART_STATEMENT_HOSTS must contain "
            "exactly the two canonical HDFC Smart Statement hosts. "
            "Adding more hosts requires a deliberate review — this "
            "assertion forces the test to update alongside the "
            "code.",
        )

    # (1) Accepts allowed hosts.
    def test_accepts_canonical_hdfcbank_com(self):
        self.assertTrue(self.fn(
            "https://smartstatements.hdfcbank.com/page?jobkey=abc",
        ))

    def test_accepts_canonical_hdfc_bank_in(self):
        self.assertTrue(self.fn(
            "https://smartstatements.hdfc.bank.in/download",
        ))

    def test_rejects_http_scheme(self):
        # ak-x5l9: https only. http was tolerated by ak-ptxq; it is now
        # refused (the Customer ID password must never go over plaintext).
        self.assertFalse(self.fn(
            "http://smartstatements.hdfcbank.com/",
        ))

    def test_case_insensitive_host(self):
        # (7) uppercased host still allowlisted.
        self.assertTrue(self.fn(
            "https://SmartStatements.HdfcBank.COM/page",
        ))

    # (2) Rejects attacker hosts.
    def test_rejects_attacker_host(self):
        self.assertFalse(self.fn("https://evil.example/collect"))

    def test_rejects_look_alike_subdomain(self):
        # Exact-match (not endswith/suffix) — no `evil.hdfcbank.com`.
        self.assertFalse(self.fn(
            "https://evil.hdfcbank.com/collect",
        ))

    def test_rejects_look_alike_parent(self):
        # Reversed shape — no `hdfcbank.com` without smartstatements.
        self.assertFalse(self.fn("https://hdfcbank.com/foo"))

    # (3) Substring-exploit URL rejected.
    def test_rejects_substring_exploit_query(self):
        """ak-ptxq CORE: pre-fix the substring check `in href` would
        match this URL. host-anchored validation rejects it."""
        self.assertFalse(self.fn(
            "https://evil.example/?q=smartstatements.hdfcbank.com",
        ))

    def test_rejects_substring_exploit_path(self):
        self.assertFalse(self.fn(
            "https://evil.example/smartstatements.hdfcbank.com/foo",
        ))

    def test_rejects_substring_exploit_userinfo(self):
        # Attacker encodes the allowlisted host as userinfo. urlparse
        # puts `smartstatements.hdfcbank.com` in `username`, while
        # `hostname` (what we check) is evil.example. This passes.
        self.assertFalse(self.fn(
            "https://smartstatements.hdfcbank.com@evil.example/collect",
        ))

    # (4) Non-http(s) schemes rejected.
    def test_rejects_file_scheme(self):
        self.assertFalse(self.fn(
            "file:///etc/passwd",
        ))

    def test_rejects_javascript_scheme(self):
        self.assertFalse(self.fn(
            "javascript:alert(1)",
        ))

    def test_rejects_gopher_scheme(self):
        self.assertFalse(self.fn(
            "gopher://smartstatements.hdfcbank.com/0",
        ))

    # (5) Degenerate inputs.
    def test_rejects_none(self):
        self.assertFalse(self.fn(None))

    def test_rejects_empty_string(self):
        self.assertFalse(self.fn(""))

    def test_rejects_non_string(self):
        self.assertFalse(self.fn(12345))
        self.assertFalse(self.fn(["https://smartstatements.hdfcbank.com"]))
        self.assertFalse(self.fn({"url": "https://smartstatements.hdfcbank.com"}))

    # (6) Unparseable URL must NOT raise.
    def test_malformed_url_rejected_without_raise(self):
        # urlparse is forgiving so most strings parse as an empty
        # host; just confirm the function doesn't raise and returns
        # False for garbage.
        self.assertFalse(self.fn("not a url at all"))
        self.assertFalse(self.fn("http://"))
        self.assertFalse(self.fn("://nohost"))


# ─── Source-grep drift guards ─────────────────────────────────


class TestHardeningSourceGrep(unittest.TestCase):
    """Pin the structural shape — if someone removes the allowlist
    constant, the helper, or any of the 4 validation sites, these
    red immediately (no network / integration test required).
    """

    def setUp(self):
        self.src = _read_source("services/mailProcessorService.py")

    # (8) Constant present with both canonical hosts.
    def test_allowlist_constant_present(self):
        # ak-tspe: the allowlist lives in utils/hdfc_smart_statement.py;
        # the service aliases it and delegates the gate to it.
        self.assertRegex(
            self.src,
            r"_HDFC_SMART_STATEMENT_HOSTS\s*=\s*HDFC_SMART_STATEMENT_HOSTS",
        )
        self.assertIn("return is_hdfc_smart_statement_url(url)", self.src)
        util_src = _read_source("utils/hdfc_smart_statement.py")
        self.assertIsNotNone(
            re.search(
                r"HDFC_SMART_STATEMENT_HOSTS\s*=\s*frozenset",
                util_src,
            ),
            "ak-ptxq: _HDFC_SMART_STATEMENT_HOSTS allowlist constant "
            "missing — the sole gate between attacker URLs and "
            "credentialed session.get/.post calls",
        )
        self.assertIn(
            '"smartstatements.hdfcbank.com"', util_src,
            "ak-ptxq: smartstatements.hdfcbank.com must appear in "
            "the host allowlist",
        )
        self.assertIn(
            '"smartstatements.hdfc.bank.in"', util_src,
            "ak-ptxq: smartstatements.hdfc.bank.in must appear in "
            "the host allowlist",
        )

    # (9) Strategy-1 colour-match uses the gate.
    def test_strategy1_colour_match_uses_host_gate(self):
        # Slice the colour-match block and assert the gate call
        # lives between td_tag.find and the return.
        m = re.search(
            r"td_tag\s*=\s*soup\.find\(\"td\", style=.*?004b8d.*?"
            r"return\s+href",
            self.src, re.DOTALL,
        )
        self.assertIsNotNone(
            m,
            "ak-ptxq: strategy-1 colour-match block not found in "
            "_extract_smart_statement_link — did the shape change?",
        )
        block = m.group(0)
        self.assertIn(
            "_is_hdfc_smart_statement_url", block,
            "ak-ptxq: strategy-1 colour-match MUST gate the href "
            "through _is_hdfc_smart_statement_url before returning "
            "— a phishing email can match the #004b8d colour easily",
        )

    # (10) Substring-check replaced with host gate.
    def test_substring_check_replaced(self):
        # The pre-fix shape `"smartstatements.hdfcbank.com" in href`
        # must be entirely absent — any reintroduction reds this.
        self.assertNotRegex(
            self.src,
            r'"smartstatements\.hdfcbank\.com"\s+in\s+href',
            "ak-ptxq: substring check `\"smartstatements.hdfcbank.com\" "
            "in href` must be fully removed — host-anchored "
            "validation only. Pre-fix shape resurfaced.",
        )
        self.assertNotRegex(
            self.src,
            r'"smartstatements\.hdfc\.bank\.in"\s+in\s+href',
            "ak-ptxq: substring check for the second canonical host "
            "must be fully removed",
        )

    # (11) Pre-POST allowlist gate in _download_from_smart_statement_link.
    def test_pre_post_allowlist_gate_present(self):
        # The final session.post MUST be preceded by an
        # _is_hdfc_smart_statement_url check — enforce via text
        # position (last _is_hdfc call index < last session.post index
        # AND in close vicinity).
        post_idx = self.src.rfind("session.post(action, data=login_data")
        self.assertGreater(
            post_idx, -1,
            "ak-ptxq: the password-carrying session.post call not "
            "found — shape change; review this guard",
        )
        # Find the nearest preceding _is_hdfc_smart_statement_url
        # call. Must be within 800 chars (same function).
        preceding = self.src.rfind(
            "_is_hdfc_smart_statement_url(action)", 0, post_idx,
        )
        self.assertGreater(
            preceding, -1,
            "ak-ptxq: no _is_hdfc_smart_statement_url(action) call "
            "precedes the credentialed session.post — final gate "
            "missing.",
        )
        self.assertLess(
            post_idx - preceding, 800,
            "ak-ptxq: final allowlist gate is >800 chars before the "
            "session.post — likely a refactor moved them apart. "
            "Review that nothing bypasses the gate.",
        )

    # (12) Password log-hygiene guard — no password / login_data[*]
    # values appearing in log-call shapes. Keys-only is OK.
    def test_password_not_in_logs(self):
        # Scan for any logger call that f-strings the password or
        # login_data VALUES. Allowed shape (keys only): list(login_data.keys()).
        # Any `self.logger.*(... {password} ...)` is a red flag.
        # Match f-string segments inside logger.debug/info/warning/error.
        offenders = re.findall(
            r"self\.logger\.(?:debug|info|warning|error)"
            r"\([^)]*\{password\}[^)]*\)",
            self.src,
        )
        self.assertEqual(
            offenders, [],
            f"ak-ptxq §23 #4: password MUST NOT appear in log calls. "
            f"Found: {offenders!r}",
        )
        # Also disallow interpolating login_data as a whole (values
        # include password at custid field).
        value_offenders = re.findall(
            r"self\.logger\.(?:debug|info|warning|error)"
            r"\([^)]*\{login_data\}[^)]*\)",
            self.src,
        )
        self.assertEqual(
            value_offenders, [],
            f"ak-ptxq §23 #4: login_data (which carries the password "
            f"at the custid field) MUST NOT be interpolated whole "
            f"into log calls — keys only. Found: {value_offenders!r}",
        )


# ─── Entry-gate source-grep (behavior-test substitute) ───────
#
# A true behavior test of `_download_from_smart_statement_link`
# would import MailProcessorService, but the full module import
# pulls in `anyio` which isn't in this test env. The AST-lift
# pattern works for a leaf method like _is_hdfc_smart_statement_
# url (self-contained), but _download_from_smart_statement_link
# touches dozens of other members + requires requests + bs4. The
# pragmatic substitute: source-grep that the entry check lives
# IMMEDIATELY after the function def, before ANY `requests` call.


class TestDownloadFunctionEntryGate(unittest.TestCase):

    def setUp(self):
        self.src = _read_source("services/mailProcessorService.py")
        # Slice the whole _download_from_smart_statement_link body.
        m = re.search(
            r"def _download_from_smart_statement_link\(.*?\)"
            r".*?(?=\n    def [a-zA-Z_])",
            self.src, re.DOTALL,
        )
        self.assertIsNotNone(
            m,
            "_download_from_smart_statement_link body not found — "
            "shape changed; review this test",
        )
        self.body = m.group(0)

    def _strip_docstring(self, body):
        """Strip the leading triple-quoted docstring from the
        sliced function body so our pattern searches skip prose
        mentions of `session.get()` / `session.post()`."""
        # Find the first triple-quote (starts docstring) and the
        # matching close. Non-greedy match handles both """ and '''.
        m = re.search(
            r'^(?:\s*def [^\n]+\n\s*)("""|\'\'\')([\s\S]*?)\1',
            body,
        )
        if not m:
            return body
        return body[:m.start(1)] + body[m.end(1):]

    # (13) Entry gate precedes ALL `requests`-touching lines.
    def test_entry_gate_before_any_requests(self):
        body_no_doc = self._strip_docstring(self.body)
        # Position of the first `if not self._is_hdfc...(link):`.
        gate_match = re.search(
            r"if\s+not\s+self\._is_hdfc_smart_statement_url\(link\)",
            body_no_doc,
        )
        self.assertIsNotNone(
            gate_match,
            "ak-ptxq: entry-gate "
            "`if not self._is_hdfc_smart_statement_url(link)` not "
            "found at the top of _download_from_smart_statement_"
            "link — this is the belt-and-brace guard against any "
            "caller that bypassed _extract_smart_statement_link's "
            "allowlist.",
        )
        gate_idx = gate_match.start()
        # Positions of every `session.get(...)` / `session.post(...)`
        # with an actual argument (skip empty-paren docstring
        # mentions, though we stripped the docstring anyway).
        bad_idxs = [
            m.start() for m in re.finditer(
                r"session\.(?:get|post)\([^)]|requests\.Session\(",
                body_no_doc,
            )
        ]
        if not bad_idxs:
            self.fail(
                "No requests calls found in the function body — "
                "shape changed; review this test"
            )
        for idx in bad_idxs:
            self.assertLess(
                gate_idx, idx,
                f"ak-ptxq: entry gate must precede ALL requests "
                f"calls. A requests call at offset {idx} happens "
                f"BEFORE the gate at {gate_idx}. Attacker link "
                f"would reach the network before being rejected.",
            )

    # (14) Fail-closed path on gate miss returns None (not falls
    # through or raises).
    def test_entry_gate_fails_closed_with_return_none(self):
        body_no_doc = self._strip_docstring(self.body)
        # The if-block through `return None` is a tight window.
        self.assertRegex(
            body_no_doc,
            r"if\s+not\s+self\._is_hdfc_smart_statement_url\(link\)"
            r"[\s\S]{0,400}return\s+None",
            "ak-ptxq: entry-gate must fail CLOSED with `return "
            "None` inside `if not self._is_hdfc_smart_statement_"
            "url(link):` within ~400 chars.",
        )

    # (15) Fail-closed log line mentions ak-ptxq for operator triage.
    def test_entry_gate_log_mentions_bead(self):
        gate_idx = self.body.find("_is_hdfc_smart_statement_url(link)")
        window = self.body[gate_idx:gate_idx + 500]
        self.assertIn(
            "ak-ptxq", window,
            "ak-ptxq: entry-gate warning log should mention the "
            "bead tag so prod triage finds this path. Window:\n"
            + window,
        )


if __name__ == "__main__":  # pragma: no cover
    unittest.main(verbosity=2)
