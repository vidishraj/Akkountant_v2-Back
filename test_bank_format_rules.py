"""ak-x6p regression tests for services/bankFormatRules.py.

Root cause of the ak-x6p bug (fileDetails.bank=UNKNOWN for BOI
statements even though the ak-2ql domain-map was updated) was
`get_bank_from_sender` doing an EXACT match against DOMAIN_TO_BANK:
the resolved sender domain `alerts.bankofindia.bank.in` never
matched `bankofindia.bank.in` in the map, so downstream stamped
"UNKNOWN" on both the LLM prompt hint AND (via the same variable)
the fileDetails row.

These tests lock in the subdomain-suffix match so a future
regression re-opens loudly. Pure-Python; no framework deps.
"""

import os
import sys
import unittest

sys.path.insert(0, os.path.dirname(os.path.abspath(__file__)))

from services.bankFormatRules import (
    DOMAIN_TO_BANK,
    get_bank_from_sender,
    get_banks_for_domain,
)


class TestGetBankFromSenderExactMatch(unittest.TestCase):
    """Backward-compatibility: exact-match callers keep working."""

    def test_hdfc_canonical(self):
        self.assertEqual(
            get_bank_from_sender("noreply@hdfcbank.net"), "HDFC_DEBIT",
        )

    def test_boi_legacy(self):
        # Pre-ak-2ql BOI domain still works.
        self.assertEqual(
            get_bank_from_sender("estatements@bankofindia.co.in"), "BOI",
        )

    def test_boi_bank_in(self):
        # ak-2ql-added .bank.in root domain (exact, no subdomain).
        self.assertEqual(
            get_bank_from_sender("noreply@bankofindia.bank.in"), "BOI",
        )

    def test_no_at_sign_returns_unknown(self):
        self.assertEqual(get_bank_from_sender("noatsignhere"), "UNKNOWN")

    def test_wrapped_email(self):
        # "Display Name <email@domain>" format.
        self.assertEqual(
            get_bank_from_sender("Bank of India <noreply@bankofindia.co.in>"),
            "BOI",
        )


class TestGetBankFromSenderSubdomain(unittest.TestCase):
    """ak-x6p fix: subdomain-suffix match resolves alerts.<canonical>."""

    def test_boi_alerts_subdomain(self):
        """The specific case from the ak-5oi backfill: BOI's new
        estatement sender uses an `alerts.` subdomain prefix."""
        self.assertEqual(
            get_bank_from_sender(
                "noreply-estatement@alerts.bankofindia.bank.in",
            ),
            "BOI",
        )

    def test_hdfc_alerts_subdomain(self):
        self.assertEqual(
            get_bank_from_sender("foo@alerts.hdfcbank.bank.in"),
            "HDFC_DEBIT",
        )

    def test_yes_bank_mail_subdomain(self):
        self.assertEqual(
            get_bank_from_sender("x@mail.yesbank.in"),
            "YES_BANK_DEBIT",
        )

    def test_deep_subdomain(self):
        # Multi-level subdomain still matches on the base.
        self.assertEqual(
            get_bank_from_sender("y@a.b.c.hdfcbank.net"),
            "HDFC_DEBIT",
        )

    def test_dot_boundary_guard(self):
        """Non-boundary substring match must NOT resolve.
        `evilbankofindia.bank.in` is NOT `bankofindia.bank.in` even
        though the raw string endswith it — the leading dot is
        required to prevent hijacking."""
        self.assertEqual(
            get_bank_from_sender("bad@evilbankofindia.bank.in"),
            "UNKNOWN",
        )

    def test_no_match_still_unknown(self):
        self.assertEqual(
            get_bank_from_sender("nobody@nowhere.example"),
            "UNKNOWN",
        )

    def test_wrapped_subdomain_email(self):
        self.assertEqual(
            get_bank_from_sender(
                "BOI <noreply-estatement@alerts.bankofindia.bank.in>",
            ),
            "BOI",
        )


class TestGetBanksForDomain(unittest.TestCase):
    """The new suffix-tolerant lookup helper used by password-lookup
    call sites in mailProcessorService.py."""

    def test_exact_hit(self):
        result = get_banks_for_domain("bankofindia.bank.in")
        self.assertEqual(result, ["BOI"])

    def test_subdomain_hit(self):
        result = get_banks_for_domain("alerts.bankofindia.bank.in")
        self.assertEqual(result, ["BOI"])

    def test_empty(self):
        self.assertEqual(get_banks_for_domain(""), [])
        self.assertEqual(get_banks_for_domain(None), [])

    def test_no_match(self):
        self.assertEqual(get_banks_for_domain("nowhere.example"), [])

    def test_dot_boundary_guard(self):
        # Same guard as get_bank_from_sender — non-boundary suffix
        # must not resolve.
        self.assertEqual(
            get_banks_for_domain("evilbankofindia.bank.in"), [],
        )

    def test_trailing_bracket_stripped(self):
        # Callers may pass "alerts.bankofindia.bank.in>" from a
        # partially-parsed sender header; helper strips gracefully.
        self.assertEqual(
            get_banks_for_domain("alerts.bankofindia.bank.in>"),
            ["BOI"],
        )


class TestDomainToBankShape(unittest.TestCase):
    """Sanity that DOMAIN_TO_BANK still contains the ak-2ql-critical
    entries so the .bank.in TLD migration doesn't accidentally
    regress out of the map."""

    def test_boi_bank_in_present(self):
        self.assertEqual(DOMAIN_TO_BANK.get("bankofindia.bank.in"), "BOI")

    def test_hdfc_bank_in_present(self):
        self.assertEqual(
            DOMAIN_TO_BANK.get("hdfcbank.bank.in"), "HDFC_DEBIT",
        )


if __name__ == "__main__":
    print("ak-x6p bankFormatRules regression tests")
    print("=" * 60)
    unittest.main(verbosity=2, exit=False)
    print("=" * 60)
