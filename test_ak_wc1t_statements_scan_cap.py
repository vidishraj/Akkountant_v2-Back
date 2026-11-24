"""ak-wc1t (2026-11-24): ak-d06w follow-ups on the statements list
(TransactionService.fetchProcessedStatements).

1. The bank filter scans at most STATEMENT_BANK_SCAN_CAP statement rows.
   Hitting the cap was silent (an incomplete list looked complete). The
   response now carries truncated (exact: one row past the cap is fetched)
   and scan_cap. Both are additive.
2. Sort-by-bank on the filtered path ordered by the sender STRING. It now
   orders by the resolved bank, with ties newest-first (every match of a
   single-bank filter shares one bank, so it reads newest-first). The scan
   is ordered newest-first, so the cap keeps the most recent statements.
Reuses the ak-d06w real-SQLite fixture. Synthetic data only.
"""
from __future__ import annotations

import os
import sys
import unittest
from unittest import mock

HERE = os.path.dirname(os.path.abspath(__file__))
sys.path.insert(0, HERE)

from test_ak_d06w_processed_statements import _DB  # noqa: E402


class TestScanCapFlag(_DB):

    def fetch(self, **filters):
        return self.ts.fetchProcessedStatements("u1", filters=filters)

    def test_unfiltered_has_no_scan(self):
        out = self.fetch()
        self.assertEqual((out["truncated"], out["scan_cap"]), (False, None))

    def test_filtered_under_cap(self):
        out = self.fetch(bank="HDFC_DEBIT")
        self.assertEqual((out["truncated"], out["scan_cap"]), (False, self.TS.STATEMENT_BANK_SCAN_CAP))
        self.assertEqual(self.TS.STATEMENT_BANK_SCAN_CAP, 5000)

    def test_exactly_at_cap_is_not_truncated(self):
        # u1 has 5 statement rows; scanning exactly 5 is complete.
        with mock.patch.object(self.TS, "STATEMENT_BANK_SCAN_CAP", 5):
            out = self.fetch(bank="HDFC_DEBIT")
        self.assertEqual((out["truncated"], out["scan_cap"]), (False, 5))
        self.assertEqual(self._ids(out), ["g_hdfc"])

    def test_over_cap_is_truncated_and_keeps_newest(self):
        # Four matching rows, cap 3: the row fetched past the cap is a match
        # too, so it must be dropped (not counted / not listed).
        for m in (19, 20, 21, 22):
            self._row(f"g_new{m}", "HDFC <alerts@hdfcbank.net>", "bank_statement", minute=m)
        self.session.commit()
        with mock.patch.object(self.TS, "STATEMENT_BANK_SCAN_CAP", 3), \
                self.assertLogs(level="WARNING") as cm:
            out = self.fetch(bank="HDFC_DEBIT")
        self.assertEqual((out["truncated"], out["scan_cap"]), (True, 3))
        self.assertEqual(self._ids(out), ["g_new22", "g_new21", "g_new20"])   # newest 3
        self.assertEqual(out["total_count"], 3)
        self.assertTrue(any("scan cap" in line for line in cm.output))
        self.assertFalse(any("hdfcbank" in line for line in cm.output))  # no sender data logged


class TestSortByResolvedBank(_DB):

    def seed_two(self):
        # Same resolved bank (HDFC_DEBIT), senders chosen so that sender
        # order is the OPPOSITE of recency.
        self._row("g_old", "AAA <a@hdfcbank.net>", "bank_statement", minute=30)
        self._row("g_newest", "ZZZ <z@hdfcbank.net>", "bank_statement", minute=40)
        self.session.commit()

    def ids_sorted(self, order, bank="HDFC_DEBIT"):
        f = {"sorted": {"column": "bank", "order": order}}
        if bank:
            f["bank"] = bank
        return self._ids(self.ts.fetchProcessedStatements("u1", filters=f))

    def test_filtered_bank_sort_ignores_sender_and_reads_newest_first(self):
        self.seed_two()
        for order in ("asc", "desc"):
            self.assertEqual(self.ids_sorted(order), ["g_newest", "g_old", "g_hdfc"], order)

    def test_resolved_bank_not_sender_decides(self):
        # g_cc's sender is HDFC but its resolved bank is Millenia_Credit
        # (summary): it is not in the HDFC_DEBIT list at all.
        self.seed_two()
        self.assertNotIn("g_cc", self.ids_sorted("asc"))
        self.assertEqual(self.ids_sorted("asc", bank="Millenia_Credit"), ["g_cc"])

    def test_paging_follows_the_resolved_order(self):
        self.seed_two()
        f = {"bank": "HDFC_DEBIT", "sorted": {"column": "bank", "order": "asc"}}
        p1 = self.ts.fetchProcessedStatements("u1", page=1, limit=2, filters=f)
        p2 = self.ts.fetchProcessedStatements("u1", page=2, limit=2, filters=f)
        self.assertEqual(self._ids(p1) + self._ids(p2), ["g_newest", "g_old", "g_hdfc"])
        self.assertEqual(p1["total_count"], 3)

    def test_unfiltered_bank_sort_unchanged_sender_grouping(self):
        # No filter: the resolved bank isn't a column; sender order is kept.
        self.seed_two()
        ids = self.ids_sorted("asc", bank=None)
        self.assertLess(ids.index("g_old"), ids.index("g_newest"))     # 'AAA' < 'ZZZ'

    def test_other_sorts_on_filtered_path_unchanged(self):
        self.seed_two()
        f = {"bank": "HDFC_DEBIT", "sorted": {"column": "processed_at", "order": "asc"}}
        self.assertEqual(self._ids(self.ts.fetchProcessedStatements("u1", filters=f)),
                         ["g_hdfc", "g_old", "g_newest"])


class TestReviewOrdering(_DB):
    """Review minor: id DESC tie-breaker everywhere, and the filtered cap
    always keeps the newest rows whatever the requested sort."""

    def page_ids(self, filters, n_pages=3, limit=2):
        out = []
        for p in range(1, n_pages + 1):
            out += self._ids(self.ts.fetchProcessedStatements("u1", page=p, limit=limit, filters=filters))
        return out

    def seed_ties(self):
        for g in ("t1", "t2", "t3"):          # identical processed_at (minute 50)
            self._row(g, "HDFC <alerts@hdfcbank.net>", "bank_statement", minute=50)
        self.session.commit()
        ids = {r.gmail_id: r.id for r in self.session.query(self.PE).filter(
            self.PE.gmail_id.in_(["t1", "t2", "t3"])).all()}
        return [g for g, _ in sorted(ids.items(), key=lambda kv: kv[1], reverse=True)]

    def test_tied_sort_column_unfiltered_two_pages_no_dup_no_gap(self):
        tie_order = self.seed_ties()
        f = {"sorted": {"column": "processed_at", "order": "desc"}}
        got = self.page_ids(f, n_pages=4, limit=2)
        self.assertEqual(len(got), len(set(got)))                 # no duplicates
        self.assertEqual(set(got), {"t1", "t2", "t3", "g_hdfc", "g_cc", "g_null", "g_mis", "g_cas"})
        self.assertEqual(got[:3], tie_order)                       # ties by id DESC

    def test_tied_sort_column_filtered_two_pages(self):
        tie_order = self.seed_ties()
        for order in ("asc", "desc"):
            f = {"bank": "HDFC_DEBIT", "sorted": {"column": "processed_at", "order": order}}
            got = self.page_ids(f, n_pages=2, limit=2)
            self.assertEqual(len(got), len(set(got)), order)
            self.assertEqual(set(got), {"t1", "t2", "t3", "g_hdfc"}, order)
            ties = [g for g in got if g.startswith("t")]
            self.assertEqual(ties, tie_order, order)               # stable id DESC within ties

    def test_ascending_sort_with_cap_keeps_newest(self):
        for m in (19, 20, 21, 22):
            self._row(f"g_new{m}", "HDFC <alerts@hdfcbank.net>", "bank_statement", minute=m)
        self.session.commit()
        f = {"bank": "HDFC_DEBIT", "sorted": {"column": "processed_at", "order": "asc"}}
        with mock.patch.object(self.TS, "STATEMENT_BANK_SCAN_CAP", 3), self.assertLogs(level="WARNING"):
            out = self.ts.fetchProcessedStatements("u1", filters=f)
        self.assertTrue(out["truncated"])
        # The newest 3 (not the oldest 3), shown in the requested ascending order.
        self.assertEqual(self._ids(out), ["g_new20", "g_new21", "g_new22"])

    def test_filtered_python_sort_matches_columns(self):
        self._row("s_b", "HDFC <alerts@hdfcbank.net>", "bank_statement", minute=31)
        self._row("s_a", "HDFC <alerts@hdfcbank.net>", "bank_statement", minute=32)
        self.session.commit()
        rows = {r.gmail_id: r for r in self.session.query(self.PE).all()}
        rows["s_a"].subject, rows["s_b"].subject, rows["g_hdfc"].subject = "alpha", "Beta", "gamma"
        rows["s_a"].email_date = dt_(2026, 11, 2)
        rows["s_b"].email_date = dt_(2026, 11, 1)
        self.session.commit()
        f = lambda col, order: self._ids(self.ts.fetchProcessedStatements(
            "u1", filters={"bank": "HDFC_DEBIT", "sorted": {"column": col, "order": order}}))
        self.assertEqual(f("fileName", "asc"), ["s_a", "s_b", "g_hdfc"])      # case-insensitive
        self.assertEqual(f("email_date", "asc"), ["g_hdfc", "s_b", "s_a"])    # NULL first asc
        self.assertEqual(f("email_date", "desc"), ["s_a", "s_b", "g_hdfc"])   # NULL last desc

    def test_scan_and_offset_orders_end_with_id_desc(self):
        with open(os.path.join(HERE, "services", "transactionsService.py")) as fh:
            src = fh.read()
        i = src.index("def fetchProcessedStatements(")
        body = src[i:src.index("\n    def ", i + 10)]
        self.assertIn("query.order_by(desc(ProcessedEmails.processed_at), desc(ProcessedEmails.id))", body)
        self.assertIn("query.order_by(order_fn(sort_col), desc(ProcessedEmails.id))", body)


def dt_(*a):
    import datetime as _dt
    return _dt.datetime(*a)


if __name__ == "__main__":  # pragma: no cover
    unittest.main(verbosity=2)
