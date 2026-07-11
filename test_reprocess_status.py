"""ak-1rh regression tests — reprocess_pdf return contract.

Pure-Python coverage of summarize_reprocess_result and
_classify_reason. The service-layer integration
(services/mailProcessorService.reprocess_pdf) exercises the same
helper — env-limited integration tests stay deferred at lead-verify.

Locked behavior:

  Status ladder:
    - success: analysis.success=True AND no failed_chunks AND
      transactions_inserted > 0
    - empty:   analysis.success=True AND no failed_chunks AND
      transactions_inserted == 0 (ambiguous: legit empty statement
      OR silent extractor miss)
    - partial: chunks_failed > 0 AND transactions_inserted > 0
    - failed:  every-chunk-failure (chunks_failed > 0 AND tx == 0)
      OR analysis.success=False AND tx == 0
"""

import os
import sys
import unittest

sys.path.insert(0, os.path.dirname(os.path.abspath(__file__)))

from utils.reprocess_status import (
    _classify_reason,
    summarize_reprocess_result,
)


# ── Status classification ───────────────────────────────────────────


class TestStatusSuccess(unittest.TestCase):
    def test_all_chunks_ok_positive_tx(self):
        res = summarize_reprocess_result(
            {"success": True, "failed_chunks": [], "total_chunks": 8},
            transactions_inserted=127,
        )
        self.assertEqual(res["status"], "success")
        self.assertEqual(res["chunks_ok"], 8)
        self.assertEqual(res["chunks_failed"], 0)
        self.assertEqual(res["chunks_total"], 8)
        self.assertEqual(res["transactions_inserted"], 127)
        self.assertEqual(res["failures"], [])


class TestStatusEmpty(unittest.TestCase):
    """All chunks succeeded but zero tx landed. Ambiguous —
    caller inspects the log to decide."""

    def test_zero_tx_success_marked_empty(self):
        res = summarize_reprocess_result(
            {"success": True, "failed_chunks": [], "total_chunks": 4},
            transactions_inserted=0,
        )
        self.assertEqual(res["status"], "empty")
        self.assertEqual(res["chunks_failed"], 0)


class TestStatusPartial(unittest.TestCase):
    """Some chunks failed but ledger got at least one row. Callers
    can re-run the file — ak-8l5 dedup will absorb the already-done
    chunks."""

    def test_some_chunks_failed_some_tx_landed(self):
        res = summarize_reprocess_result(
            {"success": False,
             "failed_chunks": [[3, 4], [5, 6]],
             "total_chunks": 8},
            transactions_inserted=42,
        )
        self.assertEqual(res["status"], "partial")
        self.assertEqual(res["chunks_ok"], 6)
        self.assertEqual(res["chunks_failed"], 2)
        self.assertEqual(res["chunks_total"], 8)
        self.assertEqual(res["transactions_inserted"], 42)


class TestStatusFailed(unittest.TestCase):
    """Full failure — every chunk died OR analysis says failure and
    ledger got nothing."""

    def test_all_chunks_failed_zero_tx(self):
        res = summarize_reprocess_result(
            {"success": False,
             "failed_chunks": [[1, 2], [3, 4], [5, 6], [7, 8]],
             "total_chunks": 4},
            transactions_inserted=0,
        )
        self.assertEqual(res["status"], "failed")
        # chunks_total was 4 (page pairs are a different unit here —
        # the helper just counts what the summary claims).
        self.assertEqual(res["chunks_total"], 4)

    def test_analysis_exception_zero_tx(self):
        """pdf_open_failed / analysis_exception path from ak-bwe v2
        emits success=False with reason set. Status must be failed."""
        res = summarize_reprocess_result(
            {"success": False,
             "failed_chunks": [], "total_chunks": 0,
             "reason": "pdf_open_failed: file not found"},
            transactions_inserted=0,
        )
        self.assertEqual(res["status"], "failed")
        self.assertEqual(res["analysis_reason"], "pdf_open_failed: file not found")

    def test_none_summary_zero_tx_failed(self):
        """A caller passing None (legacy path, or a caller that
        didn't get a summary back) → failed. Never marked success."""
        res = summarize_reprocess_result(None, transactions_inserted=0)
        self.assertEqual(res["status"], "failed")
        self.assertEqual(res["chunks_ok"], 0)
        self.assertEqual(res["chunks_failed"], 0)


class TestNonDictSummary(unittest.TestCase):
    """Bad summary shapes → treated as failure with zero counts."""

    def test_string_summary_failed(self):
        res = summarize_reprocess_result(
            "everything is fine", transactions_inserted=0,
        )
        self.assertEqual(res["status"], "failed")

    def test_int_summary_failed(self):
        res = summarize_reprocess_result(42, transactions_inserted=0)
        self.assertEqual(res["status"], "failed")


class TestReviewerScenario(unittest.TestCase):
    """The scenario Lead's dispatch called out verbatim:
      'ak-32o run 1 reported all 12 files "success" while files
       4/7/8/9 silently produced 0 tx.'
    Under ak-1rh, that same shape produces "failed" (or "partial"
    if some tx landed), never "success"."""

    def test_all_chunks_died_zero_tx_reports_failed(self):
        # 8 of 8 chunks failed. Pre-ak-1rh: status='success' (BUG).
        # Post-ak-1rh: status='failed'.
        res = summarize_reprocess_result(
            {"success": False,
             "failed_chunks": [
                 [1, 2], [3, 4], [5, 6], [7, 8],
                 [9, 10], [11, 12], [13, 14], [15, 16],
             ],
             "total_chunks": 8,
             "reason": "mysql 1040 too many connections"},
            transactions_inserted=0,
        )
        self.assertEqual(res["status"], "failed")
        self.assertEqual(res["chunks_failed"], 8)
        # The failures list carries page ranges + classified type
        # so a caller can log per-chunk.
        self.assertEqual(len(res["failures"]), 8)
        for f in res["failures"]:
            self.assertEqual(f["error_type"], "mysql_1040")


# ── failures list contents ──────────────────────────────────────────


class TestFailuresList(unittest.TestCase):
    """The failures list carries {page_range, error_type, message}
    per failed chunk so callers can log per-chunk without needing to
    inspect the summary's raw shape."""

    def test_failures_carry_page_range(self):
        res = summarize_reprocess_result(
            {"success": False,
             "failed_chunks": [[3, 4], [7, 8]],
             "total_chunks": 4,
             "reason": "SDK Fatal error in message reader"},
            transactions_inserted=0,
        )
        self.assertEqual(len(res["failures"]), 2)
        self.assertEqual(res["failures"][0]["page_range"], [3, 4])
        self.assertEqual(res["failures"][1]["page_range"], [7, 8])

    def test_failures_carry_classified_error_type(self):
        res = summarize_reprocess_result(
            {"success": False,
             "failed_chunks": [[1, 2]],
             "total_chunks": 1,
             "reason": "SDK Fatal error in message reader"},
            transactions_inserted=0,
        )
        self.assertEqual(res["failures"][0]["error_type"], "sdk_message_reader")

    def test_no_failures_when_no_failed_chunks(self):
        res = summarize_reprocess_result(
            {"success": True, "failed_chunks": [], "total_chunks": 4},
            transactions_inserted=10,
        )
        self.assertEqual(res["failures"], [])


# ── _classify_reason ────────────────────────────────────────────────


class TestClassifyReason(unittest.TestCase):
    """Best-effort mapping from a reason string to a canonical
    error_type label."""

    def test_mysql_1040(self):
        self.assertEqual(_classify_reason("mysql error 1040"), "mysql_1040")
        self.assertEqual(_classify_reason("too many connections"),
                         "mysql_1040")
        self.assertEqual(_classify_reason("MAX_CONNECTIONS exceeded"),
                         "mysql_1040")

    def test_sdk_message_reader(self):
        self.assertEqual(
            _classify_reason("Fatal error in message reader"),
            "sdk_message_reader",
        )
        self.assertEqual(
            _classify_reason("message reader crashed mid-stream"),
            "sdk_message_reader",
        )

    def test_pdf_password(self):
        self.assertEqual(_classify_reason("PDF is password-protected"),
                         "pdf_password")
        self.assertEqual(_classify_reason("failed to authenticate"),
                         "pdf_password")

    def test_pdf_open(self):
        self.assertEqual(_classify_reason("pdf_open_failed: not found"),
                         "pdf_open")
        self.assertEqual(_classify_reason("cannot open PDF"), "pdf_open")

    def test_extraction_empty(self):
        self.assertEqual(_classify_reason("returned 0 transactions"),
                         "extraction_empty")

    def test_stream(self):
        self.assertEqual(_classify_reason("stream reset by peer"),
                         "stream")
        self.assertEqual(_classify_reason("connection reset"), "stream")
        self.assertEqual(_classify_reason("unexpected EOF"), "stream")

    def test_rate_limit(self):
        self.assertEqual(_classify_reason("rate_limit exceeded"),
                         "rate_limit")
        self.assertEqual(_classify_reason("Rate Limit reached"),
                         "rate_limit")

    def test_timeout(self):
        self.assertEqual(_classify_reason("request timeout"), "timeout")
        self.assertEqual(_classify_reason("deadline exceeded"), "timeout")

    def test_analysis_exception(self):
        self.assertEqual(
            _classify_reason("analysis_exception: something broke"),
            "analysis_exception",
        )

    def test_unknown_chunk_result_shape(self):
        self.assertEqual(
            _classify_reason("unknown_chunk_result_shape: NoneType"),
            "unknown_chunk_result_shape",
        )

    def test_no_reason_returns_unknown(self):
        self.assertEqual(_classify_reason(None), "unknown")
        self.assertEqual(_classify_reason(""), "unknown")
        self.assertEqual(_classify_reason("something random"), "unknown")

    def test_non_string_returns_unknown(self):
        self.assertEqual(_classify_reason(42), "unknown")
        self.assertEqual(_classify_reason(["a", "b"]), "unknown")


# ── Numeric coercion / defensive shapes ────────────────────────────


class TestCoercion(unittest.TestCase):
    """Bad shapes in the summary shouldn't crash the helper."""

    def test_non_numeric_transactions_becomes_zero(self):
        res = summarize_reprocess_result(
            {"success": True, "failed_chunks": [], "total_chunks": 4},
            transactions_inserted="not-a-number",
        )
        # Non-numeric → 0 → status=empty because chunks_ok=4 tx=0.
        self.assertEqual(res["status"], "empty")
        self.assertEqual(res["transactions_inserted"], 0)

    def test_non_list_failed_chunks_becomes_empty(self):
        res = summarize_reprocess_result(
            {"success": True, "failed_chunks": "boom", "total_chunks": 4},
            transactions_inserted=10,
        )
        self.assertEqual(res["chunks_failed"], 0)
        self.assertEqual(res["status"], "success")

    def test_missing_total_chunks_defaults_zero(self):
        res = summarize_reprocess_result(
            {"success": True, "failed_chunks": []},
            transactions_inserted=10,
        )
        self.assertEqual(res["chunks_total"], 0)


if __name__ == "__main__":
    print("ak-1rh reprocess_pdf return contract tests")
    print("=" * 60)
    unittest.main(verbosity=2, exit=False)
    print("=" * 60)
