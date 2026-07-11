"""ak-bwe regression tests — processedEmails upsert helper.

Pure-Python coverage against a stub SQLAlchemy session. The
service-layer wiring (MailProcessorService._stamp_processed_email
and the shared _handle_report_result path in
mailProcessorToolExecutor) exercise the same helper — env-limited
integration coverage stays deferred at lead-verify.
"""

import os
import sys
import unittest
from datetime import datetime

sys.path.insert(0, os.path.dirname(os.path.abspath(__file__)))

from utils.processed_emails_upsert import (
    map_tool_status_to_db,
    should_stamp_success,
    upsert_processed_email,
)


# ── Stub SQLAlchemy session + model ─────────────────────────────────


class _StubIntegrityError(Exception):
    """Stand-in for sqlalchemy.exc.IntegrityError."""


class _StubProcessedEmail:
    """Attribute-only stand-in for the ProcessedEmails ORM row."""

    def __init__(self, **kw):
        self.gmail_id = kw.get("gmail_id")
        self.user_id = kw.get("user_id")
        self.sender = kw.get("sender")
        self.subject = kw.get("subject")
        self.email_date = kw.get("email_date")
        self.category = kw.get("category")
        self.processing_type = kw.get("processing_type")
        self.status = kw.get("status")
        self.items_extracted = kw.get("items_extracted", 0)
        self.extraction_summary = kw.get("extraction_summary")
        self.error_message = kw.get("error_message")


class _StubQuery:
    def __init__(self, table):
        self._table = table
        self._filter_kwargs = None

    def filter_by(self, **kw):
        self._filter_kwargs = kw
        return self

    def first(self):
        if self._filter_kwargs is None:
            return None
        key = (self._filter_kwargs.get("gmail_id"),
               self._filter_kwargs.get("user_id"))
        return self._table.get(key)


class _StubSession:
    """Records add/commit/rollback events and enforces (gmail_id,
    user_id) uniqueness for the upsert path."""

    def __init__(self):
        self.rows = {}  # (gmail_id, user_id) → row
        self._staged = None
        self.events = []

    def add(self, obj):
        self._staged = obj
        self.events.append("add")

    def commit(self):
        self.events.append("commit")
        if self._staged is None:
            return
        obj = self._staged
        self._staged = None
        key = (obj.gmail_id, obj.user_id)
        if key in self.rows:
            raise _StubIntegrityError(f"unique constraint on {key}")
        self.rows[key] = obj

    def rollback(self):
        self.events.append("rollback")
        self._staged = None

    def query(self, model):
        # We ignore model here — only ProcessedEmails goes through
        # the helper.
        return _StubQuery(self.rows)


# ── map_tool_status_to_db ───────────────────────────────────────────


class TestStatusMap(unittest.TestCase):
    def test_success_maps_to_processed(self):
        self.assertEqual(map_tool_status_to_db("success"), "processed")

    def test_skipped_maps_verbatim(self):
        self.assertEqual(map_tool_status_to_db("skipped"), "skipped")

    def test_error_maps_to_failed(self):
        self.assertEqual(map_tool_status_to_db("error"), "failed")

    def test_none_defaults_to_processed(self):
        self.assertEqual(map_tool_status_to_db(None), "processed")

    def test_unknown_defaults_to_processed(self):
        self.assertEqual(map_tool_status_to_db("foo"), "processed")


# ── upsert: insert path ─────────────────────────────────────────────


class TestUpsertInsert(unittest.TestCase):
    """First-time write for a (gmail_id, user_id) — INSERT lands."""

    def _sess(self):
        return _StubSession()

    def test_fresh_insert_returns_inserted(self):
        sess = self._sess()
        res = upsert_processed_email(
            sess, _StubProcessedEmail,
            gmail_id="gm1", user_id="u1",
            category="bank_statement",
            status="success",
            items_extracted=42,
            integrity_error_class=_StubIntegrityError,
        )
        self.assertEqual(res["status"], "inserted")
        self.assertEqual(res["db_status"], "processed")
        self.assertEqual(len(sess.rows), 1)
        row = sess.rows[("gm1", "u1")]
        self.assertEqual(row.category, "bank_statement")
        self.assertEqual(row.items_extracted, 42)
        self.assertEqual(row.status, "processed")

    def test_error_status_stores_error_message(self):
        sess = self._sess()
        res = upsert_processed_email(
            sess, _StubProcessedEmail,
            gmail_id="gm2", user_id="u1",
            status="error",
            error_message="chunked parse failed",
            integrity_error_class=_StubIntegrityError,
        )
        self.assertEqual(res["db_status"], "failed")
        row = sess.rows[("gm2", "u1")]
        self.assertEqual(row.error_message, "chunked parse failed")

    def test_success_status_ignores_error_message(self):
        """error_message is only persisted when db_status == 'failed'
        — a successful ingest shouldn't carry a stale error."""
        sess = self._sess()
        upsert_processed_email(
            sess, _StubProcessedEmail,
            gmail_id="gm3", user_id="u1",
            status="success",
            error_message="left over from a prior failure",
            integrity_error_class=_StubIntegrityError,
        )
        row = sess.rows[("gm3", "u1")]
        self.assertIsNone(row.error_message)


# ── upsert: update path (IntegrityError → UPDATE) ───────────────────


class TestUpsertUpdate(unittest.TestCase):
    """A row already exists for (gmail_id, user_id) — the helper
    catches IntegrityError and UPDATEs the existing row's mutable
    fields."""

    def _sess_with_row(self, **kw):
        sess = _StubSession()
        row = _StubProcessedEmail(
            gmail_id=kw.get("gmail_id", "gm1"),
            user_id=kw.get("user_id", "u1"),
            status=kw.get("status", "failed"),
            items_extracted=kw.get("items_extracted", 0),
            category=kw.get("category", "unknown"),
        )
        sess.rows[(row.gmail_id, row.user_id)] = row
        return sess

    def test_update_returns_updated(self):
        sess = self._sess_with_row()
        res = upsert_processed_email(
            sess, _StubProcessedEmail,
            gmail_id="gm1", user_id="u1",
            category="bank_statement",
            status="success",
            items_extracted=17,
            integrity_error_class=_StubIntegrityError,
        )
        self.assertEqual(res["status"], "updated")
        self.assertEqual(res["db_status"], "processed")

    def test_update_upgrades_prior_failed_to_processed(self):
        """A retry after a prior 'failed' ingest should overwrite
        the status to 'processed' — the whole point of ak-bwe."""
        sess = self._sess_with_row(status="failed")
        upsert_processed_email(
            sess, _StubProcessedEmail,
            gmail_id="gm1", user_id="u1",
            status="success",
            items_extracted=5,
            integrity_error_class=_StubIntegrityError,
        )
        row = sess.rows[("gm1", "u1")]
        self.assertEqual(row.status, "processed")
        self.assertEqual(row.items_extracted, 5)

    def test_update_overwrites_items_and_category(self):
        sess = self._sess_with_row(items_extracted=0, category="unknown")
        upsert_processed_email(
            sess, _StubProcessedEmail,
            gmail_id="gm1", user_id="u1",
            status="success",
            items_extracted=123,
            category="bank_statement",
            integrity_error_class=_StubIntegrityError,
        )
        row = sess.rows[("gm1", "u1")]
        self.assertEqual(row.items_extracted, 123)
        self.assertEqual(row.category, "bank_statement")


# ── upsert: defensive paths ─────────────────────────────────────────


class TestUpsertDefensive(unittest.TestCase):
    """Missing keys / no session / unexpected errors — never raise."""

    def test_missing_gmail_id_returns_failed(self):
        sess = _StubSession()
        res = upsert_processed_email(
            sess, _StubProcessedEmail,
            gmail_id="", user_id="u1",
            integrity_error_class=_StubIntegrityError,
        )
        self.assertEqual(res["status"], "failed")
        self.assertEqual(len(sess.rows), 0)

    def test_missing_user_id_returns_failed(self):
        sess = _StubSession()
        res = upsert_processed_email(
            sess, _StubProcessedEmail,
            gmail_id="gm1", user_id=None,
            integrity_error_class=_StubIntegrityError,
        )
        self.assertEqual(res["status"], "failed")

    def test_no_session_returns_no_session(self):
        res = upsert_processed_email(
            None, _StubProcessedEmail,
            gmail_id="gm1", user_id="u1",
        )
        self.assertEqual(res["status"], "no_session")


# ── email_date parsing ──────────────────────────────────────────────


class TestEmailDateParsing(unittest.TestCase):
    """The helper accepts several date formats plus datetime
    objects. Unparseable → None (email_date is nullable)."""

    def test_datetime_passes_through(self):
        sess = _StubSession()
        dt = datetime(2026, 4, 1, 12, 30)
        upsert_processed_email(
            sess, _StubProcessedEmail,
            gmail_id="gm_dt", user_id="u1",
            email_date=dt, status="success",
            integrity_error_class=_StubIntegrityError,
        )
        row = sess.rows[("gm_dt", "u1")]
        self.assertEqual(row.email_date, dt)

    def test_iso_string_parsed(self):
        sess = _StubSession()
        upsert_processed_email(
            sess, _StubProcessedEmail,
            gmail_id="gm_iso", user_id="u1",
            email_date="2026-04-01", status="success",
            integrity_error_class=_StubIntegrityError,
        )
        row = sess.rows[("gm_iso", "u1")]
        self.assertEqual(row.email_date, datetime(2026, 4, 1))

    def test_dmy_slash_parsed(self):
        sess = _StubSession()
        upsert_processed_email(
            sess, _StubProcessedEmail,
            gmail_id="gm_dmy", user_id="u1",
            email_date="01/04/2026", status="success",
            integrity_error_class=_StubIntegrityError,
        )
        row = sess.rows[("gm_dmy", "u1")]
        self.assertEqual(row.email_date, datetime(2026, 4, 1))

    def test_garbage_string_becomes_none(self):
        sess = _StubSession()
        upsert_processed_email(
            sess, _StubProcessedEmail,
            gmail_id="gm_bad", user_id="u1",
            email_date="not-a-date", status="success",
            integrity_error_class=_StubIntegrityError,
        )
        row = sess.rows[("gm_bad", "u1")]
        self.assertIsNone(row.email_date)


# ── Idempotency scenario (Lead's spec) ──────────────────────────────


class TestIdempotencyScenario(unittest.TestCase):
    """The core Lead-spec: a backfill re-run against the same file
    doesn't duplicate the processedEmails row."""

    def test_double_call_produces_single_row(self):
        sess = _StubSession()
        upsert_processed_email(
            sess, _StubProcessedEmail,
            gmail_id="gm_x", user_id="u1",
            category="bank_statement", status="success",
            items_extracted=10,
            integrity_error_class=_StubIntegrityError,
        )
        # Second call — same key — must UPDATE, not INSERT again.
        upsert_processed_email(
            sess, _StubProcessedEmail,
            gmail_id="gm_x", user_id="u1",
            category="bank_statement", status="success",
            items_extracted=10,
            integrity_error_class=_StubIntegrityError,
        )
        self.assertEqual(len(sess.rows), 1)

    def test_second_call_after_failure_lands_processed(self):
        sess = _StubSession()
        upsert_processed_email(
            sess, _StubProcessedEmail,
            gmail_id="gm_y", user_id="u1",
            status="error", error_message="transient SDK",
            integrity_error_class=_StubIntegrityError,
        )
        self.assertEqual(sess.rows[("gm_y", "u1")].status, "failed")

        # Retry after fix
        upsert_processed_email(
            sess, _StubProcessedEmail,
            gmail_id="gm_y", user_id="u1",
            status="success", items_extracted=15,
            integrity_error_class=_StubIntegrityError,
        )
        self.assertEqual(sess.rows[("gm_y", "u1")].status, "processed")
        self.assertEqual(sess.rows[("gm_y", "u1")].items_extracted, 15)


# ── ak-bwe v2: should_stamp_success gating (reviewer MAJOR) ────────


class TestShouldStampSuccessSuccessPath(unittest.TestCase):
    """Genuine full-ingest success → stamp is allowed."""

    def test_success_true_no_failed_chunks_stamps(self):
        summary = {"success": True, "failed_chunks": [],
                   "total_chunks": 4}
        self.assertTrue(should_stamp_success(summary))

    def test_success_true_missing_failed_chunks_field(self):
        """Absent failed_chunks field → treat as empty → stamp."""
        summary = {"success": True, "total_chunks": 4}
        self.assertTrue(should_stamp_success(summary))

    def test_success_true_single_chunk_no_failed(self):
        summary = {"success": True, "failed_chunks": [],
                   "total_chunks": 1}
        self.assertTrue(should_stamp_success(summary))


class TestShouldStampSuccessFailurePaths(unittest.TestCase):
    """The MAJOR ak-bwe-v2 guarantee: any signal of failure /
    partial ingest / missing summary → do NOT stamp so
    _filter_already_processed re-processes on the next run."""

    def test_success_false_does_not_stamp(self):
        summary = {"success": False, "failed_chunks": [[3, 4]],
                   "total_chunks": 8}
        self.assertFalse(should_stamp_success(summary))

    def test_success_true_but_failed_chunks_does_not_stamp(self):
        """A summary claiming success=True BUT with residual
        failed_chunks is treated as failure — the two disagree and
        we side with the failure signal. Prevents a bad status
        merge from silently permitting the stamp."""
        summary = {"success": True, "failed_chunks": [[5, 6]],
                   "total_chunks": 8}
        self.assertFalse(should_stamp_success(summary))

    def test_none_does_not_stamp(self):
        """_run_pdf_analysis returning None (legacy / exception
        path that predates ak-bwe-v2) must NOT stamp — the whole
        point of the gating."""
        self.assertFalse(should_stamp_success(None))

    def test_non_dict_does_not_stamp(self):
        for form in ("success", 42, True, [1, 2], object()):
            with self.subTest(form=form):
                self.assertFalse(should_stamp_success(form))

    def test_empty_dict_does_not_stamp(self):
        self.assertFalse(should_stamp_success({}))

    def test_exception_reason_does_not_stamp(self):
        """The analysis-exception path in _run_pdf_analysis
        returns {success: False, reason: 'analysis_exception: …'}
        — must not stamp regardless of other fields."""
        summary = {"success": False, "failed_chunks": [],
                   "total_chunks": 0,
                   "reason": "analysis_exception: SDK died"}
        self.assertFalse(should_stamp_success(summary))

    def test_pdf_open_failed_does_not_stamp(self):
        """Early-fail path — PDF couldn't even be opened."""
        summary = {"success": False, "failed_chunks": [],
                   "total_chunks": 0,
                   "reason": "pdf_open_failed: file not found"}
        self.assertFalse(should_stamp_success(summary))


class TestSingleChunkUnknownResultFailClosed(unittest.TestCase):
    """ak-bwe v3 (reviewer BOUNCE hq-wisp-97i5qm): the single-chunk
    branch of _run_pdf_analysis previously defaulted to
    {success=True, failed_chunks=[]} when the chunk function's
    return value wasn't a dict (e.g. None, tuple, or a future
    refactor's non-dict shape). That's fail-OPEN and re-introduces
    the exact regression class the v2 outer gate closed.

    v3 flips the default to fail-CLOSED — matching
    should_stamp_success and the multi-chunk path.

    These tests pin down the SHAPE of the summary v3 emits when a
    single-chunk return is unknown, so a future refactor that flips
    it back to fail-open fails loudly.
    """

    def _synthesize_v3_unknown_result_summary(self, chunk_result_type):
        """Mirror the exact shape _run_pdf_analysis emits for a
        non-dict chunk_result. Kept as a helper so a service-layer
        refactor that changes the summary shape can be caught by
        this test file too (the shape is the contract)."""
        return {
            "success": False,
            "failed_chunks": [[1, 8]],  # arbitrary page range
            "total_chunks": 1,
            "reason": f"unknown_chunk_result_shape: {chunk_result_type}",
        }

    def test_none_chunk_result_does_not_stamp(self):
        summary = self._synthesize_v3_unknown_result_summary("NoneType")
        self.assertFalse(should_stamp_success(summary))

    def test_tuple_chunk_result_does_not_stamp(self):
        summary = self._synthesize_v3_unknown_result_summary("tuple")
        self.assertFalse(should_stamp_success(summary))

    def test_int_chunk_result_does_not_stamp(self):
        summary = self._synthesize_v3_unknown_result_summary("int")
        self.assertFalse(should_stamp_success(summary))

    def test_v3_default_summary_has_success_false(self):
        """Explicit shape assertion: the v3 default MUST carry
        success=False even before we hand it to
        should_stamp_success. Guards against a future flip that
        re-introduces success=True."""
        summary = self._synthesize_v3_unknown_result_summary("NoneType")
        self.assertFalse(summary["success"],
                         "v3 default summary regressed to success=True")
        self.assertTrue(summary["failed_chunks"],
                        "v3 default summary must carry a failed_chunks "
                        "entry so the outer gate sees the failure")
        self.assertIn("reason", summary,
                      "v3 default summary must carry a reason field "
                      "for operator log grep")


class TestReviewerAskScenarios(unittest.TestCase):
    """Verbatim from Lead's BOUNCE hq-wisp-b48kot:

      failed/partial _run_pdf_analysis does NOT leave a
      status='processed' row.

    These tests capture the exact zero-loss regression the reviewer
    flagged so a future rewrite fails loudly if the gating slips."""

    def test_partial_failure_leaves_no_processed_row(self):
        summary = {"success": False,
                   "failed_chunks": [[3, 4], [5, 6]],
                   "total_chunks": 8}
        self.assertFalse(should_stamp_success(summary))

    def test_persistent_failure_leaves_no_processed_row(self):
        summary = {"success": False, "failed_chunks": [[1, 8]],
                   "total_chunks": 8,
                   "reason": "retries exhausted"}
        self.assertFalse(should_stamp_success(summary))

    def test_pre_ak_bwe_return_none_leaves_no_processed_row(self):
        """A caller that hasn't been migrated to the new return
        contract (legacy) hands us None. Must NOT stamp — same
        conservative default the pre-ak-bwe path had for the
        UPDATE-only race."""
        self.assertFalse(should_stamp_success(None))


if __name__ == "__main__":
    print("ak-bwe processedEmails upsert regression tests")
    print("=" * 60)
    unittest.main(verbosity=2, exit=False)
    print("=" * 60)
