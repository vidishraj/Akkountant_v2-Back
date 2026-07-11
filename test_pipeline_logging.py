"""ak-n44 regression tests — structured pipeline observability.

Pure-Python coverage of the payload builder + emitters. Service-
layer integration (mailProcessorService's ak-wty retry loop,
_run_all_chunks_async's failed-chunks aggregation, ak-ifc v3's
reconciliation-divergent path) exercises the same helpers — env-
limited integration coverage stays deferred at lead-verify.

Locked contract (single-line JSON payloads):

  Mandatory:
    event, timestamp

  Optional (populated when the caller supplies them):
    file_id, gmail_id, user_id,
    chunk_index, chunks_total, page_range,
    error_type, message, traceback,
    plus any `extra` fields spread at TOP level.

  Behaviors:
    - error_type auto-classified from message or exception when
      caller omits it (single source of truth: ak-1rh's
      _classify_reason).
    - message auto-populated from str(exception) when caller
      omits it.
    - traceback populated when caller passes exception=<exc>.
    - extra keys can't overwrite the mandatory event/timestamp.
    - json.dumps failure never crashes the emitter — falls back
      to a hardcoded skeleton.
"""

import io
import json
import logging
import os
import sys
import unittest

sys.path.insert(0, os.path.dirname(os.path.abspath(__file__)))

from utils.pipeline_logging import (
    EVENT_CHUNK_ERROR,
    EVENT_CHUNK_RETRY,
    EVENT_RECONCILIATION_DIVERGENT,
    build_pipeline_event,
    emit_chunk_error,
    emit_chunk_retry,
    emit_pipeline_event,
    emit_reconciliation_divergent,
)


# ── Test helpers ────────────────────────────────────────────────────


def _capture_logger():
    """Attach an in-memory StringIO handler to a fresh logger and
    return (logger, buffer)."""
    buf = io.StringIO()
    logger = logging.getLogger(f"ak_n44_test_{id(buf)}")
    logger.handlers.clear()
    logger.setLevel(logging.DEBUG)
    handler = logging.StreamHandler(buf)
    handler.setLevel(logging.DEBUG)
    handler.setFormatter(logging.Formatter("%(message)s"))
    logger.addHandler(handler)
    logger.propagate = False
    return logger, buf


def _last_line_as_json(buf):
    lines = [ln for ln in buf.getvalue().splitlines() if ln.strip()]
    assert lines, "no lines captured"
    return json.loads(lines[-1])


# ── build_pipeline_event: mandatory shape ───────────────────────────


class TestPayloadMandatory(unittest.TestCase):
    def test_event_and_timestamp_always_present(self):
        payload = build_pipeline_event("foo")
        self.assertEqual(payload["event"], "foo")
        self.assertIn("timestamp", payload)
        self.assertRegex(
            payload["timestamp"],
            r"^\d{4}-\d{2}-\d{2}T\d{2}:\d{2}:\d{2}\.\d+Z$",
            "timestamp not ISO-8601 UTC with trailing Z",
        )

    def test_event_stamped_verbatim(self):
        """The event field is the grep key — no case normalization."""
        for form in ("chunk_error", "CHUNK_ERROR", "Chunk_Error"):
            with self.subTest(form=form):
                payload = build_pipeline_event(form)
                self.assertEqual(payload["event"], form)


class TestPayloadOptional(unittest.TestCase):
    def test_optional_fields_omitted_when_none(self):
        payload = build_pipeline_event("chunk_error")
        for k in ("file_id", "gmail_id", "user_id",
                  "chunk_index", "chunks_total", "page_range",
                  "traceback"):
            self.assertNotIn(k, payload)

    def test_optional_fields_included_when_set(self):
        payload = build_pipeline_event(
            "chunk_error",
            file_id="mail_pipeline_u1_HDFC_DEBIT_2026_04",
            gmail_id="gm1", user_id="u1",
            chunk_index=3, chunks_total=8, page_range=[7, 8],
        )
        self.assertEqual(
            payload["file_id"], "mail_pipeline_u1_HDFC_DEBIT_2026_04"
        )
        self.assertEqual(payload["gmail_id"], "gm1")
        self.assertEqual(payload["user_id"], "u1")
        self.assertEqual(payload["chunk_index"], 3)
        self.assertEqual(payload["chunks_total"], 8)
        self.assertEqual(payload["page_range"], [7, 8])


# ── error_type auto-classification ──────────────────────────────────


class TestErrorTypeClassification(unittest.TestCase):
    """error_type is populated from the caller's supplied label OR
    auto-classified via ak-1rh's _classify_reason. The two paths
    share the SAME label taxonomy so reprocess_pdf.summary and
    every chunk-error log line agree."""

    def test_explicit_label_wins(self):
        payload = build_pipeline_event(
            "chunk_error",
            error_type="pdf_password",
            message="unrelated string",
        )
        self.assertEqual(payload["error_type"], "pdf_password")

    def test_auto_from_message_mysql_1040(self):
        payload = build_pipeline_event(
            "chunk_error",
            message="ERROR 1040 too many connections",
        )
        self.assertEqual(payload["error_type"], "mysql_1040")

    def test_auto_from_message_sdk_reader(self):
        payload = build_pipeline_event(
            "chunk_error",
            message="Fatal error in message reader",
        )
        self.assertEqual(payload["error_type"], "sdk_message_reader")

    def test_auto_from_exception(self):
        try:
            raise RuntimeError("SDK stream reset by peer")
        except RuntimeError as e:
            payload = build_pipeline_event(
                "chunk_error", exception=e,
            )
        self.assertEqual(payload["error_type"], "stream")

    def test_unknown_falls_back_to_unknown(self):
        payload = build_pipeline_event(
            "chunk_error", message="a novel problem",
        )
        self.assertEqual(payload["error_type"], "unknown")


class TestExceptionHandling(unittest.TestCase):
    def test_exception_populates_message_and_traceback(self):
        try:
            raise ValueError("boom")
        except ValueError as e:
            payload = build_pipeline_event(
                "chunk_error", exception=e,
            )
        self.assertIn("boom", payload["message"])
        self.assertIn("traceback", payload)
        # Traceback should mention ValueError somewhere.
        self.assertIn("ValueError", payload["traceback"])

    def test_no_exception_no_traceback_field(self):
        payload = build_pipeline_event(
            "chunk_error", message="explicit message",
        )
        self.assertNotIn("traceback", payload)
        self.assertEqual(payload["message"], "explicit message")

    def test_explicit_message_wins_over_exception_message(self):
        """When both message and exception are supplied, the
        caller's explicit message takes priority (they know their
        context better than str(exc))."""
        try:
            raise ValueError("cryptic")
        except ValueError as e:
            payload = build_pipeline_event(
                "chunk_error", exception=e,
                message="human-friendly wrapper",
            )
        self.assertEqual(payload["message"], "human-friendly wrapper")
        # Traceback still included.
        self.assertIn("traceback", payload)


# ── extra fields ────────────────────────────────────────────────────


class TestExtraFields(unittest.TestCase):
    def test_extra_spread_at_top_level(self):
        payload = build_pipeline_event(
            "chunk_error",
            extra={"attempts_used": 3, "processing_mode": "text"},
        )
        self.assertEqual(payload["attempts_used"], 3)
        self.assertEqual(payload["processing_mode"], "text")

    def test_extra_cannot_overwrite_mandatory(self):
        """event and timestamp are grep keys — extra must never
        clobber them."""
        original_event = "chunk_error"
        payload = build_pipeline_event(
            original_event,
            extra={"event": "hijacked", "timestamp": "1970-01-01T00:00:00Z"},
        )
        self.assertEqual(payload["event"], original_event)
        self.assertNotEqual(payload["timestamp"], "1970-01-01T00:00:00Z")


# ── Emitter shape (JSON on the wire) ────────────────────────────────


class TestEmitPipelineEvent(unittest.TestCase):
    def test_writes_single_line_json(self):
        logger, buf = _capture_logger()
        emit_pipeline_event(
            logger, "chunk_error",
            file_id="f1", chunk_index=0,
            message="MySQL 1040 error",
        )
        raw = buf.getvalue()
        # Exactly one line (no embedded newlines in the JSON).
        self.assertEqual(len(raw.splitlines()), 1)
        got = json.loads(raw.splitlines()[0])
        self.assertEqual(got["event"], "chunk_error")
        self.assertEqual(got["file_id"], "f1")
        self.assertEqual(got["error_type"], "mysql_1040")

    def test_default_level_is_info(self):
        logger, buf = _capture_logger()
        # Set to WARNING so INFO would be dropped.
        logger.setLevel(logging.WARNING)
        emit_pipeline_event(logger, "chunk_ok", file_id="f1")
        # Nothing should land since INFO < WARNING.
        self.assertEqual(buf.getvalue().strip(), "")

    def test_error_level_lands_at_warning_threshold(self):
        logger, buf = _capture_logger()
        logger.setLevel(logging.WARNING)
        emit_chunk_error(logger, file_id="f1", message="boom")
        self.assertGreater(len(buf.getvalue().strip()), 0)


# ── emit_chunk_error ────────────────────────────────────────────────


class TestEmitChunkError(unittest.TestCase):
    def test_default_event_name(self):
        logger, buf = _capture_logger()
        emit_chunk_error(logger, file_id="f1")
        got = _last_line_as_json(buf)
        self.assertEqual(got["event"], EVENT_CHUNK_ERROR)

    def test_page_range_serialized_as_list(self):
        logger, buf = _capture_logger()
        emit_chunk_error(
            logger, file_id="f1", page_range=[3, 4],
            message="boom",
        )
        got = _last_line_as_json(buf)
        self.assertEqual(got["page_range"], [3, 4])

    def test_carries_ak_wty_retry_context(self):
        logger, buf = _capture_logger()
        emit_chunk_error(
            logger, file_id="f1", page_range=[1, 2],
            message="SDK Fatal error in message reader",
            extra={"attempts_used": 4, "retryable": True,
                   "processing_mode": "text"},
        )
        got = _last_line_as_json(buf)
        self.assertEqual(got["error_type"], "sdk_message_reader")
        self.assertEqual(got["attempts_used"], 4)
        self.assertTrue(got["retryable"])
        self.assertEqual(got["processing_mode"], "text")


# ── emit_chunk_retry ────────────────────────────────────────────────


class TestEmitChunkRetry(unittest.TestCase):
    def test_event_name(self):
        logger, buf = _capture_logger()
        emit_chunk_retry(
            logger, file_id="f1", page_range=[1, 2],
            attempt=2, max_attempts=4,
            message="rate_limit exceeded",
        )
        got = _last_line_as_json(buf)
        self.assertEqual(got["event"], EVENT_CHUNK_RETRY)
        self.assertEqual(got["attempt"], 2)
        self.assertEqual(got["max_attempts"], 4)
        self.assertEqual(got["error_type"], "rate_limit")


# ── emit_reconciliation_divergent ───────────────────────────────────


class TestEmitReconciliationDivergent(unittest.TestCase):
    def test_carries_totals(self):
        logger, buf = _capture_logger()
        emit_reconciliation_divergent(
            logger, file_id="f1", bank="HDFC_DEBIT",
            extracted_debits=500.0, extracted_credits=100.0,
            stated_debits=900.0, stated_credits=300.0,
            checked_fields=("total_debits", "total_credits"),
            reason="debits: extracted=500 vs stated=900",
            extra={"num_chunks": 8},
        )
        got = _last_line_as_json(buf)
        self.assertEqual(got["event"], EVENT_RECONCILIATION_DIVERGENT)
        self.assertEqual(got["file_id"], "f1")
        self.assertEqual(got["bank"], "HDFC_DEBIT")
        self.assertEqual(got["extracted_debits"], 500.0)
        self.assertEqual(got["stated_debits"], 900.0)
        self.assertEqual(got["num_chunks"], 8)


# ── Reviewer scenario ──────────────────────────────────────────────


class TestReviewerScenario(unittest.TestCase):
    """Verbatim from Lead's dispatch: 'a single grep can produce a
    per-file per-chunk error report'. Simulate the ak-32o run 1
    silent-failure pattern (8/8 chunks died with MySQL 1040 on
    file 4) and show that a grep on the JSON produces the whole
    ledger for that file."""

    def test_grep_produces_per_chunk_ledger(self):
        logger, buf = _capture_logger()
        # Simulate 8 chunk failures on the same file.
        for i, pair in enumerate([[1, 2], [3, 4], [5, 6], [7, 8],
                                   [9, 10], [11, 12], [13, 14], [15, 16]]):
            emit_chunk_error(
                logger,
                file_id="mail_pipeline_uxxx_HDFC_DEBIT_2026_01",
                chunk_index=i, chunks_total=8,
                page_range=pair,
                message="ERROR 1040 too many connections",
                extra={"attempts_used": 4, "retryable": True},
            )
        # Grep-like filter.
        lines = [
            ln for ln in buf.getvalue().splitlines()
            if '"event":"chunk_error"' in ln.replace(" ", "")
        ]
        # 8 chunks × 1 line each = 8.
        self.assertEqual(len(lines), 8)
        # Every one classifies as mysql_1040.
        for ln in lines:
            payload = json.loads(ln)
            self.assertEqual(payload["error_type"], "mysql_1040")
            self.assertEqual(
                payload["file_id"],
                "mail_pipeline_uxxx_HDFC_DEBIT_2026_01",
            )
        # Chunk indices cover the full range.
        indices = sorted(json.loads(ln)["chunk_index"] for ln in lines)
        self.assertEqual(indices, list(range(8)))


if __name__ == "__main__":
    print("ak-n44 structured pipeline observability tests")
    print("=" * 60)
    unittest.main(verbosity=2, exit=False)
    print("=" * 60)
