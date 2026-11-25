"""ak-b5sv (2026-11-24): surface the ak-tik degraded-dedup signal
(degraded_dedup_rows / dual_tier_tuples) from statement imports run via
AgentChat as an `ingest_notice` SSE event.

Before: insert_batch_transactions returned both counts (ak-qic6), but
every caller kept only inserted/duplicates, so neither process_emails'
summary (/readEmails/status, the ak-dngn banner) nor the agent stream
ever carried them.

After: add_landed sums them into a run-level ContextVar set by
process_emails; the summary always carries both ints; the agent tool
handler turns a scan tool's summary into an ingest_notice event.
Synthetic data only; no SDK / network.
"""
from __future__ import annotations

import ast
import json
import os
import sys
import unittest

HERE = os.path.dirname(os.path.abspath(__file__))
sys.path.insert(0, HERE)

from utils import statement_ingest_outcome as io  # noqa: E402
from utils.ingest_notice import EVENT_TYPE, INGEST_TOOLS, ingest_notice_from_result  # noqa: E402


def _read(rel):
    with open(os.path.join(HERE, rel)) as fh:
        return fh.read()


def _batch(inserted=3, duplicates=0, degraded=0, dual=0):
    """An accepted insert_batch_transactions result (ak-qic6 shape)."""
    return {"result": "success", "total": inserted + duplicates, "inserted": inserted,
            "duplicates": duplicates, "file_id": "f", "degraded_dedup_rows": degraded,
            "dual_tier_tuples": dual}


class TestRunSignalAccumulation(unittest.TestCase):

    def run_with(self, results):
        signal = io.new_dedup_signal()
        token = io.DEDUP_SIGNAL.set(signal)
        try:
            # Two emails, each with its own LANDED_ROWS, like
            # _process_single_pdf_email; chunks call add_landed.
            landed_totals = []
            for email_results in results:
                landed = {"rows": 0, "recorded": 0}
                lt = io.LANDED_ROWS.set(landed)
                try:
                    for r in email_results:
                        io.add_landed(r)
                finally:
                    io.LANDED_ROWS.reset(lt)
                landed_totals.append(landed["rows"])
        finally:
            io.DEDUP_SIGNAL.reset(token)
        return signal, landed_totals

    def test_sums_across_chunks_and_emails(self):
        signal, landed = self.run_with([
            [_batch(10, 2, degraded=3), _batch(5, 0, degraded=2, dual=1)],
            [_batch(4, 1, degraded=0, dual=2)],
        ])
        self.assertEqual(signal, {"degraded_dedup_rows": 5, "dual_tier_tuples": 3})
        self.assertEqual(landed, [17, 5])  # landed-row accounting unchanged

    def test_bad_values_count_as_zero(self):
        signal, _ = self.run_with([[
            {"inserted": 1, "degraded_dedup_rows": "7", "dual_tier_tuples": -2},
            {"inserted": 1, "degraded_dedup_rows": True, "dual_tier_tuples": 1.5},
            {"inserted": 1},
            "not a dict", None,
        ]])
        self.assertEqual(signal, {"degraded_dedup_rows": 0, "dual_tier_tuples": 0})

    def test_no_run_signal_is_a_noop(self):
        landed = {"rows": 0, "recorded": 0}
        lt = io.LANDED_ROWS.set(landed)
        try:
            io.add_landed(_batch(2, 1, degraded=4))
        finally:
            io.LANDED_ROWS.reset(lt)
        self.assertEqual(landed["rows"], 3)
        self.assertIsNone(io.DEDUP_SIGNAL.get())

    def test_signal_counted_even_without_email_counter(self):
        # Text-email inserts have no per-email LANDED_ROWS; still counted.
        signal = io.new_dedup_signal()
        token = io.DEDUP_SIGNAL.set(signal)
        try:
            io.add_landed(_batch(degraded=2))
        finally:
            io.DEDUP_SIGNAL.reset(token)
        self.assertEqual(signal["degraded_dedup_rows"], 2)


class TestIngestNotice(unittest.TestCase):

    def test_payload_for_scan_tools(self):
        for tool in INGEST_TOOLS:
            self.assertEqual(
                ingest_notice_from_result(tool, {"degraded_dedup_rows": 5, "dual_tier_tuples": 0,
                                                 "errors": [], "pdf_emails_processed": 2}),
                {"tool": tool, "degraded_dedup_rows": 5, "dual_tier_tuples": 0},
            )
        self.assertEqual(
            ingest_notice_from_result("scan_statements", {"dual_tier_tuples": 2}),
            {"tool": "scan_statements", "degraded_dedup_rows": 0, "dual_tier_tuples": 2},
        )

    def test_nothing_to_report(self):
        self.assertIsNone(ingest_notice_from_result("scan_statements",
                                                    {"degraded_dedup_rows": 0, "dual_tier_tuples": 0}))
        self.assertIsNone(ingest_notice_from_result("scan_statements", {"errors": []}))
        self.assertIsNone(ingest_notice_from_result("scan_statements", "text"))
        self.assertIsNone(ingest_notice_from_result("scan_statements",
                                                    {"degraded_dedup_rows": "5", "dual_tier_tuples": -1}))
        # Not an ingest tool, even with counts.
        self.assertIsNone(ingest_notice_from_result("fetch_transactions", {"degraded_dedup_rows": 5}))

    def test_tool_set_matches_executor(self):
        src = _read("services/agent_tool_executor.py")
        for tool in INGEST_TOOLS:
            i = src.index(f'tool_name == "{tool}"')
            self.assertIn("mail_processor.process_emails(", src[i:i + 900], tool)


def _lift_sse_event():
    src = _read("services/agentService.py")
    tree = ast.parse(src)
    cls = next(n for n in tree.body if isinstance(n, ast.ClassDef) and n.name == "AgentService")
    fn = next(n for n in cls.body if isinstance(n, ast.FunctionDef) and n.name == "_sse_event")
    fn.decorator_list = []
    mod = ast.Module(body=[fn], type_ignores=[])
    ast.fix_missing_locations(mod)
    ns = {"json": json}
    exec(compile(mod, "<lift:_sse_event>", "exec"), ns)
    return ns["_sse_event"]


class TestSseFixture(unittest.TestCase):
    """Bead fixture: a statement import via AgentChat with
    degraded_dedup_rows=5 produces an SSE frame carrying 5."""

    def test_frame(self):
        sse = _lift_sse_event()
        summary = {"total_emails_fetched": 3, "financial_emails": 2, "text_emails_processed": 0,
                   "pdf_emails_processed": 2, "errors": [],
                   "degraded_dedup_rows": 5, "dual_tier_tuples": 1}
        frame = sse(EVENT_TYPE, ingest_notice_from_result("scan_statements", summary))
        self.assertTrue(frame.startswith("data: ") and frame.endswith("\n\n"))
        self.assertEqual(json.loads(frame[len("data: "):]), {
            "type": "ingest_notice", "tool": "scan_statements",
            "degraded_dedup_rows": 5, "dual_tier_tuples": 1,
        })


class TestWiring(unittest.TestCase):

    def setUp(self):
        self.mail = _read("services/mailProcessorService.py")
        self.agent = _read("services/agentService.py")

    def _process_emails(self):
        i = self.mail.index("    def process_emails(")
        return self.mail[i:self.mail.index("\n    def ", i + 10)]

    def test_process_emails_summary_carries_counts(self):
        fn = self._process_emails()
        init = fn[fn.index("summary = {"):fn.index("}", fn.index("summary = {"))]
        self.assertIn('"degraded_dedup_rows": 0', init)
        self.assertIn('"dual_tier_tuples": 0', init)
        set_i = fn.index("ingest_outcome.DEDUP_SIGNAL.set(dedup_signal)")
        try_i = fn.index("        try:\n", set_i)
        self.assertLess(set_i, try_i)
        fin = fn[fn.index("        finally:\n", try_i):]
        self.assertIn("ingest_outcome.DEDUP_SIGNAL.reset(dedup_signal_token)", fin)
        self.assertIn("summary.update(dedup_signal)", fin)

    def test_both_insert_sites_feed_add_landed(self):
        self.assertGreaterEqual(self.mail.count("ingest_outcome.add_landed(result)"), 2)

    def test_handler_emits_notice_after_tool(self):
        i = self.agent.index("result = execute_tool(")
        body = self.agent[i:i + 4000]
        self.assertIn("ingest_notice_from_result(_tn, result)", body)
        self.assertIn("tool_events.append((INGEST_NOTICE_EVENT, _notice))", body)

    def test_error_path_still_reports_notice(self):
        i = self.agent.index("if error_msg:")
        block = self.agent[i:i + 600]
        self.assertIn('event_type in ("outcome", INGEST_NOTICE_EVENT)', block)


if __name__ == "__main__":  # pragma: no cover
    unittest.main(verbosity=2)
