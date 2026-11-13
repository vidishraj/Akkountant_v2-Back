"""ak-ucta (2026-11-13): unit tests for the stale-error-banner fix in
``WealthDigestService._latest_job_error``.

Observed bug: the ``/wealth-digest`` page rendered two mutually-
inconsistent banners simultaneously — "Last successful digest:
2026-11-12" alongside "Latest digest task failed at Sun, Aug 30".
Root cause: the pre-ak-ucta ``_latest_job_error`` only checked the
LATEST Job row's status without cross-referencing against the latest
successful digest's timestamp. In the ak-iqum-recovery edge case
(``failures==10`` on the pre-Opus Sonnet outage left a Failed row
as the latest, and the Nov 12 bootstrap re-seed wrote a successful
AgentMessage without clearing the stale Jobs-table record), the
Aug 30 banner persisted indefinitely.

Fix: ``_latest_job_error`` accepts ``success_ts`` kwarg; when passed,
returns None if the Failed Job's ``due_date`` is older than
``success_ts``. The read-side caller (``_shape_response``) passes the
AgentMessage.ts of the digest being served.

What this file covers (no Flask, no DB — AST-lifts the method into a
mockable namespace):

  1. Pre-success scenario (no newer success): Failed row IS returned
     as the banner (regression guard on the active-error case).
  2. Post-success scenario (Nov-12-after-Aug-30): Failed row is
     SUPPRESSED when success_ts > failed_due_date.
  3. Edge: success_ts exactly equal to failed_due_date → still
     return the error (strict > comparison; equal means the Failed
     row's cycle overlaps with the success, so we surface it).
  4. Edge: success_ts is None (legacy caller / pre-ak-ucta
     compat) → old behavior preserved; Failed row returned.
  5. Edge: Failed row's due_date is None → still surface it (can't
     cross-reference without a timestamp; fail-open to safer side).
  6. Non-Failed latest row → return None (unchanged).
  7. No Job rows at all → return None (unchanged).
  8. ``_shape_response`` wires ``success_ts=msg.ts`` (source-text
     drift guard so a future refactor that drops the kwarg surfaces
     this regression).
"""
import ast
import os
import sys
import types
import unittest
from datetime import datetime, timedelta
from unittest.mock import MagicMock


_HERE = os.path.dirname(os.path.abspath(__file__))


def _ast_lift_latest_job_error():
    """Extract ``WealthDigestService._latest_job_error`` as a
    free-standing callable so tests exercise the real body without
    dragging the google.oauth2 / firebase / dolt chain.

    Same AST-lift idiom as ``test_wealth_digest_migration_gap``.
    """
    src_path = os.path.join(_HERE, "services", "wealthDigestService.py")
    with open(src_path) as fh:
        src = fh.read()
    tree = ast.parse(src)
    for node in tree.body:
        if isinstance(node, ast.ClassDef) and node.name == "WealthDigestService":
            for item in node.body:
                if isinstance(item, ast.FunctionDef) and item.name == "_latest_job_error":
                    # Strip decorators so exec gets a plain callable.
                    item.decorator_list = []
                    # Replace the type annotation `datetime | None` with
                    # a string so Python 3.9 doesn't trip on PEP 604 at
                    # module-body exec time (the original file uses
                    # `from __future__ import annotations` at module scope;
                    # the exec'd fragment doesn't inherit that).
                    for arg in item.args.args:
                        arg.annotation = None
                    if item.returns is not None:
                        item.returns = None
                    ns = {
                        # Provide a Job placeholder with .title + .user_id
                        # attributes so the query filter builds cleanly.
                        "Job": types.SimpleNamespace(
                            title=MagicMock(),
                            user_id=MagicMock(),
                            id=MagicMock(),
                        ),
                        # The literal the function compares against.
                        "WEALTH_DIGEST_JOB_TITLE": "WealthDigestTask",
                        "datetime": datetime,
                    }
                    exec(
                        compile(ast.Module([item], []), "<extract>", "exec"),
                        ns,
                    )
                    return ns["_latest_job_error"]
    raise RuntimeError("_latest_job_error not found")


def _make_service_with_latest_job(latest_job):
    """Build a service stub whose ``db.session.query(Job).filter(...)
    .order_by(...).first()`` returns ``latest_job``. Bind the extracted
    ``_latest_job_error`` method to this stub so ``self`` resolves."""
    svc = types.SimpleNamespace()

    class _Chain:
        def filter(self, *a, **kw): return self
        def order_by(self, *a, **kw): return self
        def first(self): return latest_job

    svc.db = types.SimpleNamespace(
        session=types.SimpleNamespace(query=lambda _m: _Chain()),
    )
    _lje = _ast_lift_latest_job_error()
    svc._latest_job_error = types.MethodType(_lje, svc)
    return svc


def _failed_job(due_date, result="SDK terminal error", status="Failed"):
    return types.SimpleNamespace(
        status=status, due_date=due_date, result=result,
    )


class TestLatestJobErrorStaleBannerFix(unittest.TestCase):

    # ── 1-5: Failed-row + success_ts cross-reference ──────────────────

    def test_returns_error_when_no_success_passed(self):
        """Legacy caller (pre-ak-ucta, no success_ts kwarg) must still
        get the Failed row — backwards compat for any out-of-band
        caller that hasn't adopted the new kwarg."""
        failed_at = datetime(2026, 8, 30, 21, 44)
        svc = _make_service_with_latest_job(_failed_job(failed_at))
        result = svc._latest_job_error("user1")
        self.assertIsNotNone(result)
        self.assertEqual(result["message"], "SDK terminal error")

    def test_returns_error_when_success_is_older(self):
        """Classic active-error case: a Failed row that's newer than
        the last successful digest. Banner SHOULD fire. Regression
        guard against overzealous suppression."""
        failed_at = datetime(2026, 11, 13, 6, 15)
        success_ts = datetime(2026, 11, 12, 8, 32)
        svc = _make_service_with_latest_job(_failed_job(failed_at))
        result = svc._latest_job_error("user1", success_ts=success_ts)
        self.assertIsNotNone(result)
        self.assertIn("2026-11-13", result["at"])

    def test_suppresses_error_when_success_is_newer(self):
        """ak-ucta bug case: historical Aug 30 Failed + Nov 12 success.
        Banner must NOT fire — the error is stale per the newer
        successful digest."""
        failed_at = datetime(2026, 8, 30, 21, 44)
        success_ts = datetime(2026, 11, 12, 8, 32)
        svc = _make_service_with_latest_job(_failed_job(failed_at))
        result = svc._latest_job_error("user1", success_ts=success_ts)
        self.assertIsNone(result)

    def test_returns_error_on_exact_timestamp_tie(self):
        """Strict > comparison — equal timestamps surface the error.
        Rationale: a Failed Job scheduled at the same instant as a
        success is semantically the same cycle; surface the error
        so operators see the mixed signal."""
        ts = datetime(2026, 11, 12, 8, 32)
        svc = _make_service_with_latest_job(_failed_job(ts))
        result = svc._latest_job_error("user1", success_ts=ts)
        self.assertIsNotNone(result)

    def test_returns_error_when_failed_due_date_is_null(self):
        """Defensive: Job.due_date is nullable in principle. Without
        a Failed-side timestamp we can't cross-reference → fall back
        to the pre-ak-ucta behavior (return the error) so banner
        regressions fail-open to visible-to-user rather than silently
        suppressed."""
        svc = _make_service_with_latest_job(_failed_job(None))
        result = svc._latest_job_error(
            "user1", success_ts=datetime(2026, 11, 12),
        )
        self.assertIsNotNone(result)

    # ── 6-7: Non-Failed / no-row paths unchanged ──────────────────────

    def test_returns_none_when_latest_row_is_completed(self):
        svc = _make_service_with_latest_job(
            _failed_job(datetime(2026, 11, 12), status="Completed"),
        )
        result = svc._latest_job_error(
            "user1", success_ts=datetime(2026, 11, 12),
        )
        self.assertIsNone(result)

    def test_returns_none_when_latest_row_is_pending(self):
        svc = _make_service_with_latest_job(
            _failed_job(datetime(2026, 11, 13), status="Pending"),
        )
        result = svc._latest_job_error("user1")
        self.assertIsNone(result)

    def test_returns_none_when_no_job_rows_exist(self):
        svc = _make_service_with_latest_job(None)
        result = svc._latest_job_error("user1")
        self.assertIsNone(result)

    # ── 8: _shape_response wires success_ts=msg.ts ────────────────────

    def test_shape_response_wires_msg_ts_as_success_ts(self):
        """Source-grep drift guard: ``_shape_response`` must call
        ``_latest_job_error(user_id, success_ts=ts)``. A future
        refactor that drops this kwarg restores the stale-banner
        bug silently — test is the structural pin."""
        src_path = os.path.join(_HERE, "services", "wealthDigestService.py")
        with open(src_path) as fh:
            src = fh.read()
        # The exact wiring the fix introduces. Pin on the full
        # call shape so a rename / reorder also reds.
        assert "_latest_job_error(user_id, success_ts=ts)" in src, (
            "_shape_response must pass success_ts=ts to "
            "_latest_job_error — ak-ucta fix dropped / regressed"
        )


if __name__ == "__main__":
    unittest.main()
