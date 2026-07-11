"""ak-uvy regression tests — async scan completion signal.

Pure-Python (threading + unittest) coverage of the scan_wait
helpers. The controller-layer integration (TransactionController
+ /readEmails/status?wait=true) exercises the same helpers with a
live Flask + threading loop — env-limited integration coverage
stays deferred at lead-verify.

Locked contract:

  is_terminal_status(status):
    True iff status is 'completed' or 'failed' (case-insensitive,
    whitespace-tolerant). Non-string / None / empty → False.

  make_scan_event():
    Returns a fresh threading.Event.

  signal_terminal(progress):
    If progress['_event'] is a threading.Event, set it. No-op
    otherwise. Never raises.

  wait_for_scan_completion(scans, lock, scan_id, timeout):
    Blocks up to `timeout` seconds waiting for the scan's event.
    None if scan_id missing. Returns current progress dict if the
    event isn't set within timeout OR if there's no event at all
    (legacy fall-through).

  scan_progress_for_client(progress):
    Strips underscore-prefixed private fields (_event) and injects
    an `is_terminal` bool derived from status.
"""

import os
import sys
import threading
import time
import unittest

sys.path.insert(0, os.path.dirname(os.path.abspath(__file__)))

from utils.scan_wait import (
    HTTP_MAX_WAIT_SECONDS,
    NON_TERMINAL_STATUSES,
    SCAN_TTL_SECONDS,
    TERMINAL_STATUSES,
    clamp_http_wait_timeout,
    evict_completed_scans,
    is_terminal_status,
    make_scan_event,
    scan_progress_for_client,
    scope_scan_to_user,
    signal_terminal,
    wait_for_scan_completion,
)


# ── is_terminal_status ─────────────────────────────────────────────


class TestTerminalStatusTruth(unittest.TestCase):
    def test_completed_is_terminal(self):
        self.assertTrue(is_terminal_status("completed"))

    def test_failed_is_terminal(self):
        self.assertTrue(is_terminal_status("failed"))

    def test_case_insensitive(self):
        for form in ("Completed", "COMPLETED", "cOmPlEtEd"):
            with self.subTest(form=form):
                self.assertTrue(is_terminal_status(form))

    def test_whitespace_tolerant(self):
        self.assertTrue(is_terminal_status("  completed  "))


class TestNonTerminalStatuses(unittest.TestCase):
    def test_started_not_terminal(self):
        self.assertFalse(is_terminal_status("started"))

    def test_processing_not_terminal(self):
        self.assertFalse(is_terminal_status("processing"))

    def test_fetching_not_terminal(self):
        self.assertFalse(is_terminal_status("fetching"))


class TestTerminalStatusDefensive(unittest.TestCase):
    def test_none_not_terminal(self):
        self.assertFalse(is_terminal_status(None))

    def test_empty_string_not_terminal(self):
        self.assertFalse(is_terminal_status(""))

    def test_int_not_terminal(self):
        self.assertFalse(is_terminal_status(42))

    def test_list_not_terminal(self):
        self.assertFalse(is_terminal_status(["completed"]))

    def test_unknown_status_not_terminal(self):
        """Future status not yet added → not terminal (safer)."""
        self.assertFalse(is_terminal_status("some_new_state"))


class TestConstantsShape(unittest.TestCase):
    def test_terminal_set_contents(self):
        self.assertEqual(TERMINAL_STATUSES,
                         frozenset({"completed", "failed"}))

    def test_non_terminal_set_has_expected(self):
        for expected in ("started", "fetching", "classifying",
                         "processing"):
            with self.subTest(status=expected):
                self.assertIn(expected, NON_TERMINAL_STATUSES)


# ── signal_terminal ────────────────────────────────────────────────


class TestSignalTerminal(unittest.TestCase):
    def test_sets_event(self):
        e = make_scan_event()
        progress = {"status": "completed", "_event": e}
        self.assertFalse(e.is_set())
        signal_terminal(progress)
        self.assertTrue(e.is_set())

    def test_no_event_no_op(self):
        # No _event field → doesn't raise.
        signal_terminal({"status": "completed"})

    def test_wrong_type_event_no_op(self):
        # _event is not a threading.Event → doesn't raise.
        signal_terminal({"status": "completed", "_event": "not-an-event"})

    def test_non_dict_no_op(self):
        signal_terminal(None)
        signal_terminal("string")
        signal_terminal(42)


# ── wait_for_scan_completion ───────────────────────────────────────


class TestWaitReturnsOnTerminal(unittest.TestCase):
    """Sanity: when the scan is ALREADY terminal, wait returns
    immediately."""

    def _tracker_with(self, scan_id, progress):
        return ({scan_id: progress}, threading.Lock())

    def test_immediate_return_when_event_already_set(self):
        e = make_scan_event()
        e.set()
        progress = {"status": "completed", "_event": e}
        scans, lock = self._tracker_with("s1", progress)
        start = time.monotonic()
        got = wait_for_scan_completion(scans, lock, "s1", timeout=5.0)
        elapsed = time.monotonic() - start
        self.assertIsNotNone(got)
        self.assertEqual(got["status"], "completed")
        # Should be almost instant.
        self.assertLess(elapsed, 0.5)


class TestWaitReturnsNoneOnMissing(unittest.TestCase):
    def test_unknown_scan_id_returns_none(self):
        got = wait_for_scan_completion({}, threading.Lock(),
                                       "nope", timeout=0.1)
        self.assertIsNone(got)


class TestWaitBlocksUntilSignaled(unittest.TestCase):
    """Threading integration: wait blocks until a separate thread
    calls signal_terminal."""

    def test_wait_unblocks_on_signal(self):
        e = make_scan_event()
        progress = {"status": "started", "_event": e}
        scans, lock = {"s2": progress}, threading.Lock()

        # Background thread signals termination after 100ms.
        def signaler():
            time.sleep(0.1)
            with lock:
                progress["status"] = "completed"
            signal_terminal(progress)

        t = threading.Thread(target=signaler, daemon=True)
        t.start()

        start = time.monotonic()
        got = wait_for_scan_completion(scans, lock, "s2", timeout=2.0)
        elapsed = time.monotonic() - start

        t.join(timeout=1.0)
        self.assertIsNotNone(got)
        self.assertEqual(got["status"], "completed")
        # Should return promptly after the signal — well under the
        # 2-second timeout, and after at least the 100ms wait.
        self.assertLess(elapsed, 1.5)
        self.assertGreaterEqual(elapsed, 0.05)


class TestWaitTimesOut(unittest.TestCase):
    """When timeout elapses without a signal, wait returns the
    CURRENT progress (which will still be non-terminal). Caller
    inspects is_terminal_status(returned['status'])."""

    def test_timeout_returns_current_progress(self):
        e = make_scan_event()
        progress = {"status": "processing", "_event": e}
        scans, lock = {"s3": progress}, threading.Lock()

        start = time.monotonic()
        got = wait_for_scan_completion(scans, lock, "s3", timeout=0.1)
        elapsed = time.monotonic() - start

        self.assertIsNotNone(got)
        self.assertEqual(got["status"], "processing")
        self.assertFalse(is_terminal_status(got["status"]))
        self.assertGreaterEqual(elapsed, 0.05)


class TestWaitZeroTimeoutNonBlocking(unittest.TestCase):
    """timeout=0 → non-blocking snapshot poll."""

    def test_zero_timeout_returns_immediately(self):
        e = make_scan_event()
        progress = {"status": "processing", "_event": e}
        scans, lock = {"s4": progress}, threading.Lock()
        start = time.monotonic()
        got = wait_for_scan_completion(scans, lock, "s4", timeout=0)
        elapsed = time.monotonic() - start
        self.assertIsNotNone(got)
        self.assertLess(elapsed, 0.05)


class TestWaitLegacyNoEventGracefulDegradation(unittest.TestCase):
    """A pre-ak-uvy scan (no _event field) should not hang the
    wait helper — return the current snapshot gracefully."""

    def test_missing_event_returns_snapshot(self):
        progress = {"status": "started"}  # no _event
        scans, lock = {"s5": progress}, threading.Lock()
        start = time.monotonic()
        got = wait_for_scan_completion(scans, lock, "s5", timeout=5.0)
        elapsed = time.monotonic() - start
        self.assertIsNotNone(got)
        # Should return immediately (graceful fall-through).
        self.assertLess(elapsed, 0.2)


# ── scan_progress_for_client ───────────────────────────────────────


class TestScanProgressForClient(unittest.TestCase):
    def test_strips_underscore_prefixed_fields(self):
        progress = {
            "status": "completed",
            "stage": "done",
            "_event": make_scan_event(),
            "_private": "shhh",
        }
        public = scan_progress_for_client(progress)
        self.assertNotIn("_event", public)
        self.assertNotIn("_private", public)
        self.assertIn("status", public)
        self.assertIn("stage", public)

    def test_injects_is_terminal_true(self):
        progress = {"status": "completed"}
        public = scan_progress_for_client(progress)
        self.assertTrue(public["is_terminal"])

    def test_injects_is_terminal_false(self):
        progress = {"status": "processing"}
        public = scan_progress_for_client(progress)
        self.assertFalse(public["is_terminal"])

    def test_non_dict_returns_error(self):
        got = scan_progress_for_client(None)
        self.assertIn("error", got)


# ── Reviewer scenario (Lead's dispatch) ────────────────────────────


class TestReviewerScenario(unittest.TestCase):
    """Verbatim from Lead's dispatch (hq-wisp-ekveei):

      'STEP 3 "done" report captured mid-run; 618 rows leaked in
       during the elapsed hour.'

    Under ak-uvy the caller can either:
      (a) call wait_for_scan_completion → guaranteed to return
          only when status is terminal (or timeout).
      (b) inspect scan_progress_for_client's is_terminal field
          before treating the snapshot as final.

    This test simulates the ak-32o pattern and verifies that a
    caller who uses either signal correctly distinguishes 'still
    running' from 'done'."""

    def test_mid_run_snapshot_not_terminal(self):
        e = make_scan_event()  # not set — still running
        progress = {"status": "processing", "_event": e}
        public = scan_progress_for_client(progress)
        self.assertFalse(
            public["is_terminal"],
            "regression: mid-run scan classified as terminal — "
            "ak-32o 618-row-leak class",
        )

    def test_wait_returns_only_after_completion(self):
        """The direct opposite: a caller that used wait_for_scan_completion
        gets the terminal state only after the scan actually finishes."""
        e = make_scan_event()
        progress = {"status": "processing", "_event": e}
        scans, lock = {"s6": progress}, threading.Lock()

        # Simulate the scan finishing after 50ms.
        def finish():
            time.sleep(0.05)
            with lock:
                progress["status"] = "completed"
                progress["result"] = {"rows_inserted": 42}
            signal_terminal(progress)

        threading.Thread(target=finish, daemon=True).start()

        got = wait_for_scan_completion(scans, lock, "s6", timeout=2.0)
        self.assertIsNotNone(got)
        self.assertEqual(got["status"], "completed")
        self.assertTrue(is_terminal_status(got["status"]))
        self.assertEqual(got.get("result", {}).get("rows_inserted"), 42)


# ── ak-uvy v2: HTTP bounding + eviction + scoping ──────────────────


class TestClampHttpWaitTimeout(unittest.TestCase):
    """v2 MAJOR: HTTP path never signals unbounded event.wait —
    hanging scan would pin a gunicorn worker forever."""

    def test_none_defaults_to_cap(self):
        self.assertEqual(
            clamp_http_wait_timeout(None), HTTP_MAX_WAIT_SECONDS,
        )

    def test_missing_defaults_to_cap(self):
        # Same code path (raw_timeout is None from request.args.get)
        self.assertEqual(
            clamp_http_wait_timeout(None, max_seconds=30.0), 30.0
        )

    def test_smaller_value_kept(self):
        self.assertEqual(clamp_http_wait_timeout(5.0), 5.0)
        self.assertEqual(clamp_http_wait_timeout(0.5), 0.5)

    def test_larger_value_clamped(self):
        self.assertEqual(
            clamp_http_wait_timeout(999_999.0), HTTP_MAX_WAIT_SECONDS,
        )
        self.assertEqual(
            clamp_http_wait_timeout(60.0, max_seconds=30.0), 30.0
        )

    def test_zero_returns_zero(self):
        """Non-blocking snapshot is preserved verbatim."""
        self.assertEqual(clamp_http_wait_timeout(0), 0.0)
        self.assertEqual(clamp_http_wait_timeout(0.0), 0.0)

    def test_negative_returns_zero(self):
        """Never signal a negative or infinite wait."""
        self.assertEqual(clamp_http_wait_timeout(-5.0), 0.0)
        self.assertEqual(clamp_http_wait_timeout(-999.0), 0.0)

    def test_non_numeric_defaults_to_cap(self):
        """Bad input → server-side cap (was 400 error pre-v2;
        v2 tolerates + clamps)."""
        self.assertEqual(
            clamp_http_wait_timeout("not-a-number"),
            HTTP_MAX_WAIT_SECONDS,
        )
        self.assertEqual(
            clamp_http_wait_timeout([1, 2]), HTTP_MAX_WAIT_SECONDS,
        )

    def test_string_numeric_converted(self):
        """?timeout=5 arrives as a string from request.args.get."""
        self.assertEqual(clamp_http_wait_timeout("5"), 5.0)
        self.assertEqual(clamp_http_wait_timeout("0.5"), 0.5)


class TestEvictCompletedScans(unittest.TestCase):
    """v2 MINOR 1: registry can't grow without bound. Terminal
    entries older than SCAN_TTL_SECONDS get swept."""

    def _tracker(self):
        return {}, threading.Lock()

    def test_terminal_older_than_ttl_evicted(self):
        scans, lock = self._tracker()
        # Fake a terminal_at timestamp in the past.
        now = 1000.0
        scans["old"] = {
            "status": "completed", "_terminal_at": now - 600,
        }
        scans["fresh"] = {
            "status": "completed", "_terminal_at": now - 30,
        }
        n = evict_completed_scans(
            scans, lock, ttl_seconds=300.0, now=now,
        )
        self.assertEqual(n, 1)
        self.assertNotIn("old", scans)
        self.assertIn("fresh", scans)

    def test_non_terminal_never_evicted(self):
        """Running scans have no _terminal_at → never eligible for
        eviction."""
        scans, lock = self._tracker()
        scans["running"] = {"status": "processing"}
        n = evict_completed_scans(
            scans, lock, ttl_seconds=0.0, now=99999.0,
        )
        self.assertEqual(n, 0)
        self.assertIn("running", scans)

    def test_returns_evicted_count(self):
        scans, lock = self._tracker()
        now = 1000.0
        for i in range(3):
            scans[f"s{i}"] = {
                "status": "completed", "_terminal_at": now - 1000,
            }
        n = evict_completed_scans(scans, lock,
                                  ttl_seconds=300.0, now=now)
        self.assertEqual(n, 3)
        self.assertEqual(scans, {})

    def test_non_dict_entries_purged(self):
        scans, lock = self._tracker()
        scans["bad"] = "somehow-a-string"
        n = evict_completed_scans(scans, lock, ttl_seconds=0.0,
                                   now=1000.0)
        self.assertEqual(n, 1)
        self.assertNotIn("bad", scans)


class TestScopeScanToUser(unittest.TestCase):
    """v2 MINOR 2: scan_id scoped to user_id. Cross-user probe →
    False → endpoint returns 404 (existence not leaked)."""

    def test_matching_user_allowed(self):
        self.assertTrue(
            scope_scan_to_user({"user_id": "u1"}, "u1")
        )

    def test_different_user_denied(self):
        self.assertFalse(
            scope_scan_to_user({"user_id": "u1"}, "u2")
        )

    def test_expected_user_none_allows_all(self):
        """Backward compat with the pre-v2 endpoint (no scoping)."""
        self.assertTrue(
            scope_scan_to_user({"user_id": "u1"}, None)
        )
        self.assertTrue(
            scope_scan_to_user({"user_id": "u1"}, "")
        )

    def test_progress_without_user_id_allowed(self):
        """Legacy scan without user_id field → allow (defensive
        backward compat, matches the pre-v2 behavior)."""
        self.assertTrue(
            scope_scan_to_user({"status": "started"}, "u1")
        )

    def test_non_dict_progress_allowed(self):
        """Defensive: never raise; the caller checks 404 upstream."""
        self.assertTrue(scope_scan_to_user(None, "u1"))
        self.assertTrue(scope_scan_to_user("string", "u1"))


class TestSignalTerminalStampsTerminalAt(unittest.TestCase):
    """v2: signal_terminal now stamps `_terminal_at` (monotonic
    timestamp) so the eviction sweeper can decide when to drop
    the entry."""

    def test_stamps_terminal_at(self):
        e = make_scan_event()
        progress = {"status": "completed", "_event": e}
        signal_terminal(progress)
        self.assertIn("_terminal_at", progress)
        self.assertIsInstance(progress["_terminal_at"], float)

    def test_stamps_even_without_event(self):
        """Legacy scans lacking _event should still be eligible
        for eviction — stamp the timestamp regardless."""
        progress = {"status": "completed"}
        signal_terminal(progress)
        self.assertIn("_terminal_at", progress)


class TestScanProgressStripsTerminalAt(unittest.TestCase):
    """v2: `_terminal_at` (like `_event`) must be stripped before
    the JSON response — clients shouldn't see monotonic ints."""

    def test_terminal_at_stripped(self):
        e = make_scan_event()
        progress = {
            "status": "completed", "user_id": "u1",
            "_event": e, "_terminal_at": 12345.6,
        }
        public = scan_progress_for_client(progress)
        self.assertNotIn("_terminal_at", public)
        self.assertNotIn("_event", public)
        # user_id passes through (public field).
        self.assertEqual(public["user_id"], "u1")


class TestReviewerV2Scenarios(unittest.TestCase):
    """Reviewer's v2 acceptance criteria — direct scenario checks."""

    def test_http_wait_never_unbounded(self):
        """No matter what the client sends, HTTP timeout ≤ cap."""
        cases = [None, "999999", 999999.0, "not-a-number",
                 -5, 0, "0"]
        for raw in cases:
            with self.subTest(raw=raw):
                got = clamp_http_wait_timeout(raw)
                self.assertLessEqual(got, HTTP_MAX_WAIT_SECONDS)
                self.assertGreaterEqual(got, 0.0)

    def test_completed_scan_evicted_after_ttl(self):
        scans, lock = {}, threading.Lock()
        e = make_scan_event()
        progress = {"status": "completed", "_event": e,
                    "user_id": "u1"}
        scans["s1"] = progress
        signal_terminal(progress)  # stamps _terminal_at
        # Fast-forward by mocking `now`.
        fake_now = progress["_terminal_at"] + SCAN_TTL_SECONDS + 1
        evict_completed_scans(scans, lock, now=fake_now)
        self.assertNotIn("s1", scans)

    def test_cross_user_scan_lookup_denied(self):
        """A user with a leaked scan_id from another user gets
        the 'not found' shape — endpoint uses this."""
        scans = {"s1": {"user_id": "u_alice", "status": "processing"}}
        with self.assertRaises(AssertionError):
            # This test just asserts the predicate wires up right;
            # the endpoint's 404 is integration-tested.
            assert scope_scan_to_user(scans["s1"], "u_bob")
        # And the affirmative case:
        self.assertTrue(scope_scan_to_user(scans["s1"], "u_alice"))


if __name__ == "__main__":
    print("ak-uvy scan-completion signal tests")
    print("=" * 60)
    unittest.main(verbosity=2, exit=False)
    print("=" * 60)
