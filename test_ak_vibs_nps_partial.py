"""ak-vibs (2026-11-24): an NPS partial outage must not shrink the
published scheme list, and a revived dead scheme must get a rate.

* If any download ERRORED this run, previous entries missing from the new
  list are carried over; absent (empty-body) schemes are removed only on
  a clean run.
* A result smaller than 80% of the previous list is refused (Failed, the
  previous file is kept), whatever the cause.
* SetNPSRate re-probes the 75 dead ids at most daily (like SetNPSDetails),
  so a revived id gets a rate.
Synthetic data only.
"""
from __future__ import annotations

import os
import sys
import unittest
from unittest import mock

HERE = os.path.dirname(os.path.abspath(__file__))
sys.path.insert(0, HERE)

import test_ak_4caa_nps_download as T  # noqa: E402  (loaders + fakes)

D, DETAILS, RATE = T.D, T.DETAILS, T.RATE


def entry(i):
    return {"id": i, "name": f"N {i}", "pfm_id": "PFM001", "pfm_name": "P"}


def pentry(i, pfm):
    return {"id": i, "name": f"N {i}", "pfm_id": pfm, "pfm_name": "P"}


class TestMerge(unittest.TestCase):

    def test_errored_id_is_kept(self):
        prev = [entry(f"S{i}") for i in range(10)]
        new = prev[:9] + [entry("NEW")]
        out, carried, refuse = D.merge_scheme_lists(new, prev, error_ids={"S9"})
        self.assertEqual((carried, refuse), (1, None))
        self.assertIn("S9", {s["id"] for s in out})

    def test_errored_pfm_keeps_its_unseen_entries_only(self):
        # Review minor 1: a chronic error in PFM001 pins only PFM001's
        # unseen entries; a scheme removed from a clean PFM is dropped.
        prev = ([pentry(f"SM001{i:03d}", "PFM001") for i in range(1, 5)]
                + [pentry(f"SM008{i:03d}", "PFM008") for i in range(1, 8)])
        new = [s for s in prev if s["id"] not in ("SM001004", "SM008007")]
        out, carried, _ = D.merge_scheme_lists(new, prev, error_ids={"SM001002"},
                                               error_pfms={"PFM001"})
        ids = {s["id"] for s in out}
        self.assertIn("SM001004", ids)              # unseen: its PFM had an error
        self.assertNotIn("SM008007", ids)           # removed from a clean PFM
        self.assertEqual(carried, 1)

    def test_confirmed_absent_is_removed_even_in_an_errored_pfm(self):
        prev = [pentry(f"SM001{i:03d}", "PFM001") for i in range(1, 11)]
        out, carried, _ = D.merge_scheme_lists(prev[:9], prev, error_ids={"SM001001"},
                                               error_pfms={"PFM001"}, absent_ids={"SM001010"})
        self.assertEqual((len(out), carried), (9, 0))

    def test_clean_run_honours_removals(self):
        prev = [entry(f"S{i}") for i in range(10)]
        out, carried, refuse = D.merge_scheme_lists(prev[:9], prev)
        self.assertEqual((len(out), carried, refuse), (9, 0, None))

    def test_big_shrink_refused_unless_accepted(self):
        prev = [entry(f"S{i}") for i in range(10)]
        out, _c, refuse = D.merge_scheme_lists(prev[:7], prev)
        self.assertIsNone(out)
        self.assertIn("shrink from 10 to 7", refuse)
        self.assertIsNone(D.merge_scheme_lists(prev[:8], prev)[2])          # 80% ok
        out, _c, refuse = D.merge_scheme_lists(prev[:3], prev, accept_shrink=True)
        self.assertEqual((len(out), refuse), (3, None))

    def test_no_previous_list(self):
        self.assertEqual(D.merge_scheme_lists([entry("A")], [], error_ids={"X"}),
                         ([entry("A")], 0, None))
        self.assertEqual(D.merge_scheme_lists([entry("A")], None)[0], [entry("A")])


class TestRunPublishing(unittest.TestCase):

    def _task(self, previous):
        t = T.TestSetNPSDetails._task(self)
        t.jsonService.getNPSList.return_value = {"data": previous}
        return t

    def _outcomes(self, ok_ids, error_ids=()):
        base = T._outcomes(set(ok_ids))

        def fake(pfm_id, scheme_id, logger, get=None):
            if scheme_id in error_ids:
                return None, D.ERROR, "request failed: Timeout"
            return base(pfm_id, scheme_id, logger)
        return fake

    def _published(self, t):
        saved = t.save_json.call_args[0][0]
        return {s["id"] for s in saved["data"]}

    def test_partial_outage_publishes_merged_list(self):
        prev = [entry(i) for i in ("SM001001", "SM001002", "SM008001")]
        t = self._task(prev)
        with mock.patch.object(DETAILS, "download_scheme_tsv",
                               self._outcomes({"SM001001", "SM001002"}, error_ids={"SM008001"})):
            msg, status, _ = t.run()
        self.assertEqual(status, "Completed")
        self.assertEqual(self._published(t), {"SM001001", "SM001002", "SM008001"})

    def test_successor_id_kept_when_its_host_file_errors(self):
        # SM001025 is never requested itself: it arrives inside the
        # SM001003 file (a successor id). When that file errors, the
        # PFM-level rule keeps it; nothing can confirm it absent.
        prev = [pentry("SM001001", "PFM001"), pentry("SM001025", "PFM001"),
                pentry("SM008001", "PFM008")]
        t = self._task(prev)
        with mock.patch.object(DETAILS, "download_scheme_tsv",
                               self._outcomes({"SM001001", "SM008001"}, error_ids={"SM001003"})):
            _, status, _ = t.run()
        self.assertEqual(status, "Completed")
        self.assertIn("SM001025", self._published(t))

    def test_shrink_without_errors_is_refused_but_job_stays_alive(self):
        prev = [entry(f"SM00100{i}") for i in range(1, 6)] + [entry(f"SM00800{i}") for i in range(1, 6)]
        t = self._task(prev)
        with mock.patch.object(DETAILS, "download_scheme_tsv", self._outcomes({"SM001001"})), \
                mock.patch.dict(os.environ, {}, clear=False):
            os.environ.pop("NPS_ACCEPT_SHRINK", None)
            msg, status, _ = t.run()
        # Review minor 2: Completed (the scheduler keeps rescheduling), with
        # the previous list kept and nothing written.
        self.assertEqual(status, "Completed")
        self.assertIn("kept the previous list", msg)
        t.save_json.assert_not_called()
        t.safe_replace_file.assert_not_called()

    def test_operator_can_accept_a_genuine_restructure(self):
        prev = [entry(f"SM00100{i}") for i in range(1, 6)] + [entry(f"SM00800{i}") for i in range(1, 6)]
        t = self._task(prev)
        with mock.patch.object(DETAILS, "download_scheme_tsv", self._outcomes({"SM001001"})), \
                mock.patch.dict(os.environ, {"NPS_ACCEPT_SHRINK": "1"}):
            _, status, _ = t.run()
        self.assertEqual(status, "Completed")
        self.assertEqual(self._published(t), {"SM001001"})

    def test_previous_list_unreadable_is_tolerated(self):
        t = self._task([])
        t.jsonService.getNPSList.side_effect = FileNotFoundError("no file")
        with mock.patch.object(DETAILS, "download_scheme_tsv", self._outcomes({"SM001001"})):
            _, status, _ = t.run()
        self.assertEqual(status, "Completed")


class TestRateRevived(unittest.TestCase):

    def test_rate_full_probe_daily_includes_revived(self):
        t = T.TestSetNPSRate._task(self)
        t._last_full_probe = 0.0
        calls = []
        base = T._outcomes({"SM001001", "SM009001"})          # SM009001 is on the dead list

        def fake(pfm_id, scheme_id, logger, get=None):
            calls.append(scheme_id)
            return base(pfm_id, scheme_id, logger)
        day = D.FULL_PROBE_INTERVAL_S
        with mock.patch.object(RATE, "download_scheme_tsv", fake), \
                mock.patch.object(RATE, "_now", side_effect=[1_000_000.0, 1_000_060.0,
                                                             1_000_000.0 + day + 1]):
            first = t.fetchNPSRatesFromExcel()
            n1, calls[:] = len(calls), []
            t.fetchNPSRatesFromExcel()
            n2, calls[:] = len(calls), []
            t.fetchNPSRatesFromExcel()
            n3 = len(calls)
        self.assertEqual((n1, n2, n3), (224, 149, 224))
        self.assertIn("SM009001", [r["scheme_id"] for r in first["data"]])


if __name__ == "__main__":  # pragma: no cover
    unittest.main(verbosity=2)
