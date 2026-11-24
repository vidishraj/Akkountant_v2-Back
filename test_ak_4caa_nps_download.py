"""ak-4caa (2026-11-24): NPS 'No columns to parse' on every run.

Measured from the fleet host: all 224 probed PFM x scheme URLs answer
HTTP 200; 149 carry the TSV, 75 (combinations that don't exist) are
text/html with an EMPTY body. SetNPSDetails pd.read_csv'd every body and
logged pandas' EmptyDataError ("No columns to parse") at ERROR for those
75 on every run. The fix classifies each download (ok / absent / error),
keeps "absent" at DEBUG, reports real errors in aggregate, and refuses to
complete green (or replace the scheme list) when nothing usable came
back. Synthetic data only.
"""
from __future__ import annotations

import importlib.util
import logging
import os
import sys
import types
import unittest
from unittest import mock

HERE = os.path.dirname(os.path.abspath(__file__))
sys.path.insert(0, HERE)

import requests  # noqa: E402

from services.tasks import npsSchemeDownload as D  # noqa: E402

HEADER = "ID\tDATE OF NAV\tPFM ID\tPFM NAME\tSCHEME ID\tSCHEME NAME\tNAV VALUE\n"


def tsv(scheme_id="SM001001", rows=3):
    out = HEADER
    for i in range(rows):
        out += (f"{i + 1}\t2026-11-{23 - i:02d}\tPFM001\tSynthetic PFM\t"
                f"{scheme_id}\tSYNTHETIC SCHEME E - TIER I\t{50 + i}.1234\n")
    return out.encode()


class FakeResp:
    def __init__(self, body=b"", status=200, ctype="application/vnd.ms-excel"):
        self.content = body
        self.status_code = status
        self.headers = {"content-type": ctype}

    def raise_for_status(self):
        if self.status_code >= 400:
            raise requests.HTTPError(f"{self.status_code}")


def getter(resp):
    def get(url, **kw):
        if isinstance(resp, Exception):
            raise resp
        return resp
    return get


LOG = logging.getLogger("ak4caa.test")


def _load_task(modname):
    """Load a task module from its file with BaseTask/BaseRateTask
    stubbed (the real chain imports the whole app stack, incl. tabula,
    which this sandbox lacks). Only the task's own code is exercised."""
    base = types.ModuleType("services.tasks.baseTask")
    base.BaseTask = type("BaseTask", (), {"__init__": lambda self, *a, **k: None})
    rbase = types.ModuleType("services.tasks.BaseRateTask")
    rbase.BaseRateTask = type("BaseRateTask", (), {"__init__": lambda self, *a, **k: None})
    lg = types.ModuleType("utils.logger")
    lg.Logger = lambda name: types.SimpleNamespace(get_logger=lambda: LOG)
    with mock.patch.dict(sys.modules, {"services.tasks.baseTask": base,
                                       "services.tasks.BaseRateTask": rbase,
                                       "utils.logger": lg}):
        spec = importlib.util.spec_from_file_location(
            f"ak4caa_{modname}", os.path.join(HERE, "services", "tasks", f"{modname}.py"))
        m = importlib.util.module_from_spec(spec)
        spec.loader.exec_module(m)
    return m


DETAILS = _load_task("SetNPSDetails")
RATE = _load_task("SetNPSRate")


class TestClassification(unittest.TestCase):

    def dl(self, resp):
        return D.download_scheme_tsv("PFM001", "SM001001", LOG, get=getter(resp))

    def test_real_tsv_is_ok(self):
        df, outcome, reason = self.dl(FakeResp(tsv()))
        self.assertEqual(outcome, D.OK)
        self.assertEqual(len(df), 3)
        self.assertIsNone(reason)

    def test_empty_200_html_is_absent_not_error(self):
        # The exact prod shape for a non-existent combination.
        for body in (b"", b"  \n"):
            _, outcome, _ = self.dl(FakeResp(body, ctype="text/html; charset=UTF-8"))
            self.assertEqual(outcome, D.ABSENT, body)

    def test_header_only_is_absent(self):
        self.assertEqual(self.dl(FakeResp(HEADER.encode()))[1], D.ABSENT)

    def test_http_and_network_failures_are_errors(self):
        self.assertEqual(self.dl(FakeResp(b"x", status=503))[1], D.ERROR)
        self.assertEqual(self.dl(requests.ConnectionError("boom"))[1], D.ERROR)

    def test_html_page_or_changed_columns_are_errors(self):
        _, outcome, reason = self.dl(FakeResp(b"<html><body>Maintenance</body></html>",
                                              ctype="text/html"))
        self.assertEqual(outcome, D.ERROR)
        moved = b"Date\tNAV\n2026-11-23\t50.1\n"
        _, outcome, reason = self.dl(FakeResp(moved))
        self.assertEqual(outcome, D.ERROR)
        self.assertIn("format changed", reason)

    def test_absent_never_logs_above_debug(self):
        with self.assertLogs(LOG, level="DEBUG") as cm:
            LOG.debug("sentinel")
            self.dl(FakeResp(b"", ctype="text/html"))
        self.assertTrue(all(r.levelno <= logging.DEBUG for r in cm.records))
        self.assertNotIn("No columns to parse", "\n".join(cm.output))

    def test_universe_is_224(self):
        ids = list(D.scheme_ids(include_dead=True))
        self.assertEqual(len(ids), 224)
        self.assertEqual(ids[0], ("PFM001", "SM001001"))
        self.assertEqual(ids[-1], ("PFM014", "SM014016"))


class TestTallyLoudness(unittest.TestCase):

    def _log(self, ok, absent, error):
        t = D.DownloadTally()
        for _ in range(ok):
            t.add("P", "S", D.OK, None)
        for _ in range(absent):
            t.add("P", "S", D.ABSENT, None)
        for _ in range(error):
            t.add("P", "S", D.ERROR, "request failed: Timeout")
        with self.assertLogs(LOG, level="INFO") as cm:
            t.log(LOG, "T")
        return cm.records[-1]

    def test_levels(self):
        self.assertEqual(self._log(149, 75, 0).levelno, logging.INFO)
        self.assertEqual(self._log(140, 75, 9).levelno, logging.WARNING)
        rec = self._log(0, 224, 0)
        self.assertEqual(rec.levelno, logging.ERROR)
        self.assertIn("NO usable NPS scheme files", rec.getMessage())
        self.assertEqual(self._log(0, 0, 224).levelno, logging.ERROR)


def _outcomes(ok_ids):
    def fake(pfm_id, scheme_id, logger, get=None):
        if scheme_id in ok_ids:
            import pandas as pd
            from io import BytesIO
            return pd.read_csv(BytesIO(tsv(scheme_id)), sep="\t"), D.OK, None
        return None, D.ABSENT, None
    return fake


class TestSetNPSDetails(unittest.TestCase):

    def _task(self):
        t = DETAILS.SetNPSDetails.__new__(DETAILS.SetNPSDetails)
        t.logger = LOG
        t.interval = 90
        t.tmp_dir = "/nonexistent-ak4caa"
        t.save_json = mock.MagicMock()
        t.safe_replace_file = mock.MagicMock(return_value=(True, None))
        t.jsonService = mock.MagicMock()
        t._last_full_probe = 0.0        # singleton: reset → first run is a full probe
        return t

    def test_all_absent_fails_and_keeps_previous_list(self):
        t = self._task()
        with mock.patch.object(DETAILS, "download_scheme_tsv", _outcomes(set())), \
                self.assertLogs(LOG, level="ERROR"):
            msg, status, _ = t.run()
        self.assertEqual(status, "Failed")
        self.assertIn("keeping the previous list", msg)
        t.safe_replace_file.assert_not_called()
        t.save_json.assert_not_called()

    def test_real_schemes_complete_and_absent_is_quiet(self):
        t = self._task()
        ok = {"SM001001", "SM001002", "SM008001"}
        with mock.patch.object(DETAILS, "download_scheme_tsv", _outcomes(ok)), \
                self.assertLogs(LOG, level="DEBUG") as cm:
            data = t.fetchNPSDetailsFromExcel()
        self.assertEqual(sorted(s["id"] for s in data["data"]), sorted(ok))
        self.assertIn("ok=3 absent=221 error=0", data["download_summary"])
        self.assertFalse([r for r in cm.records if r.levelno >= logging.WARNING])

    def test_run_writes_when_schemes_found(self):
        t = self._task()
        with mock.patch.object(DETAILS, "download_scheme_tsv", _outcomes({"SM001001"})):
            _, status, _ = t.run()
        self.assertEqual(status, "Completed")
        t.safe_replace_file.assert_called_once()


class TestSetNPSRate(unittest.TestCase):

    def _task(self):
        t = RATE.SetNPSRate.__new__(RATE.SetNPSRate)
        t.logger = LOG
        t.db = mock.MagicMock()
        t._persist_historical_rows = mock.MagicMock(return_value=0)
        return t

    def test_rates_from_real_schemes_and_loud_when_none(self):
        t = self._task()
        with mock.patch.object(RATE, "download_scheme_tsv", _outcomes({"SM001001", "SM002001"})):
            out = t.fetchNPSRatesFromExcel()
        self.assertEqual(out["total_schemes"], 2)
        self.assertEqual(out["data"][0]["date"], "2026-11-23")
        self.assertEqual(t._persist_historical_rows.call_count, 2)

        t = self._task()
        with mock.patch.object(RATE, "download_scheme_tsv", _outcomes(set())), \
                self.assertLogs(LOG, level="ERROR") as cm:
            out = t.fetchNPSRatesFromExcel()
        self.assertEqual(out["total_schemes"], 0)       # BaseRateTask gate → Failed
        self.assertIn("NO usable NPS scheme files", "\n".join(cm.output))


class TestDeadIdsAndHtml(unittest.TestCase):
    """Lead rescope (P3): stop polling the 75 measured-dead ids, keep one
    INFO count per run, and never hand a non-TSV body to pandas."""

    def _counting(self, ok_ids=frozenset({"SM001001"}), revived=frozenset()):
        calls = []
        base = _outcomes(set(ok_ids) | set(revived))

        def fake(pfm_id, scheme_id, logger, get=None):
            calls.append(scheme_id)
            return base(pfm_id, scheme_id, logger)
        return fake, calls

    def test_dead_list_is_the_measured_75(self):
        self.assertEqual(len(D.DEAD_SCHEME_IDS), 75)
        self.assertEqual(len(list(D.scheme_ids())), 149)
        self.assertEqual(len(list(D.scheme_ids(include_dead=True))), 224)
        for pfm in ("004", "006", "009", "012"):          # whole PFMs dead
            self.assertTrue(all(f"SM{pfm}{n:03d}" in D.DEAD_SCHEME_IDS for n in range(1, 17)))

    def test_rate_task_never_polls_dead_ids(self):
        t = TestSetNPSRate._task(self)
        fake, calls = self._counting()
        with mock.patch.object(RATE, "download_scheme_tsv", fake):
            t.fetchNPSRatesFromExcel()
        self.assertEqual(len(calls), 149)
        self.assertFalse(set(calls) & D.DEAD_SCHEME_IDS)

    def test_details_full_probe_at_most_daily(self):
        t = TestSetNPSDetails._task(self)
        t._last_full_probe = 0.0
        fake, calls = self._counting()
        with mock.patch.object(DETAILS, "download_scheme_tsv", fake), \
                mock.patch.object(DETAILS, "_now", side_effect=[1_000_000.0, 1_000_060.0,
                                                                1_000_000.0 + D.FULL_PROBE_INTERVAL_S + 1]):
            t.fetchNPSDetailsFromExcel()
            first = len(calls)
            calls.clear()
            out = t.fetchNPSDetailsFromExcel()
            second = len(calls)
            calls.clear()
            t.fetchNPSDetailsFromExcel()
            third = len(calls)
        self.assertEqual((first, second, third), (224, 149, 224))
        self.assertIn("skipped_dead=75", out["download_summary"])

    def test_revived_dead_id_is_warned(self):
        t = TestSetNPSDetails._task(self)
        t._last_full_probe = 0.0
        fake, _ = self._counting(revived={"SM009001"})
        with mock.patch.object(DETAILS, "download_scheme_tsv", fake), \
                self.assertLogs(LOG, level="WARNING") as cm:
            data = t.fetchNPSDetailsFromExcel()
        self.assertIn("SM009001", [s["id"] for s in data["data"]])
        self.assertTrue(any("now serve data" in m and "SM009001" in m for m in cm.output))

    def test_html_page_is_error_and_never_parsed(self):
        with mock.patch.object(D.pd, "read_csv") as rc:
            _, outcome, reason = D.download_scheme_tsv(
                "PFM001", "SM001001", LOG,
                get=getter(FakeResp(b"<html>maintenance</html>", ctype="text/html; charset=UTF-8")))
            _, outcome2, _ = D.download_scheme_tsv(
                "PFM001", "SM001001", LOG, get=getter(FakeResp(b"", ctype="text/html")))
        rc.assert_not_called()
        self.assertEqual((outcome, outcome2), (D.ERROR, D.ABSENT))
        self.assertIn("html page", reason)

    def test_clean_run_logs_one_info_summary(self):
        t = TestSetNPSRate._task(self)
        fake, _ = self._counting()
        with mock.patch.object(RATE, "download_scheme_tsv", fake), \
                self.assertLogs(LOG, level="INFO") as cm:
            t.fetchNPSRatesFromExcel()
        summaries = [r for r in cm.records if "NPS downloads" in r.getMessage()]
        self.assertEqual(len(summaries), 1)
        self.assertEqual(summaries[0].levelno, logging.INFO)
        self.assertFalse([r for r in cm.records if r.levelno >= logging.ERROR])


if __name__ == "__main__":  # pragma: no cover
    unittest.main(verbosity=2)
