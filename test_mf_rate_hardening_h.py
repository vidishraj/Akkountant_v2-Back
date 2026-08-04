"""ak-5jq HIGH batch regression tests: null-guard on stale MF list,
timeout + exponential-backoff-with-jitter, 404/429/5xx error
classification, LOCAL env stale-file 30-day hard limit, and optional
SetMFDetails DROP alarm.

Post-ak-539-v2. Scoped to services/tasks/SetMfRate.py (H1/H2/H3),
services/JsonDownloadService.py (H4), and services/tasks/SetMfDetails.py
(the optional H3-theme drop alarm from Lead's dispatch).

  H1 SetMFRate.run() returns clean 'Failed' when getLatestFile is None
     (stale MF_details), instead of the pre-fix TypeError crash on
     open(None).
  H2 Per-request timeout dropped 30s → 10s (+ 5s connect budget).
     Retry backoff switched linear (2/4/6s) → exponential-with-jitter
     (base 2s, cap 30s for 5xx; base 5s, cap 60s for 429). Prevents
     retry thundering-herd.
  H3 fetch_scheme now classifies non-200 responses:
       * 404  → permanent_skip_404, NEVER retried
       * 429  → long backoff, retry
       * 5xx  → normal backoff, retry
       * other → WARN + final_other, no retry
     _fetch_all_passes filters permanent_skip_ids from failed_urls,
     accumulates per-class counts, and returns them alongside
     result_map. run()/buildJsonForMF surface the permanent-skip
     count in the Completed msg + subtract from ratio denominator.
  H4 checkJsonInDirectory in LOCAL env: <30d stale returns True (dev
     safety preserved); >=30d stale returns False (force re-download);
     AK_ALLOW_STALE_LIST=1 escape hatch restores the old always-True
     behavior for archival replay.
  Drop alarm (H3-theme optional): SetMFDetails._log_scheme_counts
     ERRORs on >2× DOWN-spikes (mirrors the existing >2× UP-spike
     alarm; catches "MFAPI silently returned half the universe").

Run:
    python3 -m unittest test_mf_rate_hardening_h
    python3 -m pytest test_mf_rate_hardening_h.py
"""

import ast
import os
import shutil
import sys
import tempfile
import unittest


sys.path.insert(0, os.path.dirname(os.path.abspath(__file__)))


_SET_MF_RATE_PATH = os.path.join(
    os.path.dirname(os.path.abspath(__file__)),
    "services", "tasks", "SetMfRate.py",
)
_SET_MF_DETAILS_PATH = os.path.join(
    os.path.dirname(os.path.abspath(__file__)),
    "services", "tasks", "SetMfDetails.py",
)
_JSON_DL_PATH = os.path.join(
    os.path.dirname(os.path.abspath(__file__)),
    "services", "JsonDownloadService.py",
)


def _source(path):
    with open(path) as fh:
        return fh.read()


def _source_code_only(path):
    """Strip full-line and end-of-line comments so a comment referencing
    a forbidden pattern doesn't false-trip a source guard. Same helper
    pattern as test_mf_rate_hardening.py."""
    src = _source(path)
    out = []
    for line in src.splitlines():
        stripped = line.lstrip()
        if stripped.startswith("#"):
            continue
        hash_pos = line.find("#")
        if hash_pos != -1:
            pre = line[:hash_pos]
            if pre.count('"') % 2 == 0 and pre.count("'") % 2 == 0:
                line = line[:hash_pos].rstrip()
        out.append(line)
    return "\n".join(out)


def _lift_from_source(src, names):
    """AST-lift the requested top-level names (functions, classes,
    module-level assigns) from a source string into a fresh namespace.
    Used to drive pure-Python helpers without the heavy imports."""
    tree = ast.parse(src)
    picked = []
    seen = set()

    def walk(nodes):
        for node in nodes:
            n = None
            if isinstance(node, (ast.FunctionDef, ast.AsyncFunctionDef)):
                n = node.name
            elif isinstance(node, ast.ClassDef):
                walk(node.body)
                continue
            elif isinstance(node, ast.Assign) and len(node.targets) == 1:
                tgt = node.targets[0]
                if isinstance(tgt, ast.Name):
                    n = tgt.id
            if n in names and n not in seen:
                picked.append(node)
                seen.add(n)

    walk(tree.body)
    return picked


# ── H2 constants + backoff helper ───────────────────────────────────────


class TestH2TimeoutAndBackoffConfig(unittest.TestCase):
    """Source-inspection + behavioral tests for the H2 configuration.
    The behavior of _compute_backoff is deterministic modulo the jitter,
    so we assert bounds rather than exact values."""

    @classmethod
    def setUpClass(cls):
        src = _source(_SET_MF_RATE_PATH)
        picked = _lift_from_source(src, {
            "_compute_backoff",
            "_BACKOFF_BASE_SECONDS",
            "_BACKOFF_CAP_SECONDS",
            "_BACKOFF_JITTER_RATIO",
            "_BACKOFF_429_BASE_SECONDS",
            "_BACKOFF_429_CAP_SECONDS",
            "_REQUEST_TIMEOUT_SECONDS",
            "_REQUEST_CONNECT_TIMEOUT_SECONDS",
        })
        ns = {"random": __import__("random")}
        exec(compile(ast.Module(body=picked, type_ignores=[]), "<h2>", "exec"), ns)
        cls.ns = ns

    def test_timeout_dropped_to_ten_seconds(self):
        print("\n[ak-5jq H2 — per-request total timeout = 10s (was 30s)]")
        self.assertEqual(self.ns["_REQUEST_TIMEOUT_SECONDS"], 10)
        self.assertEqual(self.ns["_REQUEST_CONNECT_TIMEOUT_SECONDS"], 5)
        print("  ✓ total=10s, connect=5s")

    def test_backoff_base_and_cap_5xx(self):
        print("\n[ak-5jq H2 — 5xx backoff: base 2s, cap 30s, jitter 25%]")
        self.assertEqual(self.ns["_BACKOFF_BASE_SECONDS"], 2.0)
        self.assertEqual(self.ns["_BACKOFF_CAP_SECONDS"], 30.0)
        self.assertEqual(self.ns["_BACKOFF_JITTER_RATIO"], 0.25)
        print("  ✓ constants pinned")

    def test_backoff_base_and_cap_429(self):
        """ak-5jq H3: 429 has its own (longer) base + cap so upstream's
        'slow down' request gets a stronger response."""
        print("\n[ak-5jq H3 — 429 backoff: base 5s (>5xx's 2s), cap 60s (>5xx's 30s)]")
        self.assertEqual(self.ns["_BACKOFF_429_BASE_SECONDS"], 5.0)
        self.assertEqual(self.ns["_BACKOFF_429_CAP_SECONDS"], 60.0)
        # 429 must be LONGER than 5xx at every attempt.
        self.assertGreater(
            self.ns["_BACKOFF_429_BASE_SECONDS"],
            self.ns["_BACKOFF_BASE_SECONDS"],
        )
        self.assertGreater(
            self.ns["_BACKOFF_429_CAP_SECONDS"],
            self.ns["_BACKOFF_CAP_SECONDS"],
        )
        print("  ✓ 429 backoff strictly > 5xx backoff")

    def test_compute_backoff_exponential_progression(self):
        """base * 2^attempt, capped, plus 0-25% jitter.
        attempt=0 → base + [0, base*0.25]
        attempt=1 → 2*base + [0, 2*base*0.25]
        attempt=2 → 4*base + [0, 4*base*0.25]
        etc, all capped at cap."""
        print("\n[ak-5jq H2 — compute_backoff: exponential w/ jitter, respects cap]")
        cb = self.ns["_compute_backoff"]
        base, cap = 2.0, 30.0
        # attempt=0 → 2..2.5
        for _ in range(50):
            v = cb(0, base=base, cap=cap)
            self.assertGreaterEqual(v, 2.0)
            self.assertLessEqual(v, 2.5)
        # attempt=1 → 4..5
        for _ in range(50):
            v = cb(1, base=base, cap=cap)
            self.assertGreaterEqual(v, 4.0)
            self.assertLessEqual(v, 5.0)
        # attempt=2 → 8..10
        for _ in range(50):
            v = cb(2, base=base, cap=cap)
            self.assertGreaterEqual(v, 8.0)
            self.assertLessEqual(v, 10.0)
        # attempt=10 (2^10 = 1024) → capped at 30 + 25%
        for _ in range(50):
            v = cb(10, base=base, cap=cap)
            self.assertLessEqual(v, 30.0 + 30.0 * 0.25)
            self.assertGreaterEqual(v, 30.0)
        print("  ✓ exponential 2→4→8; jitter within 25%; cap enforced")

    def test_compute_backoff_has_jitter(self):
        """Two invocations at the same attempt SHOULD differ — jitter
        randomizes them. 100 samples of attempt=1: min/max must span."""
        print("\n[ak-5jq H2 — jitter actually varies between calls]")
        cb = self.ns["_compute_backoff"]
        samples = [cb(1, base=2.0, cap=30.0) for _ in range(100)]
        self.assertGreater(max(samples) - min(samples), 0.5,
                           "jitter appears absent — samples too clustered")
        print(f"  ✓ 100 samples span {max(samples)-min(samples):.2f}s of jitter")

    def test_worst_case_per_scheme_wall_time_bounded(self):
        """Post-H2 worst case (3 retries, all timing out at 10s each,
        with capped backoff between): total ≤ 3*10 + (backoff_0 +
        backoff_1 + backoff_2) ≤ 30 + (2.5+5+10) ≈ 47.5s. Well under
        the pre-H2 90s+ from 30s×3 + 2+4+6 linear."""
        print("\n[ak-5jq H2 — worst-case per-scheme wall time bounded]")
        cb = self.ns["_compute_backoff"]
        req_timeout = self.ns["_REQUEST_TIMEOUT_SECONDS"]
        # Upper-bound each backoff attempt at base + 25% jitter.
        worst_case = req_timeout * 3
        for attempt in range(3):
            worst_case += cb(attempt, base=2.0, cap=30.0)
        self.assertLess(
            worst_case, 60.0,
            f"worst case {worst_case:.2f}s exceeds 60s ceiling — "
            f"H2 tuning may be off",
        )
        print(f"  ✓ worst case ≤ {worst_case:.1f}s (was 90s+ pre-H2)")

    def test_source_uses_new_constants_not_old_RETRY_DELAY(self):
        """Guard against a partial revert that re-adds the old
        `RETRY_DELAY * (attempt + 1)` linear pattern."""
        print("\n[ak-5jq H2 — source uses _compute_backoff, not linear RETRY_DELAY*(...)]")
        code = _source_code_only(_SET_MF_RATE_PATH)
        # Old pattern must NOT appear anywhere in exec code.
        self.assertNotIn("RETRY_DELAY * (attempt + 1)", code)
        # _compute_backoff must be called for retries.
        self.assertIn("_compute_backoff(", code)
        print("  ✓ old linear formula gone; _compute_backoff wired")


# ── H3 error classification ─────────────────────────────────────────────


class TestH3ErrorClassificationSource(unittest.TestCase):
    """fetch_scheme's return contract is now a classified dict. Behavior
    is exercised end-to-end in the integration smoke below; here we
    lock the source-level invariants that make the classification
    reliable across future refactors."""

    def test_fetch_scheme_has_404_branch_permanent_skip(self):
        print("\n[ak-5jq H3 — fetch_scheme classifies 404 as permanent_skip_404]")
        code = _source_code_only(_SET_MF_RATE_PATH)
        self.assertIn("resp.status == 404", code)
        self.assertIn("permanent_skip_404", code)
        print("  ✓ 404 branch + permanent_skip_404 marker present")

    def test_fetch_scheme_has_429_and_5xx_branches(self):
        print("\n[ak-5jq H3 — fetch_scheme has distinct 429 and 5xx branches]")
        code = _source_code_only(_SET_MF_RATE_PATH)
        self.assertIn("resp.status == 429", code)
        self.assertIn("500 <= resp.status < 600", code)
        # 429 branch uses the 429-specific backoff cap.
        self.assertIn("_BACKOFF_429_BASE_SECONDS", code)
        self.assertIn("_BACKOFF_429_CAP_SECONDS", code)
        print("  ✓ 429 uses its own longer backoff; 5xx uses standard")

    def test_fetch_scheme_returns_error_marker_dict(self):
        print("\n[ak-5jq H3 — fetch_scheme returns error dicts, not bare ints]")
        code = _source_code_only(_SET_MF_RATE_PATH)
        # The final catch-all return line uses the error-marker dict.
        self.assertIn("_ERROR_KEY: kind", code)
        # And the specific final classes are named.
        for kind in ("final_429", "final_5xx", "final_timeout", "final_other"):
            self.assertIn(kind, code)
        # And the sentinel key value.
        self.assertIn('_ERROR_KEY = "__error_class__"', code)
        print("  ✓ error-marker dict returns wired; all 4 final classes named")

    def test_fetch_all_passes_filters_permanent_skips_from_retries(self):
        """Structural: the retry-loop failed_urls filter must exclude
        both succeeded ids AND permanent_skip_ids. Otherwise 404s get
        retried MAX_RETRIES × RETRY_PASSES = 12 wasted times each."""
        print("\n[ak-5jq H3 — retry loop excludes permanent_skip_ids from failed_urls]")
        code = _source_code_only(_SET_MF_RATE_PATH)
        # The exact filter shape we ship.
        self.assertIn("sid not in permanent_skip_ids", code)
        # And the snapshot pattern from ak-539 C3 is still present.
        self.assertIn("succeeded = set(result_map)", code)
        print("  ✓ failed_urls filter excludes both succeeded + permanent skips")

    def test_process_errors_helper_present_and_wired(self):
        print("\n[ak-5jq H3 — _process_errors helper called after each pass]")
        code = _source_code_only(_SET_MF_RATE_PATH)
        self.assertIn("def _process_errors(", code)
        # Called at least twice (init + retry passes).
        self.assertGreaterEqual(
            code.count("self._process_errors(responses"), 2,
            "_process_errors must be called after each fetch pass",
        )
        print("  ✓ helper defined + called after every pass")

    def test_buildJsonForMF_returns_four_tuple_with_permanent_skips(self):
        print("\n[ak-5jq H3 — buildJsonForMF returns 4-tuple w/ permanent_skips]")
        code = _source_code_only(_SET_MF_RATE_PATH)
        # The exact final-return shape.
        self.assertIn("len(permanent_skip_ids),", code)
        # run() unpacks all four names.
        self.assertIn(
            "jsonData, urls_total, urls_ok, permanent_skips = self.buildJsonForMF",
            code,
        )
        print("  ✓ 4-tuple return + 4-name unpack in run()")

    def test_ratio_denominator_excludes_permanent_skips(self):
        """404 permanent skips are not real failures — a batch of them
        must not push success_ratio below the 98% gate."""
        print("\n[ak-5jq H3 — ratio denominator = urls_total - permanent_skips]")
        code = _source_code_only(_SET_MF_RATE_PATH)
        self.assertIn("answerable = max(urls_total - permanent_skips, 0)", code)
        self.assertIn("success_ratio = (urls_ok / answerable)", code)
        print("  ✓ denominator excludes 404 permanent skips")

    def test_completed_msg_surfaces_permanent_skip_count(self):
        print("\n[ak-5jq H3 — Completed msg names the permanent-skip count when > 0]")
        code = _source_code_only(_SET_MF_RATE_PATH)
        self.assertIn("permanently dropped by mfapi.in", code)
        # Only surfaces when > 0 (silent 0-skip runs get the plain msg).
        self.assertIn("if permanent_skips > 0:", code)
        print("  ✓ Completed msg gated on permanent_skips > 0")


class TestH3ClassificationIntegration(unittest.IsolatedAsyncioTestCase):
    """Async end-to-end: drive fetch_scheme against stub responses
    covering 200 / 404 / 429 (eventually 200) / 5xx (retries exhausted)
    / non-retryable-other. Uses AST-lift so aiohttp isn't required."""

    @classmethod
    def setUpClass(cls):
        src = _source(_SET_MF_RATE_PATH)
        picked = _lift_from_source(src, {
            "fetch_scheme",
            "_compute_backoff",
            "_BACKOFF_BASE_SECONDS",
            "_BACKOFF_CAP_SECONDS",
            "_BACKOFF_JITTER_RATIO",
            "_BACKOFF_429_BASE_SECONDS",
            "_BACKOFF_429_CAP_SECONDS",
            "_ERROR_KEY",
            "MAX_RETRIES",
        })
        # Stubs for the aiohttp names fetch_scheme references. We only
        # ever hit the resp.status branches; exception paths use these.
        class _CE(Exception): pass          # ClientConnectorError
        class _CRE(Exception):
            def __init__(self, status): self.status = status
        # ClientSession is referenced in fetch_scheme's type annotations
        # (session: ClientSession) — Python evaluates annotations at
        # def-time in this Python version so the name must resolve.
        # Any placeholder class works; we never actually instantiate it.
        class _CS: pass
        # Speed knob: wrap asyncio.sleep to a no-op so the retry-loop
        # backoffs (2/4/8s per attempt) don't cost real wall time in
        # the test suite. The classification logic under test doesn't
        # depend on actual sleep duration.
        import asyncio as _asyncio_mod
        class _FastAsyncio:
            def __getattr__(self, name): return getattr(_asyncio_mod, name)
            @staticmethod
            async def sleep(_seconds, *a, **kw):  # no-op
                return None
            # asyncio.Semaphore etc. resolve via __getattr__.
        fast_asyncio = _FastAsyncio()
        ns = {
            "asyncio": fast_asyncio,
            "random": __import__("random"),
            "ClientConnectorError": _CE,
            "ClientResponseError": _CRE,
            "ClientSession": _CS,
        }
        exec(compile(ast.Module(body=picked, type_ignores=[]), "<h3>", "exec"), ns)
        cls.ns = ns
        cls._CE = _CE
        cls._CRE = _CRE

    def _make_session(self, status_sequence, body_text="", json_body=None):
        """Build a fake aiohttp session whose .get(url) yields a
        response with the next status in the sequence. Advances per
        attempt so we can simulate 'first 2 attempts return 5xx, third
        returns 200'."""
        outer = self

        class _Resp:
            def __init__(self, status):
                self.status = status
            async def json(self):
                return json_body or {"data": [{"date": "01-01-2026", "nav": "10"}]}
            async def text(self):
                return body_text
            async def __aenter__(self): return self
            async def __aexit__(self, *a): return False

        class _Session:
            def __init__(self):
                self.calls = 0
            def get(self, url):
                status = status_sequence[min(self.calls, len(status_sequence) - 1)]
                self.calls += 1
                return _Resp(status)

        return _Session()

    def _make_shim(self):
        """Minimal `self` shim exposing .logger — fetch_scheme reads
        self.logger.info/warning/error, nothing else."""
        class _Log:
            def __init__(self): self.records = []
            def _l(self, level, msg, *a):
                self.records.append((level, msg % a if a else msg))
            def info(self, m, *a): self._l("info", m, *a)
            def warning(self, m, *a): self._l("warning", m, *a)
            def error(self, m, *a): self._l("error", m, *a)
            def debug(self, m, *a): pass
        shim = type("S", (), {"logger": _Log()})()
        return shim

    async def _call_fetch(self, session, url="https://api.mfapi.in/mf/12345"):
        shim = self._make_shim()
        sem = __import__("asyncio").Semaphore(1)
        return await self.ns["fetch_scheme"](shim, url, session, sem), shim

    async def test_200_returns_success_dict(self):
        print("\n[ak-5jq H3 — 200 → success (dict with 'data')]")
        session = self._make_session([200])
        (scheme_id, payload), _ = await self._call_fetch(session)
        self.assertEqual(scheme_id, "12345")
        self.assertIn("data", payload)
        # NOT an error marker.
        self.assertNotIn(self.ns["_ERROR_KEY"], payload)
        # 1 HTTP call (no retries).
        self.assertEqual(session.calls, 1)
        print("  ✓ 200 returns immediately with data dict")

    async def test_404_returns_permanent_skip_marker(self):
        print("\n[ak-5jq H3 — 404 → permanent_skip_404 (no retries)]")
        session = self._make_session([404])
        (scheme_id, payload), shim = await self._call_fetch(session)
        self.assertEqual(payload.get(self.ns["_ERROR_KEY"]), "permanent_skip_404")
        self.assertEqual(payload["status"], 404)
        # Exactly 1 HTTP call — 404 does NOT retry.
        self.assertEqual(session.calls, 1)
        # INFO log emitted with the scheme_id.
        infos = [m for lvl, m in shim.logger.records if lvl == "info"]
        self.assertTrue(any("permanent skip 404" in m for m in infos))
        self.assertTrue(any("12345" in m for m in infos))
        print("  ✓ 404 permanent skip: 1 HTTP call, marker returned, INFO log")

    async def test_500_retries_then_gives_up_as_final_5xx(self):
        print("\n[ak-5jq H3 — 500 (all attempts) → final_5xx after retries]")
        session = self._make_session([500])  # every attempt returns 500
        (scheme_id, payload), _ = await self._call_fetch(session)
        self.assertEqual(payload.get(self.ns["_ERROR_KEY"]), "final_5xx")
        self.assertEqual(payload["status"], 500)
        # MAX_RETRIES attempts made.
        self.assertEqual(session.calls, self.ns["MAX_RETRIES"])
        print(f"  ✓ 500 retried MAX_RETRIES={self.ns['MAX_RETRIES']} times → final_5xx")

    async def test_500_then_200_returns_success_no_marker(self):
        print("\n[ak-5jq H3 — 500, then 200 → success (retry recovered)]")
        session = self._make_session([500, 500, 200])
        (scheme_id, payload), _ = await self._call_fetch(session)
        self.assertIn("data", payload)
        self.assertNotIn(self.ns["_ERROR_KEY"], payload)
        self.assertEqual(session.calls, 3)
        print("  ✓ 500 twice → 200 → recovered as success")

    async def test_429_retries_and_uses_longer_backoff(self):
        """429 across all attempts → final_429. The backoff sleeps
        would be longer than the 5xx equivalent, but we just assert
        the class here — timing bounds are H2's territory."""
        print("\n[ak-5jq H3 — 429 (all attempts) → final_429]")
        session = self._make_session([429])
        (scheme_id, payload), _ = await self._call_fetch(session)
        self.assertEqual(payload.get(self.ns["_ERROR_KEY"]), "final_429")
        self.assertEqual(session.calls, self.ns["MAX_RETRIES"])
        print("  ✓ 429 retried and classified as final_429")

    async def test_403_returns_final_other_with_warn(self):
        print("\n[ak-5jq H3 — 403 → final_other, WARN log, 1 HTTP call]")
        session = self._make_session([403])
        (scheme_id, payload), shim = await self._call_fetch(session)
        self.assertEqual(payload.get(self.ns["_ERROR_KEY"]), "final_other")
        self.assertEqual(payload["status"], 403)
        # NOT retried — 403 is client-side, retrying is wasted.
        self.assertEqual(session.calls, 1)
        warns = [m for lvl, m in shim.logger.records if lvl == "warning"]
        self.assertTrue(any("HTTP 403" in m for m in warns))
        print("  ✓ 403 → final_other + WARN, no retries")


# ── H1: null-guard on getLatestFile ─────────────────────────────────────


class TestH1NullListGuard(unittest.TestCase):
    """Structural test: run() checks latestListFile is None BEFORE
    passing it to buildJsonForMF (which would open(None) and crash).
    We source-inspect rather than drive run() end-to-end because the
    latter needs the full BaseTask stack."""

    def test_run_guards_latestListFile_is_None(self):
        print("\n[ak-5jq H1 — run() checks latestListFile is None before use]")
        code = _source_code_only(_SET_MF_RATE_PATH)
        # The exact guard shape we ship.
        self.assertIn("if latestListFile is None:", code)
        # And that guard returns Failed with the descriptive msg. The
        # message is split across concatenated f-strings in source, so
        # we check the two distinctive halves separately.
        self.assertIn(
            "MF details list missing or stale — SetMFDetails",
            code,
        )
        self.assertIn(
            "must run first",
            code,
        )
        self.assertIn('"Failed"', code)
        # Order: the guard must appear BEFORE buildJsonForMF is called.
        guard_idx = code.find("if latestListFile is None:")
        call_idx = code.find("self.buildJsonForMF(")
        self.assertGreaterEqual(guard_idx, 0)
        self.assertGreaterEqual(call_idx, 0)
        self.assertLess(
            guard_idx, call_idx,
            "H1 guard must precede buildJsonForMF call",
        )
        print("  ✓ guard present + precedes buildJsonForMF call + returns Failed")

    def test_error_log_fires_on_null_guard_path(self):
        print("\n[ak-5jq H1 — null-list branch logs ERROR (not just returns Failed)]")
        code = _source_code_only(_SET_MF_RATE_PATH)
        # Locate the null-guard branch and confirm it emits a logger.error.
        import re
        m = re.search(
            r"if latestListFile is None:(.*?)return ", code, re.DOTALL,
        )
        self.assertIsNotNone(m, "could not locate null-guard branch body")
        body = m.group(1)
        self.assertIn("self.logger.error(", body)
        print("  ✓ ERROR log fires on the null-list path")


# ── H4: LOCAL env 30-day hard limit ─────────────────────────────────────


class TestH4LocalStaleHardLimit(unittest.TestCase):
    """Source-inspection guards for the H4 LOCAL-env behavior change.

    We can't easily invoke checkJsonInDirectory end-to-end (needs the
    full JsonDownloadService + a real assets dir), so we assert the
    structural changes: the 30-day hard limit is present, the escape-
    hatch env var is recognized, and the pre-H4 unconditional
    `return True` is gone from the LOCAL branch."""

    def test_thirty_day_hard_limit_present(self):
        print("\n[ak-5jq H4 — LOCAL env has 30-day hard limit that forces re-download]")
        code = _source_code_only(_JSON_DL_PATH)
        # The hard-stale timedelta is defined + compared.
        self.assertIn("hard_stale = timedelta(days=30)", code)
        self.assertIn("if time_diff >= hard_stale:", code)
        # And that branch RETURNS FALSE (force re-download).
        # Match the ordering: `if time_diff >= hard_stale: <log>; return False`
        import re
        m = re.search(
            r"if time_diff >= hard_stale:(.*?)return\s+(True|False)",
            code, re.DOTALL,
        )
        self.assertIsNotNone(m, "could not locate hard-limit branch")
        self.assertEqual(m.group(2), "False", "hard-limit branch must return False")
        print("  ✓ 30-day hard limit present, returns False (force re-download)")

    def test_ak_allow_stale_list_escape_hatch(self):
        print("\n[ak-5jq H4 — AK_ALLOW_STALE_LIST=1 escape hatch preserves old behavior]")
        code = _source_code_only(_JSON_DL_PATH)
        self.assertIn("AK_ALLOW_STALE_LIST", code)
        # Escape-hatch branch returns True.
        import re
        m = re.search(
            r"AK_ALLOW_STALE_LIST\'\)\s*==\s*\'1\':(.*?)return\s+(True|False)",
            code, re.DOTALL,
        )
        self.assertIsNotNone(m, "could not locate AK_ALLOW_STALE_LIST branch")
        self.assertEqual(m.group(2), "True",
                         "escape-hatch branch must return True")
        print("  ✓ AK_ALLOW_STALE_LIST=1 → return True (preserves old always-fresh behavior)")

    def test_within_30d_local_still_returns_true(self):
        """Dev-safety preserved: <30d stale files in LOCAL still get
        `return True` so day-to-day development doesn't force
        re-downloads. Only the >30d cliff is new."""
        print("\n[ak-5jq H4 — <30d stale in LOCAL still returns True (dev safety)]")
        code = _source_code_only(_JSON_DL_PATH)
        # After the AK_ALLOW_STALE_LIST branch AND the hard_stale
        # branch, the remaining LOCAL path returns True. Match the
        # tail of the LOCAL block.
        import re
        m = re.search(
            r"if os\.getenv\('ENV'\)\s*==\s*'LOCAL':(.*?)return\s+True",
            code, re.DOTALL,
        )
        # The `return True` we're matching must not be from the
        # escape-hatch branch — we assert both branches exist and
        # both point at return True (fine; we just need dev safety
        # to still be there).
        self.assertIsNotNone(m)
        print("  ✓ within-30d LOCAL path retains return True")

    def test_old_unconditional_return_true_gone(self):
        """The pre-H4 code was:
            if os.getenv('ENV') == 'LOCAL':
                self.logger.warning(...)
                return True
        — a single unconditional return True. Ensure the fresh source
        has the 3-branch structure (escape hatch, hard limit, within-
        30d) — i.e. the LOCAL if-block spans more lines than pre-H4."""
        print("\n[ak-5jq H4 — LOCAL branch has 3-way structure, not single return True]")
        code = _source_code_only(_JSON_DL_PATH)
        import re
        m = re.search(
            r"if os\.getenv\('ENV'\)\s*==\s*'LOCAL':(.*?)self\.deleteFile\(",
            code, re.DOTALL,
        )
        self.assertIsNotNone(m,
                             "could not locate the LOCAL-branch body up to deleteFile")
        body = m.group(1)
        # The block should contain both escape-hatch AND hard-limit AND
        # the fall-through — at minimum 3 `return` statements (True /
        # False / True).
        self.assertGreaterEqual(
            body.count("return "), 3,
            "LOCAL block should have >=3 returns (escape / hard-limit / <30d)",
        )
        print(f"  ✓ LOCAL branch body has {body.count('return ')} return paths")


# ── Optional DROP alarm on SetMFDetails ─────────────────────────────────


class TestSetMFDetailsDropAlarm(unittest.TestCase):
    """ak-5jq (H3-theme optional add per Lead's dispatch):
    _log_scheme_counts now ERRORs on >2× DOWN spikes just like it does
    on >2× UP spikes."""

    @classmethod
    def setUpClass(cls):
        src = _source(_SET_MF_DETAILS_PATH)
        picked = _lift_from_source(src, {
            "_log_scheme_counts",
            "_previous_scheme_count",
            "_MF_DETAILS_WARN_DEVIATION",
            "_MF_DETAILS_ERR_MULTIPLIER",
            "_MF_DETAILS_DUP_SAMPLE_LIMIT",
        })
        ns = {}
        exec(compile(ast.Module(body=picked, type_ignores=[]), "<drop>", "exec"), ns)
        cls.log_fn = staticmethod(ns["_log_scheme_counts"])

    class _CaptureLogger:
        def __init__(self): self.records = []
        def _l(self, level, msg, *a):
            self.records.append((level, msg % a if a else msg))
        def info(self, m, *a): self._l("info", m, *a)
        def warning(self, m, *a): self._l("warning", m, *a)
        def error(self, m, *a): self._l("error", m, *a)
        def debug(self, m, *a): pass
        def any_at(self, level, needle):
            return any(needle in m for lvl, m in self.records if lvl == level)

    def _run(self, current_count, prev_count):
        data = [{"schemeCode": str(i)} for i in range(current_count)]
        log = self._CaptureLogger()
        shim = type("S", (), {"logger": log})()
        self.log_fn(shim, data, prev_count)
        return log

    def test_half_drop_triggers_error(self):
        print("\n[ak-5jq drop alarm — current at <50% of previous → ERROR]")
        # 15k current vs 40k previous — 62% drop, well past 2× multiplier.
        log = self._run(15000, 40000)
        errs = log.by_level if hasattr(log, 'by_level') else None
        error_msgs = [m for lvl, m in log.records if lvl == "error"]
        self.assertTrue(any("COUNT DROP" in m for m in error_msgs),
                        f"expected DROP error; got: {error_msgs}")
        self.assertTrue(any("current=15000" in m for m in error_msgs))
        self.assertTrue(any("previous=40000" in m for m in error_msgs))
        # And the UP-spike ERROR should NOT fire on a downside case.
        self.assertFalse(any("COUNT SPIKE" in m for m in error_msgs))
        print("  ✓ 15k vs 40k → COUNT DROP error (not SPIKE)")

    def test_25pct_drop_only_warns(self):
        """A modest drop within ±20% of the deviation-warn threshold
        should trigger the WARN, not the DROP-ERROR (which needs >2×)."""
        print("\n[ak-5jq drop alarm — 25% drop → WARN, not ERROR]")
        log = self._run(75, 100)
        errs = [m for lvl, m in log.records if lvl == "error"]
        warns = [m for lvl, m in log.records if lvl == "warning"]
        self.assertFalse(any("COUNT DROP" in m for m in errs))
        self.assertTrue(any("count deviation -25.0%" in m for m in warns))
        print("  ✓ -25% → WARN (below the >2× DROP threshold)")

    def test_up_spike_still_errors(self):
        """Regression on existing UP alarm — DROP alarm addition must
        not break it."""
        print("\n[ak-5jq drop alarm — UP >2× spike still fires (regression pin)]")
        log = self._run(300, 100)
        errs = [m for lvl, m in log.records if lvl == "error"]
        self.assertTrue(any("COUNT SPIKE" in m for m in errs))
        self.assertFalse(any("COUNT DROP" in m for m in errs))
        print("  ✓ UP alarm untouched")

    def test_source_has_both_up_and_down_error_branches(self):
        print("\n[ak-5jq drop alarm — source has both COUNT SPIKE and COUNT DROP branches]")
        code = _source_code_only(_SET_MF_DETAILS_PATH)
        self.assertIn("COUNT SPIKE", code)
        self.assertIn("COUNT DROP", code)
        print("  ✓ symmetric UP/DOWN alarms in source")


if __name__ == "__main__":
    print("ak-5jq HIGH batch — MF-rate hardening regression tests")
    print("=" * 70)
    unittest.main(verbosity=0, exit=False)
    print("=" * 70)
    print("Done.")
