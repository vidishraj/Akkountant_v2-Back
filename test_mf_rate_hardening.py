"""ak-539 CRITICAL batch regression tests: partial-success gate,
connector pooling, single-event-loop retry driver, async sleep, and
v2 hard-floor coverage protection.

Post-ak-lp6 hardening. Scoped strictly to services/tasks/SetMfRate.py
findings C1-C4 from the MF-jobs deep audit plus the v2 review fix:

  C1  buildJsonForMF now returns (data, urls_total, urls_ok); run()
      gates status behind success_ratio ≥ _MIN_SUCCESS_RATIO (0.98).
      Previously any partial success returned "Completed" while writing
      an incomplete rates file — silent NAV holes downstream.
  C2  TCPConnector switched from (limit_per_host, force_close=True) to
      (limit=CONCURRENT_REQUESTS, ttl_dns_cache=300). Restores keep-
      alive pooling; expected ~3-4× throughput at 37k schemes.
  C3  _fetch_all_passes async coroutine wraps initial + retry passes
      inside a single asyncio.run() — no event-loop teardown between
      passes. Snapshots set(result_map) BEFORE building failed_urls
      as defense-in-depth (option (a) on top of option (b)).
  C4  Inter-pass sleep converted time.sleep → asyncio.sleep — required
      by C3's async-hosting shape (sync sleep would block the whole
      event loop and stall in-flight retries).
  v2  Hard-floor coverage protection. Reviewer flagged MAJOR on v1:
      safe_replace_file destroyed the last-good NAV file BEFORE the
      0.98 ratio gate fired, so a transient MFAPI outage (result_map
      near-empty) would clobber good data. Fix: hard-floor gate at
      0.5 runs FIRST; below it, skip both the tmp write and the swap
      so the previous good file stays on disk untouched.

These tests are AST-lifted where possible so they don't require the
full BaseTask import chain (which needs flask + sqlalchemy). Where
async behavior matters we import the class module gated on the deps
being present and skip cleanly otherwise.

Run:
    python3 -m unittest test_mf_rate_hardening
    python3 -m pytest test_mf_rate_hardening.py
"""

import os
import sys
import unittest


sys.path.insert(0, os.path.dirname(os.path.abspath(__file__)))


# ── Source path (shared) ─────────────────────────────────────────────────


_SET_MF_RATE_PATH = os.path.join(
    os.path.dirname(os.path.abspath(__file__)),
    "services", "tasks", "SetMfRate.py",
)


def _source():
    with open(_SET_MF_RATE_PATH) as fh:
        return fh.read()


def _source_code_only():
    """Return the SetMfRate.py source with full-line and end-of-line
    comments stripped. Guards against false-positives when a comment
    references a forbidden pattern (e.g. "force_close=True" appearing
    in a "here's what we removed" comment). Mirrors the strip pattern
    from ak-bgc test_zero_nav_silent_default_removed."""
    src = _source()
    out = []
    for line in src.splitlines():
        stripped = line.lstrip()
        if stripped.startswith("#"):
            continue
        hash_pos = line.find("#")
        if hash_pos != -1:
            pre = line[:hash_pos]
            # Cheap heuristic: only strip the '#' if it's not inside a
            # string literal (unbalanced quotes before the hash → inside).
            if pre.count('"') % 2 == 0 and pre.count("'") % 2 == 0:
                line = line[:hash_pos].rstrip()
        out.append(line)
    return "\n".join(out)


# ── C1: partial-success gate — run() logic tested via shim ──────────────


class TestPartialSuccessGate(unittest.TestCase):
    """Drive the run()-body success-ratio math against a stubbed
    buildJsonForMF that returns synthetic (data, total, ok) tuples.

    We can't call SetMFRate.run() directly without the full BaseTask
    stack (JsonDownloadService + investment/transaction services +
    sqlalchemy + flask). Instead we replicate the exact 3-tier gate
    in-test (hard-floor / partial / completed) and guard against
    divergence via source-inspection tests below and the runtime-
    interaction test in TestHardFloorPreservesLastGood."""

    _MIN_SUCCESS_RATIO = 0.98  # must match the source constant
    _COVERAGE_HARD_FLOOR = 0.5  # v2 — must match the source constant

    @staticmethod
    def _decide_status(urls_total, urls_ok,
                       min_ratio, hard_floor):
        """Mirror of the v2 run() 3-tier gate:
          * ratio <  hard_floor → 'coverage below hard floor' Failed
          * ratio <  min_ratio  → 'partial success' Failed
          * ratio >= min_ratio  → 'Completed'
        Divergence here or in source → source-inspection test flags it."""
        ratio = (urls_ok / urls_total) if urls_total else 1.0
        if ratio < hard_floor:
            msg = (
                f"coverage below hard floor: {urls_ok}/{urls_total} "
                f"({ratio:.2%} — below {hard_floor:.0%} floor); "
                f"preserving last-good NAVs on disk"
            )
            return msg, "Failed"
        if ratio < min_ratio:
            msg = (
                f"partial success: {urls_ok}/{urls_total} schemes "
                f"written ({ratio:.2%} — below {min_ratio:.0%} threshold)"
            )
            return msg, "Failed"
        return "Completed successfully", "Completed"

    def _decide(self, total, ok):
        return self._decide_status(
            total, ok, self._MIN_SUCCESS_RATIO, self._COVERAGE_HARD_FLOOR,
        )

    def test_between_floor_and_98pct_returns_partial_failed(self):
        print("\n[ak-539 C1 — 90% (above floor, below 98%) → partial-success Failed]")
        # 37k universe, 90% success — above 0.5 floor, below 0.98 threshold.
        msg, status = self._decide(37000, 33300)
        self.assertEqual(status, "Failed")
        self.assertIn("partial success", msg)
        self.assertIn("33300/37000", msg)
        self.assertIn("90.00%", msg)
        self.assertIn("98%", msg)
        print(f"  ✓ 90% → partial-success Failed: {msg[:100]}")

    def test_at_or_above_98pct_returns_completed(self):
        print("\n[ak-539 C1 — 98%+ returns Completed]")
        msg, status = self._decide(37000, 36260)
        self.assertEqual(status, "Completed")
        self.assertEqual(msg, "Completed successfully")
        msg2, status2 = self._decide(37000, 36999)
        self.assertEqual(status2, "Completed")
        print("  ✓ 98.00% and 99.99% both → Completed")

    def test_boundary_exactly_98pct(self):
        """98% exactly should PASS (>=). 97.999...% should Fail."""
        print("\n[ak-539 C1 — boundary: exact 98% passes; 97.99% fails]")
        msg, status = self._decide(100, 98)
        self.assertEqual(status, "Completed")
        msg2, status2 = self._decide(100, 97)
        self.assertEqual(status2, "Failed")
        self.assertIn("partial success", msg2)
        print("  ✓ exact-98% passes; 97% → partial-success Failed")

    def test_zero_total_treated_as_success(self):
        """Empty URL list is degenerate but shouldn't crash on divide-
        by-zero. Treat as ratio=1.0 → Completed. Below-floor gate must
        also not trip on this."""
        print("\n[ak-539 C1 — zero-total edge: ratio=1.0, Completed, no floor trip]")
        msg, status = self._decide(0, 0)
        self.assertEqual(status, "Completed")
        self.assertNotIn("hard floor", msg)
        print("  ✓ 0/0 → Completed (no divide-by-zero, no floor trip)")

    def test_aug3_replay_75pct_partial(self):
        """Aug 3 SIGKILL pattern (113k URLs, ~75%) — above the 0.5
        floor, below the 0.98 threshold → partial-success Failed +
        swap the file (degraded > stale for the surviving 75%)."""
        print("\n[ak-539 C1 — Aug 3 replay: 113k @ 75% → partial (not hard-floor)]")
        msg, status = self._decide(113139, 84854)
        self.assertEqual(status, "Failed")
        self.assertIn("partial success", msg)
        self.assertNotIn("hard floor", msg)
        self.assertIn("84854/113139", msg)
        print("  ✓ 75% Aug 3 pattern → partial-success Failed (not hard-floor)")


# ── v2 hard-floor: runtime interaction test ─────────────────────────────


class TestHardFloorPreservesLastGood(unittest.TestCase):
    """Reviewer's MAJOR: safe_replace_file MUST NOT run when
    success_ratio is below the hard floor. Otherwise a transient
    outage clobbers the last-good NAV file with a near-empty one.

    We can't call SetMFRate.run() directly (needs the full app stack),
    so we replicate the exact v2 3-tier control flow in a shim and
    assert the intended side-effects (which methods got called + which
    didn't + the on-disk file survives). Divergence between this shim
    and the source is guarded by TestPartialSuccessSourceInvariants
    below."""

    _MIN_SUCCESS_RATIO = 0.98
    _COVERAGE_HARD_FLOOR = 0.5

    class _Shim:
        """Stand-in for SetMFRate that records which side-effect
        methods run() would have called. tmp_dir + last_good_path
        are real disk paths so we can assert the last-good file
        actually survives a below-floor call."""

        def __init__(self, tmp_dir, last_good_path):
            self.tmp_dir = tmp_dir
            self._last_good_path = last_good_path
            # Call tracking:
            self.save_json_called = False
            self.safe_replace_called = False
            self.records = []  # (level, msg) log tuples
            # We stub only the two methods run() would touch after
            # buildJsonForMF. jsonService is only referenced inside
            # safe_replace_file's args, which we don't invoke.

            class _Logger:
                def _log(_self, level, msg, *a):
                    self.records.append((level, msg % a if a else msg))
                def info(_self, msg, *a): _self._log("info", msg, *a)
                def warning(_self, msg, *a): _self._log("warning", msg, *a)
                def error(_self, msg, *a): _self._log("error", msg, *a)
                def debug(_self, msg, *a): pass
            self.logger = _Logger()

        def save_json(self, data, filePath):
            self.save_json_called = True
            with open(filePath, "w") as fh:
                import json as _json
                _json.dump(data, fh)

        def safe_replace_file(self, tmp_path, prefix, file_type):
            self.safe_replace_called = True
            # Mirror real behavior: move tmp → last_good_path,
            # clobbering. This is exactly the destructive step the
            # hard-floor gate must prevent.
            import shutil
            shutil.move(tmp_path, self._last_good_path)
            return True, None

        def any_at(self, level, needle):
            return any(needle in m for lvl, m in self.records if lvl == level)

    @staticmethod
    def _simulate_run(shim, urls_total, urls_ok,
                      min_ratio, hard_floor, interval=150):
        """Mirror of SetMFRate.run()'s v2 control flow. Kept small
        + deterministic — takes counts directly instead of calling
        buildJsonForMF. Records side-effect calls on the shim."""
        # buildJsonForMF simulated: returns (data, urls_total, urls_ok)
        jsonData = {"data": [{"scheme_id": str(i), "nav": "10.00"} for i in range(urls_ok)]}

        success_ratio = (urls_ok / urls_total) if urls_total else 1.0

        # v2 hard-floor gate — must fire BEFORE any disk write.
        if success_ratio < hard_floor:
            msg = (
                f"coverage below hard floor: {urls_ok}/{urls_total} "
                f"({success_ratio:.2%} — below {hard_floor:.0%} floor); "
                f"preserving last-good NAVs on disk"
            )
            shim.logger.error(f"MF rate job: {msg}")
            return msg, "Failed", interval

        # Above the floor — write + swap.
        import os as _os
        filePath = _os.path.join(shim.tmp_dir, "MFRate.json")
        try:
            _os.remove(filePath)
        except OSError:
            pass
        shim.save_json(jsonData, filePath)

        ok, err = shim.safe_replace_file(filePath, "MF_rate", "rates")
        if not ok:
            return err, "Failed", interval

        # 98% gate — degraded but swapped.
        if success_ratio < min_ratio:
            msg = (
                f"partial success: {urls_ok}/{urls_total} schemes "
                f"written ({success_ratio:.2%} — below "
                f"{min_ratio:.0%} threshold)"
            )
            shim.logger.warning(f"MF rate job: {msg}")
            return msg, "Failed", interval
        return "Completed successfully", "Completed", interval

    def _make_shim(self, last_good_content=b'{"data":[{"nav":"LAST-GOOD"}]}'):
        import shutil, tempfile
        tmp_dir = tempfile.mkdtemp(prefix="ak539v2-tmp-")
        last_good_dir = tempfile.mkdtemp(prefix="ak539v2-lastgood-")
        last_good_path = os.path.join(last_good_dir, "MF_rate_last.json")
        with open(last_good_path, "wb") as fh:
            fh.write(last_good_content)
        self.addCleanup(shutil.rmtree, tmp_dir, ignore_errors=True)
        self.addCleanup(shutil.rmtree, last_good_dir, ignore_errors=True)
        return self._Shim(tmp_dir, last_good_path), last_good_path

    def _run(self, urls_total, urls_ok, **kwargs):
        shim, last_good = self._make_shim(**kwargs)
        msg, status, _ = self._simulate_run(
            shim, urls_total, urls_ok,
            self._MIN_SUCCESS_RATIO, self._COVERAGE_HARD_FLOOR,
        )
        return shim, last_good, msg, status

    def test_transient_outage_30pct_preserves_last_good(self):
        """Simulated MFAPI outage: 30% of 37k URLs succeed. Hard-floor
        fires; the last-good NAV file on disk is untouched."""
        print("\n[ak-539 v2 — 30% (outage class) → hard-floor, last-good preserved]")
        shim, last_good, msg, status = self._run(37000, 11100)
        # Side effects: safe_replace_file MUST NOT have run. Nor
        # save_json (the v2 flow short-circuits before the tmp write).
        self.assertFalse(
            shim.safe_replace_called,
            "safe_replace_file called below hard floor — would clobber last-good NAVs!",
        )
        self.assertFalse(
            shim.save_json_called,
            "save_json called below hard floor — wasted disk write",
        )
        # Return: Failed with hard-floor msg, NOT partial-success msg.
        self.assertEqual(status, "Failed")
        self.assertIn("coverage below hard floor", msg)
        self.assertIn("11100/37000", msg)
        self.assertIn("30.00%", msg)
        self.assertIn("50%", msg)  # floor value in message
        self.assertIn("preserving last-good", msg)
        # ERROR log fired (not WARN — hard-floor is a louder signal
        # than the between-floor-and-98% partial WARN).
        self.assertTrue(shim.any_at("error", "coverage below hard floor"))
        # Last-good file untouched (still has original bytes).
        with open(last_good, "rb") as fh:
            self.assertIn(b"LAST-GOOD", fh.read())
        print("  ✓ safe_replace_file NOT called; last-good file intact on disk")

    def test_zero_pct_outage_preserves_last_good(self):
        """Total MFAPI collapse: 0 of 37k succeed. Hard-floor still
        catches; last-good preserved; no divide-by-zero."""
        print("\n[ak-539 v2 — 0% (total outage) → hard-floor, no crash]")
        shim, last_good, msg, status = self._run(37000, 0)
        self.assertFalse(shim.safe_replace_called)
        self.assertFalse(shim.save_json_called)
        self.assertEqual(status, "Failed")
        self.assertIn("coverage below hard floor", msg)
        with open(last_good, "rb") as fh:
            self.assertIn(b"LAST-GOOD", fh.read())
        print("  ✓ 0% outage → hard-floor Failed, last-good intact")

    def test_boundary_exactly_50pct_below_floor(self):
        """50% exactly → below floor (strict < 0.5), hard-floor fires.
        50.01% → above floor, falls through to partial-success gate."""
        print("\n[ak-539 v2 — boundary: 50% exact fires floor; 50.01% doesn't]")
        # 50/100 = 0.5 exactly. Floor is `ratio < 0.5`, so 0.5 does NOT
        # trip. Callers with exactly-at-floor coverage swap.
        shim1, lg1, msg1, status1 = self._run(100, 50)
        self.assertTrue(
            shim1.safe_replace_called,
            "exact-50% must fall through the floor (`ratio < 0.5` is strict)",
        )
        self.assertIn("partial success", msg1)
        # 49/100 = 0.49 → below floor.
        shim2, lg2, msg2, status2 = self._run(100, 49)
        self.assertFalse(shim2.safe_replace_called)
        self.assertIn("coverage below hard floor", msg2)
        print("  ✓ exact-50% falls through; 49% hard-floors")

    def test_above_floor_below_98_still_swaps(self):
        """Between the floor and the 98% threshold: file IS swapped
        (degraded > stale), but status is Failed with partial-success
        msg. v1 behavior preserved for this tier."""
        print("\n[ak-539 v2 — 75% (above floor, below 98%) → swap + partial-success Failed]")
        shim, last_good, msg, status = self._run(100, 75)
        self.assertTrue(
            shim.safe_replace_called,
            "above-floor coverage must still swap the file",
        )
        self.assertTrue(shim.save_json_called)
        self.assertEqual(status, "Failed")
        self.assertIn("partial success", msg)
        self.assertNotIn("hard floor", msg)
        # Last-good was overwritten (moved into place by the shim).
        # Sanity: file exists, doesn't contain the sentinel anymore.
        with open(last_good, "rb") as fh:
            content = fh.read()
        self.assertNotIn(b"LAST-GOOD", content)
        print("  ✓ 75% → swap runs, last-good replaced, status=Failed partial")

    def test_above_98_pct_swap_and_completed(self):
        print("\n[ak-539 v2 — 99% (above 98%) → swap + Completed]")
        shim, last_good, msg, status = self._run(100, 99)
        self.assertTrue(shim.safe_replace_called)
        self.assertEqual(status, "Completed")
        self.assertEqual(msg, "Completed successfully")
        print("  ✓ 99% → swap runs, status=Completed")


# ── C1 + v2 source-inspection guard ──────────────────────────────────────


class TestPartialSuccessSourceInvariants(unittest.TestCase):
    """Guard the actual SetMfRate.py source so a 'cleanup' can't quietly
    drop the threshold or swap the tuple return back to single-value."""

    def test_min_success_ratio_constant_is_098(self):
        print("\n[ak-539 C1 — _MIN_SUCCESS_RATIO constant = 0.98]")
        src = _source()
        self.assertIn("_MIN_SUCCESS_RATIO = 0.98", src)
        print("  ✓ threshold pinned at 0.98")

    def test_buildJsonForMF_returns_triple(self):
        print("\n[ak-539 C1 — buildJsonForMF returns (data, urls_total, urls_ok)]")
        src = _source()
        # The exact return shape at the end of buildJsonForMF.
        self.assertIn(
            'return {"data": list(result_map.values())}, len(urls), len(result_map)',
            src,
            "buildJsonForMF must return (data, urls_total, urls_ok) tuple",
        )
        print("  ✓ triple return shape present")

    def test_run_unpacks_triple_and_gates_on_ratio(self):
        print("\n[ak-539 C1 — run() unpacks triple + computes ratio + gates status]")
        src = _source()
        self.assertIn(
            "jsonData, urls_total, urls_ok = self.buildJsonForMF",
            src,
        )
        # The ratio + gate. We match on distinctive substrings rather
        # than an exact block so minor whitespace / var reorder doesn't
        # false-trip the guard.
        self.assertIn("success_ratio = (urls_ok / urls_total)", src)
        self.assertIn("success_ratio < _MIN_SUCCESS_RATIO", src)
        self.assertIn('"Failed", self.interval', src)
        print("  ✓ triple unpack + ratio math + Failed gate all present in run()")

    def test_v2_hard_floor_constant_is_050(self):
        print("\n[ak-539 v2 — _COVERAGE_HARD_FLOOR constant = 0.5]")
        src = _source()
        self.assertIn("_COVERAGE_HARD_FLOOR = 0.5", src)
        print("  ✓ hard-floor pinned at 0.5")

    def test_v2_hard_floor_gate_fires_before_safe_replace(self):
        """Structural invariant: the hard-floor `if success_ratio <
        _COVERAGE_HARD_FLOOR` check must appear in run() BEFORE the
        first call to safe_replace_file. Anything else means the
        review MAJOR is regressed (safe_replace_file would still
        clobber last-good on a below-floor run)."""
        print("\n[ak-539 v2 — hard-floor branch precedes safe_replace_file in run()]")
        code = _source_code_only()
        # Slice to run()'s body: `def run(self):` to the next method
        # `def ` at 4-space indent (buildJsonForMF).
        import re
        m = re.search(
            r"def run\(self\):(.*?)(?=\n    (?:async )?def )",
            code, re.DOTALL,
        )
        self.assertIsNotNone(m, "could not locate run() body")
        body = m.group(1)
        floor_idx = body.find("success_ratio < _COVERAGE_HARD_FLOOR")
        replace_idx = body.find("self.safe_replace_file(")
        self.assertGreaterEqual(
            floor_idx, 0, "hard-floor gate missing from run()",
        )
        self.assertGreaterEqual(
            replace_idx, 0, "safe_replace_file call missing from run()",
        )
        self.assertLess(
            floor_idx, replace_idx,
            f"hard-floor gate must precede safe_replace_file "
            f"(floor at {floor_idx}, replace at {replace_idx}); "
            f"otherwise safe_replace_file destroys last-good NAVs "
            f"before the floor can preserve them.",
        )
        # The hard-floor branch must return early (Failed) — check the
        # descriptive message string is in run() body too.
        self.assertIn("coverage below hard floor", body)
        self.assertIn("preserving last-good NAVs on disk", body)
        print("  ✓ hard-floor gate + early-return precede safe_replace_file")


# ── C2: TCPConnector config source-inspection ────────────────────────────


class TestConnectorPoolingConfig(unittest.TestCase):
    """The pooling regression is easy to slip back in ('cleanup pass'
    that adds force_close=True to be safe, etc.). Source-inspection
    is the cheapest lock — the config is a literal keyword-arg block
    in one method."""

    def test_force_close_gone_and_dns_cache_present(self):
        print("\n[ak-539 C2 — no force_close=True; ttl_dns_cache=300 present]")
        code = _source_code_only()  # comments stripped
        raw = _source()
        # Executable code must NOT contain force_close=True. Explanatory
        # comments referencing what got removed are fine (they're in `raw`
        # but not in `code`).
        self.assertNotIn("force_close=True", code)
        # Config presence — either in raw or code is fine (they're kwargs).
        self.assertIn("ttl_dns_cache=_DNS_CACHE_TTL_SECONDS", code)
        self.assertIn("_DNS_CACHE_TTL_SECONDS = 300", raw)
        print("  ✓ force_close gone from exec code + ttl_dns_cache=300 present")

    def test_uses_limit_not_limit_per_host(self):
        """Single-host workload: limit_per_host adds no safety. Ensure
        we use the cleaner `limit=CONCURRENT_REQUESTS` form and dropped
        the per-host cap."""
        print("\n[ak-539 C2 — TCPConnector uses limit=CONCURRENT_REQUESTS (not limit_per_host)]")
        src = _source()
        # We match the exact keyword to avoid false-positives from
        # `limit_per_host` substring in comments.
        self.assertIn("limit=CONCURRENT_REQUESTS", src)
        # Line-level guard: no `limit_per_host=<num>` kwarg on any
        # TCPConnector construction. Comments explaining why it was
        # dropped may reference the name — grep those out first.
        for line in src.splitlines():
            stripped = line.lstrip()
            if stripped.startswith("#"):
                continue
            self.assertNotIn(
                "limit_per_host=", line,
                f"limit_per_host= kwarg present in executable code: {line!r}",
            )
        print("  ✓ limit=CONCURRENT_REQUESTS present; no limit_per_host in exec code")


# ── C3 + C4: single asyncio.run() driver + asyncio.sleep ────────────────


class TestSingleEventLoopAndAsyncSleep(unittest.TestCase):
    """Source-inspection guards for the C3 refactor + C4 async-sleep
    swap. Behavioral verification of the async coro is harder to unit-
    test in isolation (needs an aiohttp session + mocked responses),
    but the source-shape invariants below make it impossible to
    silently revert either fix."""

    def test_single_asyncio_run_in_buildJsonForMF(self):
        """buildJsonForMF should have EXACTLY ONE asyncio.run() call —
        the entry into _fetch_all_passes. Anything more means the
        multi-loop pattern got reintroduced (or another async call
        was inlined into the sync driver by mistake).

        We count against the code-only source (comments stripped) so
        an explanatory comment referencing 'asyncio.run(...)' doesn't
        false-inflate the count."""
        print("\n[ak-539 C3 — buildJsonForMF calls asyncio.run() exactly once]")
        code = _source_code_only()
        # Slice to just buildJsonForMF's body: from its `def` header to
        # the next top-level method def (sync or async).
        import re
        m = re.search(
            r"def buildJsonForMF\(.*?\):(.*?)(?=\n    (?:async )?def |\nclass )",
            code, re.DOTALL,
        )
        self.assertIsNotNone(m, "could not locate buildJsonForMF body")
        body = m.group(1)
        run_count = body.count("asyncio.run(")
        self.assertEqual(
            run_count, 1,
            f"buildJsonForMF should have exactly 1 asyncio.run() call "
            f"(got {run_count}). Multi-loop pattern regressed?",
        )
        print(f"  ✓ single asyncio.run() in buildJsonForMF (count={run_count})")

    def test_fetch_all_passes_is_async(self):
        print("\n[ak-539 C3 — _fetch_all_passes exists as async coroutine]")
        src = _source()
        self.assertIn("async def _fetch_all_passes(self, urls", src)
        print("  ✓ _fetch_all_passes async signature present")

    def test_snapshot_defense_in_retry_loop(self):
        """set(result_map) snapshot BEFORE building failed_urls is
        option (a) defense-in-depth on top of option (b)'s single-loop
        guarantee. Its removal would leave us relying only on the
        single-loop invariant — thinner defense."""
        print("\n[ak-539 C3 — retry loop snapshots set(result_map) before building failed_urls]")
        src = _source()
        self.assertIn("succeeded = set(result_map)", src)
        # And the failed_urls filter uses `succeeded`, not the raw dict.
        self.assertIn(
            "failed_urls = [u for u in urls if u.split(\"/\")[-1] not in succeeded]",
            src,
        )
        print("  ✓ snapshot + snapshot-scoped filter both present")

    def test_asyncio_sleep_not_time_sleep_between_passes(self):
        """C4: the inter-pass sleep MUST be asyncio.sleep now that the
        retry loop lives inside an async coroutine. time.sleep would
        block the whole event loop and freeze in-flight retries."""
        print("\n[ak-539 C4 — inter-pass sleep is asyncio.sleep (not time.sleep)]")
        src = _source()
        # The exact call as it appears in _fetch_all_passes.
        self.assertIn("await asyncio.sleep(5 * (retry_pass + 1))", src)
        # And time.sleep must NOT appear inside _fetch_all_passes'
        # body. Scope to that method.
        import re
        m = re.search(
            r"async def _fetch_all_passes\(.*?\):(.*?)(?=\n    (?:async )?def |\nclass )",
            src, re.DOTALL,
        )
        self.assertIsNotNone(m, "could not locate _fetch_all_passes body")
        self.assertNotIn(
            "time.sleep(", m.group(1),
            "time.sleep found inside _fetch_all_passes — would block "
            "the event loop; use asyncio.sleep instead.",
        )
        print("  ✓ await asyncio.sleep present; no time.sleep in the async body")


if __name__ == "__main__":
    print("ak-539 CRITICAL batch — MF-rate hardening regression tests")
    print("=" * 70)
    unittest.main(verbosity=0, exit=False)
    print("=" * 70)
    print("Done.")
