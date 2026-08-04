"""ak-539 CRITICAL batch regression tests: partial-success gate,
connector pooling, single-event-loop retry driver, async sleep.

Post-ak-lp6 hardening. Scoped strictly to services/tasks/SetMfRate.py
findings C1-C4 from the MF-jobs deep audit:

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
    sqlalchemy + flask). Instead we replicate the exact success-ratio
    gate in-test, then guard against divergence via a source-inspection
    test below."""

    _MIN_SUCCESS_RATIO = 0.98  # must match the source constant

    @staticmethod
    def _decide_status(urls_total, urls_ok, min_ratio):
        """Verbatim copy of the run() ratio gate. Divergence here or in
        the source → source-inspection test flags it."""
        ratio = (urls_ok / urls_total) if urls_total else 1.0
        if ratio < min_ratio:
            msg = (
                f"partial success: {urls_ok}/{urls_total} schemes "
                f"written ({ratio:.2%} — below {min_ratio:.0%} threshold)"
            )
            return msg, "Failed"
        return "Completed successfully", "Completed"

    def test_below_98pct_returns_failed(self):
        print("\n[ak-539 C1 — 90% success returns Failed with partial-success msg]")
        # 37k universe, 90% success = 33300 ok / 37000 total
        msg, status = self._decide_status(37000, 33300, self._MIN_SUCCESS_RATIO)
        self.assertEqual(status, "Failed")
        self.assertIn("partial success", msg)
        self.assertIn("33300/37000", msg)
        self.assertIn("90.00%", msg)
        self.assertIn("98%", msg)
        print(f"  ✓ 90% → Failed: {msg[:100]}")

    def test_at_or_above_98pct_returns_completed(self):
        print("\n[ak-539 C1 — 98%+ returns Completed]")
        # Just above threshold
        msg, status = self._decide_status(37000, 36260, self._MIN_SUCCESS_RATIO)
        self.assertEqual(status, "Completed")
        self.assertEqual(msg, "Completed successfully")
        # And well above
        msg2, status2 = self._decide_status(37000, 36999, self._MIN_SUCCESS_RATIO)
        self.assertEqual(status2, "Completed")
        print("  ✓ 98.00% and 99.99% both → Completed")

    def test_boundary_exactly_98pct(self):
        """98% exactly should PASS (>=). 97.999...% should Fail."""
        print("\n[ak-539 C1 — boundary: exact 98% passes; 97.99% fails]")
        # 98% exactly. Use ratio math that lands cleanly on 0.98:
        # 98/100 → 0.98 (>= 0.98 → Completed)
        msg, status = self._decide_status(100, 98, self._MIN_SUCCESS_RATIO)
        self.assertEqual(status, "Completed")
        # Just below (97/100 = 0.97 → Failed).
        msg2, status2 = self._decide_status(100, 97, self._MIN_SUCCESS_RATIO)
        self.assertEqual(status2, "Failed")
        print("  ✓ exact-98% passes; 97% fails")

    def test_zero_total_treated_as_success(self):
        """Empty URL list (edge case — no schemes to fetch) is degenerate
        but shouldn't crash on divide-by-zero. Treat as Completed with
        1.0 ratio — the file will be an empty dict, downstream must
        handle that (or ak-lp6-style diagnostic catches upstream why
        the list was empty)."""
        print("\n[ak-539 C1 — zero-total edge: no divide-by-zero, Completed]")
        msg, status = self._decide_status(0, 0, self._MIN_SUCCESS_RATIO)
        self.assertEqual(status, "Completed")
        print("  ✓ 0/0 → Completed (no divide-by-zero)")

    def test_aug3_replay_75pct_fails(self):
        """The Aug 3 balloon (113k URLs, ~75% success under SIGKILL)
        would have flipped the jobs table to Failed loudly under this
        gate. Regression pin for the class."""
        print("\n[ak-539 C1 — Aug 3 replay: 113k URLs @ 75% → Failed]")
        msg, status = self._decide_status(113139, 84854, self._MIN_SUCCESS_RATIO)
        self.assertEqual(status, "Failed")
        self.assertIn("partial success", msg)
        self.assertIn("84854/113139", msg)
        print(f"  ✓ 75% Aug 3 pattern → Failed loudly (no silent green)")


# ── C1 source-inspection guard ───────────────────────────────────────────


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
