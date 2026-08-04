import asyncio
import json
import os
import random
import time
from collections import defaultdict

from services.tasks.baseTask import BaseTask
from aiohttp import ClientSession, ClientConnectorError, TCPConnector, ClientResponseError, ClientTimeout

from utils.logger import Logger

# Tuned for mfapi.in rate limits
CONCURRENT_REQUESTS = 25
MAX_RETRIES = 3
RETRY_PASSES = 3  # number of full retry passes for failed schemes

# ak-5jq H2: per-request timeout dropped from 30s to 10s. mfapi.in p99
# is well under 5s on healthy runs; 10s catches genuine timeouts without
# stretching a bad batch into minutes of wall time. Combined with the
# H2 exponential backoff below, worst-case per-scheme wall time is now
# bounded by (10s * MAX_RETRIES=3) + backoff sum ≈ 30 + (2+4+8) = ~44s,
# vs the pre-H2 90s+ from linear 30-second timeouts + 2/4/6s delays.
_REQUEST_TIMEOUT_SECONDS = 10
_REQUEST_CONNECT_TIMEOUT_SECONDS = 5

# ak-5jq H2: exponential backoff config. Base 2s, doubles per attempt
# (2/4/8...), capped at 30s. Jitter (0-25% of the base) prevents retry
# thundering-herd when many schemes hit the same rate limit in the same
# window. Applied to 5xx/connect-error retries.
_BACKOFF_BASE_SECONDS = 2.0
_BACKOFF_CAP_SECONDS = 30.0
_BACKOFF_JITTER_RATIO = 0.25

# ak-5jq H3: 429 (rate-limit) gets a LONGER backoff — upstream is
# explicitly telling us to slow down. Base 5s (vs 2s for 5xx), cap 60s.
_BACKOFF_429_BASE_SECONDS = 5.0
_BACKOFF_429_CAP_SECONDS = 60.0

# ak-539 C1: partial-success threshold. A run that resolves ≥98% of the
# URL list is treated as Completed; anything below returns Failed so the
# jobs table (and downstream operators) sees the degraded state instead
# of a "silent NAV holes" success. 98% allows for the small number of
# legitimately-404 schemes MFAPI reports on any given day (~740 on a
# 37k universe) without flipping the whole job to Failed.
#
# ak-5jq H3: the denominator for this ratio now excludes 404 permanent-
# skips, so a run that only "fails" because a batch of schemes were
# genuinely delisted by MFAPI no longer trips the 98% gate. This makes
# the constant more honest about what it's guarding against.
_MIN_SUCCESS_RATIO = 0.98
_DNS_CACHE_TTL_SECONDS = 300  # ak-539 C2: TCPConnector DNS cache lifetime

# ak-539 v2 (post-review MAJOR fix): coverage hard floor.
# Below this ratio we treat the run as a transient-outage class and
# PRESERVE the last-good rates file on disk rather than clobbering it
# with a near-empty replacement. Rationale: on an mfapi.in outage /
# DNS flood / rate-limit apocalypse, most schemes return non-200 and
# never land in result_map — the "partial" file would be nearly empty
# and callers would see NAV holes for ~1 interval until the next good
# run. A legitimate partial run (e.g. Aug 3-class SIGKILL) still lands
# above 50% coverage because retries mostly complete before the kill;
# a real outage lands well below. 0.5 is a clear "half-or-more failed"
# signal that no legitimate partial-run scenario should hit.
_COVERAGE_HARD_FLOOR = 0.5


# ak-5jq H3: fetch_scheme now returns a classified failure marker
# instead of a bare int status. _process_responses skips these; the
# _fetch_all_passes retry loop reads them to decide whether to retry
# (all but permanent_skip_404) and to accumulate error-class stats.
_ERROR_KEY = "__error_class__"


# ak-5jq v2 MINOR-4xx: HTTP client-error statuses that we treat as
# permanent skips alongside 404. Distinct from 404 in classification so
# operators can see the split in error_class_counts logs, but both go
# into permanent_skip_ids so retry passes never re-fetch them.
_PERMANENT_CLIENT_ERROR_STATUSES = frozenset({400, 401, 403})


def _completed_msg(permanent_404, permanent_4xx):
    """ak-5jq v3 MINOR-A: build the run() Completed message with the
    permanent-skip counts split by class. Pre-v3 lumped both under
    'permanently dropped by mfapi.in' which was inaccurate — 4xx is
    OUR bad request, not their delisting. Distinct sub-strings so an
    operator can quickly triage the two failure classes.
    """
    parts = []
    if permanent_404 > 0:
        parts.append(f"{permanent_404} delisted by mfapi.in (404)")
    if permanent_4xx > 0:
        parts.append(f"{permanent_4xx} client-error (4xx)")
    if parts:
        return f"Completed successfully ({' + '.join(parts)})"
    return "Completed successfully"


def _compute_backoff(attempt, *, base, cap, jitter_ratio=_BACKOFF_JITTER_RATIO):
    """ak-5jq H2: exponential backoff with jitter and a hard cap.

    attempt is 0-indexed. Returns a float number of seconds to sleep.
    Uses random.uniform for jitter so concurrent scheme retries don't
    align into a thundering herd against mfapi.in.

    ak-5jq v2 MINOR-cap-label: the true maximum is `cap * (1 +
    jitter_ratio)` not `cap` — the jitter is ADDITIVE on top of the
    capped raw value. With jitter_ratio=0.25 the effective max is
    ~1.25× the cap (e.g. 5xx cap 30s → real max ~37.5s; 429 cap 60s
    → real max ~75s). Callers computing worst-case wall time budgets
    should use `cap * 1.25` not `cap`.
    """
    raw = min(cap, base * (2 ** attempt))
    jitter = random.uniform(0, raw * jitter_ratio)
    return raw + jitter


class SetMFRate(BaseTask):
    _instance = None

    def __new__(cls, *args, **kwargs):
        if not cls._instance:
            cls._instance = super(SetMFRate, cls).__new__(cls)
        return cls._instance

    def __init__(self, title, priority):
        if not hasattr(self, 'initialized'):  # Prevent multiple initializations
            super().__init__(title, priority)
            self.logger = Logger(__name__).get_logger()
            # 4 hours
            self.interval = 150

    def run(self):
        try:
            listUrl = "https://api.mfapi.in/mf"
            latestListFile = self.jsonService.getLatestFile(self.jsonService.listType,
                                                            self.jsonService.MfListPrefix)

            # ak-5jq H1: explicit dependency check. Previously
            # buildJsonForMF called `open(latestListFile)` which raises
            # TypeError on None — the job crashed with an unhelpful
            # traceback whenever SetMFDetails hadn't run in >7d (past
            # the MfList retention window). Now we return a clean
            # Failed with an operator-actionable message.
            if latestListFile is None:
                msg = (
                    "MF details list missing or stale — SetMFDetails "
                    f"must run first (looked for prefix "
                    f"{self.jsonService.MfListPrefix!r} under "
                    f"{self.jsonService.listType!r})"
                )
                self.logger.error(f"MF rate job: {msg}")
                return msg, "Failed", self.interval

            # ak-539 C1 + ak-5jq H3 + v3 MINOR-B: buildJsonForMF returns
            # (data, urls_total, urls_ok, permanent_404, permanent_4xx).
            # The two skip counts are tracked separately per v3 MINOR-B:
            #   * permanent_404 (delisted by mfapi.in) IS excluded from
            #     the ratio denominator — genuinely no longer part of
            #     the answerable universe.
            #   * permanent_4xx (client-error 400/401/403 — OUR bad
            #     requests) stays IN the denominator so a systematic
            #     client-side breakage wave visibly drags ratio down
            #     and trips the hard-floor / 98% gate. Excluding them
            #     from denominator (v2) hid a fixable bug class.
            jsonData, urls_total, urls_ok, permanent_404, permanent_4xx = (
                self.buildJsonForMF(listUrl, latestListFile)
            )
            answerable = max(urls_total - permanent_404, 0)

            # ak-5jq v2 MAJOR #2 + v3: degenerate-answerable guard.
            # v3 broadens the guard from `answerable == 0 and urls_total
            # > 0` to just `answerable == 0` — reviewer flagged that
            # the v2 shape still let urls_total == 0 slip through the
            # `success_ratio = 1.0` fallback and clobber last-good with
            # an empty file (SetMFDetails writing {data: []} on a
            # transient mfapi.in hiccup or parse glitch would trigger
            # this silently). The MF universe is never legitimately
            # empty, so preserving last-good on urls_total==0 is
            # strictly safe on every path including first run (H1's
            # None-guard fires before we get here on true first run).
            #
            # Note the ordering: this check must precede the hard-floor
            # gate below because success_ratio is meaningless when
            # answerable == 0 (would be 1.0 by the fallback rule).
            if answerable == 0:
                msg = (
                    f"coverage degenerate: answerable=0 "
                    f"(urls_total={urls_total} permanent_404={permanent_404} "
                    f"permanent_4xx={permanent_4xx}); preserving last-good "
                    f"NAVs on disk"
                )
                self.logger.error(f"MF rate job: {msg}")
                return msg, "Failed", self.interval

            success_ratio = (urls_ok / answerable) if answerable > 0 else 1.0

            # ak-539 v2 (post-review MAJOR fix): hard-floor gate FIRST.
            # safe_replace_file destroys the last-good rates file the
            # moment it runs; a transient-outage-shape result_map
            # (near-empty but size > 0) would clobber it with garbage
            # under the v1 flow. Below the hard floor we skip both the
            # tmp write AND the swap so the previous good file stays
            # on disk exactly as-is, and callers keep serving the last
            # known NAVs until the next run recovers.
            #
            # v3 MINOR-B: `answerable` denominator now includes 4xx
            # (client-error) schemes so a systematic client-side
            # breakage wave visibly drags the ratio down and reaches
            # this gate. Pre-v3, 4xx were excluded from denominator
            # and a bad-encoding wave (X% 400s) silently completed.
            if success_ratio < _COVERAGE_HARD_FLOOR:
                msg = (
                    f"coverage below hard floor: {urls_ok}/{answerable} "
                    f"answerable ({success_ratio:.2%} — below "
                    f"{_COVERAGE_HARD_FLOOR:.0%} floor); "
                    f"preserving last-good NAVs on disk"
                )
                self.logger.error(f"MF rate job: {msg}")
                return msg, "Failed", self.interval

            # Above the hard floor — write + swap. The file may be
            # degraded (below the 98% threshold) but is still better
            # than stale for the majority of callers.
            filePath = os.path.join(self.tmp_dir, 'MFRate.json')
            try:
                os.remove(filePath)
            except OSError:
                pass
            self.save_json(jsonData, filePath)

            ok, err = self.safe_replace_file(filePath, self.jsonService.MfRatePrefix, self.jsonService.ratesType)
            if not ok:
                return err, "Failed", self.interval

            # ak-539 C1: 98% partial-success gate.
            if success_ratio < _MIN_SUCCESS_RATIO:
                msg = (
                    f"partial success: {urls_ok}/{answerable} answerable "
                    f"schemes written ({success_ratio:.2%} — below "
                    f"{_MIN_SUCCESS_RATIO:.0%} threshold)"
                )
                self.logger.warning(f"MF rate job: {msg}")
                return msg, "Failed", self.interval

            # ak-5jq H3 + v3 MINOR-A: Completed msg surfaces the
            # permanent-skip counts when there are any. v3 splits the
            # message into distinct 404 (delisted by mfapi.in) and 4xx
            # (client-error — OUR bad requests) portions so operators
            # can distinguish "MFAPI's daily attrition" from "we're
            # sending malformed requests / lost auth".
            return _completed_msg(permanent_404, permanent_4xx), "Completed", self.interval
        except Exception as ex:
            return ex.__str__(), "Failed", self.interval

    def buildJsonForMF(self, baseUrl, listPath):
        with open(listPath, 'r') as file:
            data = json.load(file)
        data = data['data']

        # ak-lp6 IMMEDIATE fix: dedupe URLs, preserving encounter order.
        # Background: on 2026-08-03 the SetMFDetails file ballooned from
        # 37,713 → 113,139 schemes (exactly ~3× — clean-multiple smell of
        # duplicated entries, not real MF universe growth). Every
        # SetMFRate run since has taken the raw list, hit each URL once,
        # and been SIGKILLed at ~41min because it was fetching 3× the
        # work. Deduping here breaks that class regardless of what's
        # feeding dupes into the list — see SetMFDetails.py for the
        # count-deviation diagnostic that catches the upstream regression.
        # dict.fromkeys() preserves the FIRST occurrence's order so retry
        # behavior in the fetch loop stays deterministic.
        raw_urls = [f"{baseUrl}/{item.get('schemeCode')}" for item in data]
        urls = list(dict.fromkeys(raw_urls))
        dupes_removed = len(raw_urls) - len(urls)
        if dupes_removed > 0:
            self.logger.warning(
                f"MF URL list: raw={len(raw_urls)} deduped={len(urls)} "
                f"dupes_removed={dupes_removed}"
            )
        else:
            self.logger.info(
                f"MF URL list: raw={len(raw_urls)} deduped={len(urls)} "
                f"dupes_removed=0"
            )

        # ak-539 C3 + C4: drive all passes inside ONE async coroutine
        # with a single asyncio.run() at the entry point. Previously the
        # loop wrapped every pass in its own asyncio.run(...) call,
        # tearing down + rebuilding an event loop between passes; that
        # opens an event-loop-teardown edge case where a mid-write
        # coroutine could theoretically overwrite pass-1 successes on
        # loop reinit. Single-loop refactor eliminates the race entirely.
        #
        # C4: the previous retry-loop used time.sleep(5*(pass+1)) between
        # passes. That was a sync sleep inside async-adjacent code — while
        # not a bug in the pre-refactor sync-driver shape, it becomes a
        # correctness bug the moment we host the loop inside an async
        # coroutine (would block the whole event loop, freezing every
        # in-flight retry). Swapped to asyncio.sleep() as part of the
        # single-loop move so the shape is correct going forward.
        start_time = time.time()
        result_map, permanent_404_ids, permanent_4xx_ids, error_class_counts = asyncio.run(
            self._fetch_all_passes(urls, start_time=start_time)
        )

        # ak-5jq H3 + v2 + v3: log the per-class error breakdown so an
        # operator can distinguish "transient 5xx flood" from "batch
        # of 404 delistings" from "rate-limited" — none of which
        # showed up differently in the pre-H3 logs. v3 splits
        # permanent_skips into 404 vs 4xx in the summary so the
        # bad-encoding-wave class is grep-able without cross-
        # correlating with error_class_counts.
        if error_class_counts:
            breakdown = " ".join(
                f"{k}={v}" for k, v in sorted(error_class_counts.items())
            )
            self.logger.info(
                f"MF rate: error breakdown {breakdown} "
                f"permanent_404={len(permanent_404_ids)} "
                f"permanent_4xx={len(permanent_4xx_ids)}"
            )

        # Total permanent skips = 404 + 4xx (union). Retry loop already
        # filters both; here we just report the totals for context.
        permanent_total = len(permanent_404_ids) + len(permanent_4xx_ids)
        final_failed = (
            len(urls) - len(result_map) - permanent_total
        )
        if final_failed > 0:
            self.logger.warning(
                f"{final_failed} schemes still failed after all retry passes "
                f"(excluding {permanent_total} permanent skips: "
                f"{len(permanent_404_ids)} 404 + {len(permanent_4xx_ids)} 4xx)"
            )

        self.logger.info(
            f"MF rate fetch complete: {len(result_map)} schemes in "
            f"{time.time() - start_time:.2f}s "
            f"(permanent_404={len(permanent_404_ids)} "
            f"permanent_4xx={len(permanent_4xx_ids)})"
        )
        # ak-539 C1 + ak-5jq H3 + v3 MINOR-B: return
        # (data, urls_total, urls_ok, permanent_404, permanent_4xx).
        # Splitting the two permanent-skip categories lets run()
        # adjust the ratio denominator with 404 only (delisted →
        # legitimately excluded) while keeping 4xx in denominator
        # (OUR bad requests → visibility of fixable bugs preserved).
        # Callers depending on the old shape TypeError on unpack —
        # intentional; there is only one internal caller.
        return (
            {"data": list(result_map.values())},
            len(urls),
            len(result_map),
            len(permanent_404_ids),
            len(permanent_4xx_ids),
        )

    async def _fetch_all_passes(self, urls, *, start_time):
        """ak-539 C3: single-event-loop driver for the initial + retry
        passes. Owns the retry loop end-to-end so we never hop event
        loops mid-fetch, and uses set(result_map) snapshots BEFORE each
        pass' failed_urls computation as belt-and-braces defense against
        any surviving mid-pass mutation (option (a) on top of option (b)
        per Lead's dispatch).

        ak-5jq H3 + v2 + v3: additionally accumulates
        permanent_404_ids and permanent_4xx_ids (both excluded from
        retry passes via union, but tracked separately so run() can
        include only 404 in the ratio denominator) and per-class
        error counts across all passes. Returns (result_map,
        permanent_404_ids, permanent_4xx_ids, error_class_counts).
        """
        result_map = {}  # scheme_id -> parsed data
        # ak-5jq H3 + v3 MINOR-B: split permanent skips by class.
        # 404 (delisted by mfapi.in) → denominator exclusion in run().
        # 4xx (OUR bad request) → stays in denominator so systematic
        # client-side breakage visibly trips the ratio gates.
        permanent_404_ids = set()
        permanent_4xx_ids = set()
        error_class_counts = defaultdict(int)
        # ak-iwj M4: retry-effectiveness metrics — per-pass "how many
        # previously-missing schemes did this pass recover" plus a job-
        # end summary. Enables data-driven tuning of RETRY_PASSES: if
        # pass 3 consistently recovers 0 schemes, drop it. If pass 2
        # is still recovering meaningful volume, we may need pass 4.
        # Pre-M4 there was no way to answer this without grepping raw
        # completion counts across log lines.
        recovery_per_pass = []  # index i = schemes recovered by pass i+1

        # First pass — full concurrency across the deduped URL list.
        # By definition pass 1 "recovers" its successes from zero.
        responses = await self._fetch_pass(urls)
        self._process_responses(responses, result_map)
        self._process_errors(
            responses, permanent_404_ids, permanent_4xx_ids, error_class_counts,
        )
        recovery_per_pass.append(len(result_map))
        self.logger.info(
            f"Pass 1 complete: {len(result_map)}/{len(urls)} schemes "
            f"in {time.time() - start_time:.2f}s "
            f"(permanent_404={len(permanent_404_ids)} "
            f"permanent_4xx={len(permanent_4xx_ids)})"
        )

        # Retry passes for URLs whose scheme_id didn't land in result_map
        # AND aren't permanently skipped (404s never retry — mfapi.in
        # already told us that scheme is gone; 4xx also skipped —
        # sticky client-side errors won't improve on retry).
        for retry_pass in range(RETRY_PASSES):
            # Snapshot succeeded ids BEFORE building the failed list so
            # concurrent producer/consumer semantics can't produce a
            # partial view. Even inside a single event loop this is a
            # cheap defense-in-depth on top of the single-loop guarantee.
            succeeded_before = set(result_map)
            # v3: filter against the UNION of both permanent-skip sets.
            permanent_skip_union = permanent_404_ids | permanent_4xx_ids
            failed_urls = [
                u for u in urls
                if (sid := u.split("/")[-1]) not in succeeded_before
                and sid not in permanent_skip_union
            ]
            if not failed_urls:
                break
            # ak-iwj M4: snapshot the pre-pass failed count so we can
            # report exactly how many schemes THIS pass recovered vs
            # rolled forward.
            previous_missing = len(failed_urls)
            self.logger.info(
                f"Retry pass {retry_pass + 2}: {previous_missing} schemes to retry"
            )
            # Back off concurrency + async-sleep between passes. Sync
            # time.sleep would block the event loop and stall other
            # in-flight coroutines — see C4 comment on the caller.
            await asyncio.sleep(5 * (retry_pass + 1))
            concurrency = max(10, CONCURRENT_REQUESTS // (retry_pass + 2))
            responses = await self._fetch_pass(failed_urls, concurrency=concurrency)
            self._process_responses(responses, result_map)
            self._process_errors(
                responses, permanent_404_ids, permanent_4xx_ids, error_class_counts,
            )
            # ak-iwj M4: recovery = new successes minus prior successes.
            # still_failing = previous_missing - recovered (excludes any
            # schemes that JUST became permanent-skip in this pass; they
            # aren't really "still failing", they're diagnosed).
            recovered = len(result_map) - len(succeeded_before)
            newly_permanent = (
                (permanent_skip_union ^ (permanent_404_ids | permanent_4xx_ids))
            )
            still_failing = previous_missing - recovered - len(newly_permanent)
            recovery_per_pass.append(recovered)
            # M4 signature log — grep-friendly key=value form so infra
            # can chart pass-by-pass recovery over time.
            self.logger.info(
                f"MF rate: retry_pass={retry_pass + 2} "
                f"previous_missing={previous_missing} "
                f"recovered={recovered} "
                f"still_failing={still_failing} "
                f"(permanent_404={len(permanent_404_ids)} "
                f"permanent_4xx={len(permanent_4xx_ids)})"
            )
            self.logger.info(
                f"Pass {retry_pass + 2} complete: {len(result_map)}/{len(urls)} "
                f"schemes in {time.time() - start_time:.2f}s "
                f"(permanent_404={len(permanent_404_ids)} "
                f"permanent_4xx={len(permanent_4xx_ids)})"
            )

        # ak-iwj M4: job-end summary — one grep-able line with per-pass
        # recovery counts. Enables data-driven RETRY_PASSES tuning: if
        # pass_3_recovered is consistently 0, drop it.
        unrecovered = (
            len(urls) - len(result_map)
            - len(permanent_404_ids) - len(permanent_4xx_ids)
        )
        summary_parts = [
            f"pass_{i+1}_recovered={n}" for i, n in enumerate(recovery_per_pass)
        ]
        self.logger.info(
            f"MF rate: RETRY_PASSES={RETRY_PASSES} "
            + " ".join(summary_parts)
            + f" unrecovered={unrecovered}"
        )

        return result_map, permanent_404_ids, permanent_4xx_ids, error_class_counts

    def _process_errors(self, responses, permanent_404_ids,
                        permanent_4xx_ids, error_class_counts):
        """ak-5jq H3 + v2 + v3: walk responses looking for classified
        failure markers (see fetch_scheme). Extracts:
          * permanent_404_ids: schemes returning 404 (delisted).
            Retry-skipped AND excluded from denominator in run() —
            they're genuinely no longer part of the answerable universe.
          * permanent_4xx_ids: schemes returning 400/401/403 (our bad
            request). Retry-skipped BUT stays in denominator — a
            systematic client-side breakage wave should visibly drag
            the ratio down and trigger the hard-floor / 98% gate.
          * error_class_counts: per-class tallies for the summary log.
            Distinct '_404' vs '_4xx' classes preserved.
        Success responses (parsed by _process_responses) are ignored here.
        """
        for response in responses:
            if not isinstance(response, tuple):
                continue
            scheme_id, payload = response
            if not isinstance(payload, dict):
                continue
            kind = payload.get(_ERROR_KEY)
            if not kind:
                continue
            error_class_counts[kind] += 1
            # ak-5jq v3: route by exact class to the correct bucket.
            # Both are permanent-skip (retry loop filters both) but
            # only 404 is excluded from the ratio denominator.
            if kind == "permanent_skip_404":
                permanent_404_ids.add(scheme_id)
            elif kind == "permanent_skip_4xx":
                permanent_4xx_ids.add(scheme_id)

    async def _fetch_pass(self, urls, concurrency=CONCURRENT_REQUESTS):
        """Wrapper around make_requests kept for parity with the pre-
        C3 call sites. Preserved as a separate method so subclasses /
        tests can override the single-pass shape without patching the
        multi-pass driver."""
        return await self.make_requests(urls, concurrency=concurrency)

    def _process_responses(self, responses, result_map):
        """Parse successful responses into result_map, skip failures."""
        for response in responses:
            if not isinstance(response, tuple):
                continue
            scheme_id, data = response
            if not isinstance(data, dict) or 'data' not in data:
                continue
            try:
                nav_data = data['data']
                if not nav_data or not isinstance(nav_data, list):
                    continue
                entry = {
                    "date": nav_data[0]['date'],
                    "nav": nav_data[0]['nav'],
                    "scheme_id": scheme_id,
                }
                # Add optional metadata
                meta = data.get('meta', {})
                if meta:
                    entry["fundHouse"] = meta.get('fund_house', '')
                    entry["schemeType"] = meta.get('scheme_type', '')
                if len(nav_data) > 1:
                    entry["lastDate"] = nav_data[1]['date']
                    entry["lastNav"] = nav_data[1]['nav']
                result_map[scheme_id] = entry
            except (KeyError, IndexError, TypeError) as ex:
                self.logger.debug(f"Skipping scheme {scheme_id}: {ex}")

    async def make_requests(self, urls: list, concurrency=CONCURRENT_REQUESTS, **kwargs):
        semaphore = asyncio.Semaphore(concurrency)
        # ak-5jq H2: per-request total dropped 30s → 10s (mfapi.in p99
        # is well under 5s on healthy runs; 10s catches genuine hangs
        # without stretching bad batches into minutes). Connect budget
        # halved to 5s — DNS + TCP handshake for mfapi.in should
        # complete in milliseconds once ttl_dns_cache warms.
        timeout = ClientTimeout(
            total=_REQUEST_TIMEOUT_SECONDS,
            connect=_REQUEST_CONNECT_TIMEOUT_SECONDS,
        )
        # ak-539 C2: enable HTTP connection pooling. Previously
        # force_close=True + limit_per_host=<concurrency> meant every
        # one of ~37k requests opened + closed its own TCP socket → FD
        # churn, ~2-3× slower than necessary (bottleneck was socket
        # lifecycle, not request pipelining, so concurrency=25 wasn't
        # helping). Now:
        #   * limit=CONCURRENT_REQUESTS  — total in-flight cap (single-
        #     host workload against mfapi.in, so limit_per_host adds no
        #     extra safety and is dropped).
        #   * ttl_dns_cache=300          — reuse DNS lookups for 5min
        #     across the 37k-scheme fetch instead of resolving per URL.
        #   * force_close default (False) — sockets stay warm across
        #     requests to mfapi.in for keep-alive pipelining.
        # Expect ~3-4× throughput improvement + zero FD churn. Latency
        # gauge in agent_run log should drop from ~600s → ~150-200s at
        # 37k schemes.
        connector = TCPConnector(
            limit=CONCURRENT_REQUESTS,
            ttl_dns_cache=_DNS_CACHE_TTL_SECONDS,
        )
        async with ClientSession(connector=connector, timeout=timeout) as session:
            tasks = [self.fetch_scheme(url, session, semaphore) for url in urls]
            results = await asyncio.gather(*tasks, return_exceptions=True)
        return [r for r in results if not isinstance(r, Exception)]

    async def fetch_scheme(self, url: str, session: ClientSession, semaphore: asyncio.Semaphore):
        """Fetch one scheme's NAV from mfapi.in.

        ak-5jq H2 + H3 + v2: return-value contract:
          * (scheme_id, {"data": ..., "meta": ...})  → success
          * (scheme_id, {_ERROR_KEY: 'permanent_skip_404', 'status': 404})
              → mfapi.in permanently dropped this scheme; caller must
                NOT retry (see _fetch_all_passes filter).
          * (scheme_id, {_ERROR_KEY: 'permanent_skip_4xx', 'status': <400|401|403>})
              → v2 MINOR-4xx: client-error statuses treated as permanent
                skip alongside 404. Retrying wastes semaphore slots on
                schemes that will never resolve.
          * (scheme_id, {_ERROR_KEY: 'final_429', 'status': 429})
          * (scheme_id, {_ERROR_KEY: 'final_5xx', 'status': <5xx>})
          * (scheme_id, {_ERROR_KEY: 'final_timeout', 'status': 408})
          * (scheme_id, {_ERROR_KEY: 'final_other', 'status': <int>})
        Non-success shapes are counted by _process_errors and ignored
        by _process_responses (only 'data'-shaped dicts land in
        result_map).

        Backoff (ak-5jq H2): exponential with 25% jitter, capped at
        30s for 5xx/timeouts and 60s for 429 (upstream told us to
        slow down harder). Effective max is 1.25× the cap due to
        additive jitter — see _compute_backoff docstring.

        ak-5jq v2 MAJOR #1: the semaphore is released DURING the retry
        backoff sleep, not held across it. Under a 429/5xx storm the
        pre-v2 code held all N slots for multi-second sleeps and
        collapsed effective concurrency to zero — wall-time could
        exceed the pre-H2 30s-timeout regime because nobody could
        make forward progress. Restructure: acquire semaphore ONLY
        around the network I/O; on retry, compute the delay INSIDE
        the semaphore-held block, EXIT the block (release the slot),
        THEN sleep OUTSIDE, then continue the loop (which re-acquires
        the semaphore on the next attempt).
        """
        scheme_id = url.split("/")[-1]
        last_status = 500  # tracks the class the terminal `return` reports
        for attempt in range(MAX_RETRIES):
            # Sentinel: if this remains None at the end of the semaphore
            # block, we returned early (success/permanent-skip/hard fail).
            # If set to a float, we release the semaphore then sleep
            # THAT many seconds outside the with-block before retrying.
            backoff_delay = None
            async with semaphore:
                try:
                    async with session.get(url) as resp:
                        if resp.status == 200:
                            data = await resp.json()
                            return scheme_id, data
                        if resp.status == 404:
                            # ak-5jq H3: permanent skip. mfapi.in has
                            # dropped this scheme; retrying is wasted
                            # work + a wasted retry-loop slot for a
                            # legit-transient failure. INFO-level (not
                            # WARN) — this happens as a normal part of
                            # MFAPI's daily attrition.
                            self.logger.info(
                                f"MF rate: permanent skip 404 for scheme "
                                f"{scheme_id} (delisted by mfapi.in)"
                            )
                            return scheme_id, {
                                _ERROR_KEY: "permanent_skip_404",
                                "status": 404,
                            }
                        if resp.status in _PERMANENT_CLIENT_ERROR_STATUSES:
                            # ak-5jq v2 MINOR-4xx: 400/401/403 don't
                            # improve on retry (bad URL / unauthorized /
                            # forbidden are all sticky). Classify as
                            # permanent skip so _fetch_all_passes'
                            # filter excludes them from subsequent
                            # retry passes. Distinct class from 404
                            # so the error_class_counts log preserves
                            # the semantic difference (delisted vs
                            # client error).
                            body = await resp.text()
                            self.logger.warning(
                                f"MF rate: permanent skip {resp.status} "
                                f"for scheme {scheme_id} (client error, "
                                f"not retried): {body[:100]}"
                            )
                            return scheme_id, {
                                _ERROR_KEY: "permanent_skip_4xx",
                                "status": resp.status,
                            }
                        if resp.status == 429:
                            # ak-5jq H2 + H3: longer backoff — upstream
                            # explicitly asked us to slow down. v2
                            # MAJOR #1: compute delay HERE, release
                            # semaphore below, sleep OUTSIDE.
                            last_status = 429
                            backoff_delay = _compute_backoff(
                                attempt,
                                base=_BACKOFF_429_BASE_SECONDS,
                                cap=_BACKOFF_429_CAP_SECONDS,
                            )
                        elif 500 <= resp.status < 600:
                            # ak-5jq H2 + H3: transient server error —
                            # exponential backoff. v2 MAJOR #1: same
                            # release-then-sleep pattern as 429.
                            last_status = resp.status
                            backoff_delay = _compute_backoff(
                                attempt,
                                base=_BACKOFF_BASE_SECONDS,
                                cap=_BACKOFF_CAP_SECONDS,
                            )
                        else:
                            # ak-5jq H3: unusual status not covered
                            # above (e.g. 402, 405, 410, …). WARN then
                            # fail — retrying isn't useful.
                            body = await resp.text()
                            self.logger.warning(
                                f"MF rate: HTTP {resp.status} for "
                                f"{scheme_id} (non-retryable): "
                                f"{body[:100]}"
                            )
                            return scheme_id, {
                                _ERROR_KEY: "final_other",
                                "status": resp.status,
                            }
                except (ClientConnectorError, asyncio.TimeoutError):
                    # ak-5jq v2 MAJOR #1: compute delay inside, sleep
                    # outside — same as 429/5xx branches.
                    last_status = 408
                    backoff_delay = _compute_backoff(
                        attempt,
                        base=_BACKOFF_BASE_SECONDS,
                        cap=_BACKOFF_CAP_SECONDS,
                    )
                except ClientResponseError as e:
                    # aiohttp's response-shape error — treat like the
                    # generic non-retryable branch above.
                    self.logger.warning(
                        f"MF rate: ClientResponseError {e.status} for {scheme_id}"
                    )
                    return scheme_id, {
                        _ERROR_KEY: "final_other",
                        "status": e.status,
                    }
                except Exception as e:
                    self.logger.error(f"Unexpected error for {scheme_id}: {e}")
                    return scheme_id, {
                        _ERROR_KEY: "final_other",
                        "status": 500,
                    }
            # ak-5jq v2 MAJOR #1: semaphore RELEASED here (async-with
            # exited above). Do the retry backoff sleep OUTSIDE so
            # other coroutines can acquire the slot and make forward
            # progress. Under a 429/5xx storm this preserves
            # concurrency; without it, N slots would stall in
            # multi-second sleeps and the whole batch would serialize.
            if backoff_delay is not None:
                await asyncio.sleep(backoff_delay)
                continue
            # No backoff scheduled AND we didn't return early —
            # unreachable, but explicit fall-through prevents an
            # accidental infinite loop if the flow above ever changes.
            break
        # ak-5jq H3: retries exhausted — classify the final failure by
        # the last status we observed.
        if last_status == 429:
            kind = "final_429"
        elif 500 <= last_status < 600:
            kind = "final_5xx"
        elif last_status == 408:
            kind = "final_timeout"
        else:
            kind = "final_other"
        return scheme_id, {_ERROR_KEY: kind, "status": last_status}
