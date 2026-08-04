import asyncio
import json
import os
import time

from services.tasks.baseTask import BaseTask
from aiohttp import ClientSession, ClientConnectorError, TCPConnector, ClientResponseError, ClientTimeout

from utils.logger import Logger

# Tuned for mfapi.in rate limits
CONCURRENT_REQUESTS = 25
MAX_RETRIES = 3
RETRY_DELAY = 2  # seconds between retries
RETRY_PASSES = 3  # number of full retry passes for failed schemes

# ak-539 C1: partial-success threshold. A run that resolves ≥98% of the
# URL list is treated as Completed; anything below returns Failed so the
# jobs table (and downstream operators) sees the degraded state instead
# of a "silent NAV holes" success. 98% allows for the small number of
# legitimately-404 schemes MFAPI reports on any given day (~740 on a
# 37k universe) without flipping the whole job to Failed.
_MIN_SUCCESS_RATIO = 0.98
_DNS_CACHE_TTL_SECONDS = 300  # ak-539 C2: TCPConnector DNS cache lifetime


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
            # Delete existing file if it exists, else
            latestListFile = self.jsonService.getLatestFile(self.jsonService.listType,
                                                            self.jsonService.MfListPrefix)

            # ak-539 C1: buildJsonForMF now returns (data, urls_total, urls_ok)
            # so run() can compute the success ratio and gate the completed
            # status behind it. Previously any partial success returned
            # "Completed" while writing an incomplete rates file to disk —
            # downstream getMFRate() served empty dicts for missing schemes
            # (silent NAV holes for users) with the jobs table showing green.
            jsonData, urls_total, urls_ok = self.buildJsonForMF(listUrl, latestListFile)
            filePath = os.path.join(self.tmp_dir, 'MFRate.json')
            try:
                os.remove(filePath)
            except OSError:
                pass
            self.save_json(jsonData, filePath)

            ok, err = self.safe_replace_file(filePath, self.jsonService.MfRatePrefix, self.jsonService.ratesType)
            if not ok:
                return err, "Failed", self.interval

            # ak-539 C1: success-ratio gate. We write the file first (so the
            # partial data is still available to callers that would rather
            # have degraded coverage than none), but flip the jobs-table
            # status to Failed with a descriptive message so an operator
            # sees the run isn't clean.
            #
            # Ratio math is done here rather than in buildJsonForMF so the
            # threshold constant lives with the caller that decides what
            # 'Completed' means — buildJsonForMF only reports raw counts.
            success_ratio = (urls_ok / urls_total) if urls_total else 1.0
            if success_ratio < _MIN_SUCCESS_RATIO:
                msg = (
                    f"partial success: {urls_ok}/{urls_total} schemes "
                    f"written ({success_ratio:.2%} — below "
                    f"{_MIN_SUCCESS_RATIO:.0%} threshold)"
                )
                self.logger.warning(f"MF rate job: {msg}")
                return msg, "Failed", self.interval
            return 'Completed successfully', "Completed", self.interval
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
        result_map = asyncio.run(
            self._fetch_all_passes(urls, start_time=start_time)
        )

        final_failed = len(urls) - len(result_map)
        if final_failed > 0:
            self.logger.warning(f"{final_failed} schemes still failed after all retry passes")

        self.logger.info(f"MF rate fetch complete: {len(result_map)} schemes in {time.time() - start_time:.2f}s")
        # ak-539 C1: return (data_dict, urls_total, urls_ok) so run() can
        # compute the success ratio and gate the jobs-table status. Callers
        # depending on the old single-return-value shape will TypeError on
        # unpack — intentional; there is only one internal caller (run()).
        return {"data": list(result_map.values())}, len(urls), len(result_map)

    async def _fetch_all_passes(self, urls, *, start_time):
        """ak-539 C3: single-event-loop driver for the initial + retry
        passes. Owns the retry loop end-to-end so we never hop event
        loops mid-fetch, and uses set(result_map) snapshots BEFORE each
        pass' failed_urls computation as belt-and-braces defense against
        any surviving mid-pass mutation (option (a) on top of option (b)
        per Lead's dispatch).
        """
        result_map = {}  # scheme_id -> parsed data

        # First pass — full concurrency across the deduped URL list.
        responses = await self._fetch_pass(urls)
        self._process_responses(responses, result_map)
        self.logger.info(
            f"Pass 1 complete: {len(result_map)}/{len(urls)} schemes "
            f"in {time.time() - start_time:.2f}s"
        )

        # Retry passes for URLs whose scheme_id didn't land in result_map.
        for retry_pass in range(RETRY_PASSES):
            # Snapshot succeeded ids BEFORE building the failed list so
            # concurrent producer/consumer semantics can't produce a
            # partial view. Even inside a single event loop this is a
            # cheap defense-in-depth on top of the single-loop guarantee.
            succeeded = set(result_map)
            failed_urls = [u for u in urls if u.split("/")[-1] not in succeeded]
            if not failed_urls:
                break
            self.logger.info(
                f"Retry pass {retry_pass + 2}: {len(failed_urls)} schemes to retry"
            )
            # Back off concurrency + async-sleep between passes. Sync
            # time.sleep would block the event loop and stall other
            # in-flight coroutines — see C4 comment on the caller.
            await asyncio.sleep(5 * (retry_pass + 1))
            concurrency = max(10, CONCURRENT_REQUESTS // (retry_pass + 2))
            responses = await self._fetch_pass(failed_urls, concurrency=concurrency)
            self._process_responses(responses, result_map)
            self.logger.info(
                f"Pass {retry_pass + 2} complete: {len(result_map)}/{len(urls)} "
                f"schemes in {time.time() - start_time:.2f}s"
            )

        return result_map

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
        timeout = ClientTimeout(total=30, connect=10)
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
        scheme_id = url.split("/")[-1]
        for attempt in range(MAX_RETRIES):
            async with semaphore:
                try:
                    async with session.get(url) as resp:
                        if resp.status == 200:
                            data = await resp.json()
                            return scheme_id, data
                        elif resp.status in (429, 502, 503):
                            # Rate limited or server overloaded — retry with backoff
                            await asyncio.sleep(RETRY_DELAY * (attempt + 1))
                            continue
                        else:
                            body = await resp.text()
                            self.logger.debug(f"HTTP {resp.status} for {scheme_id}: {body[:100]}")
                            return scheme_id, resp.status
                except (ClientConnectorError, asyncio.TimeoutError):
                    await asyncio.sleep(RETRY_DELAY * (attempt + 1))
                except ClientResponseError as e:
                    return scheme_id, e.status
                except Exception as e:
                    self.logger.error(f"Unexpected error for {scheme_id}: {e}")
                    return scheme_id, 500
        return scheme_id, 408  # All retries exhausted
