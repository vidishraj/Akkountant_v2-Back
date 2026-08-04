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

            jsonData = self.buildJsonForMF(listUrl, latestListFile)
            filePath = os.path.join(self.tmp_dir, 'MFRate.json')
            try:
                os.remove(filePath)
            except OSError:
                pass
            self.save_json(jsonData, filePath)

            ok, err = self.safe_replace_file(filePath, self.jsonService.MfRatePrefix, self.jsonService.ratesType)
            if not ok:
                return err, "Failed", self.interval
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

        start_time = time.time()
        result_map = {}  # scheme_id -> parsed data

        # First pass
        responses = asyncio.run(self.make_requests(urls))
        self._process_responses(responses, result_map)
        self.logger.info(
            f"Pass 1 complete: {len(result_map)}/{len(urls)} schemes in {time.time() - start_time:.2f}s")

        # Retry passes for failed schemes
        for retry_pass in range(RETRY_PASSES):
            failed_urls = [u for u in urls if u.split("/")[-1] not in result_map]
            if not failed_urls:
                break
            self.logger.info(f"Retry pass {retry_pass + 2}: {len(failed_urls)} schemes to retry")
            # Back off concurrency and add delay between passes
            time.sleep(5 * (retry_pass + 1))
            responses = asyncio.run(self.make_requests(
                failed_urls, concurrency=max(10, CONCURRENT_REQUESTS // (retry_pass + 2))))
            self._process_responses(responses, result_map)
            self.logger.info(
                f"Pass {retry_pass + 2} complete: {len(result_map)}/{len(urls)} schemes in {time.time() - start_time:.2f}s")

        final_failed = len(urls) - len(result_map)
        if final_failed > 0:
            self.logger.warning(f"{final_failed} schemes still failed after all retry passes")

        self.logger.info(f"MF rate fetch complete: {len(result_map)} schemes in {time.time() - start_time:.2f}s")
        return {"data": list(result_map.values())}

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
        connector = TCPConnector(limit_per_host=concurrency, force_close=True)
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
