import asyncio
import json
import os
import time

from services.tasks.baseTask import BaseTask
from aiohttp import ClientSession, ClientConnectorError, TCPConnector, ClientResponseError

from utils.logger import Logger

start_time = None
# Check this while deploying
CONCURRENT_REQUESTS = 200  # Reduce to 50 for stability; adjust based on testing
# To count successful requests
requestsProcessed = 0
# Retry settings
MAX_RETRIES = 2  # Number of retries for failed requests
RETRY_DELAY = 0  # Seconds to wait between retries
CONCURRENT_REQUESTS2 = 20  # Reduced for stability
BATCH_SIZE = 50  # Batch size for processing


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
            try:
                filePath = self.tmp_dir + 'MFRate.json'
                # delete file if it exists
                try:
                    os.remove(filePath)
                except OSError:
                    pass
                self.save_json(jsonData, filePath)

                # get the latest rate file in assets
                latestFile = self.jsonService.getLatestFile(self.jsonService.ratesType, self.jsonService.MfRatePrefix)

                latestFilePath = self.jsonService.getFilePath(self.jsonService.MfRatePrefix,
                                                              self.jsonService.ratesType)

                fileMoved = self.move_file(filePath, latestFilePath)

                if fileMoved:
                    # delete old file
                    self.jsonService.deleteFile(latestFile)
                else:
                    return 'Failed to move file', "Failed", self.interval
                return 'Completed successfully', "Completed", self.interval
            except Exception as ex:
                return ex.__str__(), "Failed", self.interval
        except Exception as ex:
            return ex.__str__(), "Failed", self.interval

    def buildJsonForMF(self, baseUrl, listPath):
        with open(listPath, 'r') as file:
            data = json.load(file)
        data = data['data']
        urls = [f"{baseUrl}/{item.get('schemeCode')}" for item in data]
        self.logger.info(f"API URL list built for MF. {len(urls)}")

        global start_time
        start_time = time.time()
        result_data = []
        responses = asyncio.run(self.make_requests(urls))
        for response in responses:
            if isinstance(response, tuple):  # Ensure it's a valid JSON response
                try:
                    result_data.append(
                        {
                            "date": response[1]['data'][0]['date'],
                            "nav": response[1]['data'][0]['nav'],
                            "scheme_id": response[0]
                        },
                    )
                    try:
                        # will try to add additional information about mf here
                        result_data[-1]["fundHouse"] = response[1]['meta']['fund_house']
                        result_data[-1]["schemeType"] = response[1]['meta']['fund_house']
                        result_data[-1]["lastDate"] = response[1]['data'][1]['date']
                        result_data[-1]["lastNav"] = response[1]['data'][1]['nav']
                    except Exception as ex:
                        self.logger.error(f"Error adding addition info for MF {response[0]} {ex}")
                except Exception as ex:
                    self.logger.error(f"Error while adding response to the json {ex}")
                    self.logger.error(f"{response}")
            else:
                self.logger.error(f"Skipping invalid response: {response}")

        return {"data": result_data}

    async def make_requests(self, urls: list, **kwargs):
        semaphore = asyncio.Semaphore(CONCURRENT_REQUESTS)  # Limit concurrent connections
        connector = TCPConnector(limit_per_host=CONCURRENT_REQUESTS)  # Control simultaneous connections per host
        async with ClientSession(connector=connector) as session:
            tasks = [self.fetch_html(url, session, semaphore, **kwargs) for url in urls]
            results = await asyncio.gather(*tasks, return_exceptions=True)

        return results

    async def fetch_html(self, url: str, session: ClientSession, semaphore: asyncio.Semaphore, **kwargs):
        global requestsProcessed
        retries = 0
        while retries < MAX_RETRIES:
            async with semaphore:  # Control concurrency with semaphore
                try:
                    async with session.get(url, timeout=15, **kwargs) as resp:
                        if resp.status == 200:
                            requestsProcessed += 1
                            if requestsProcessed % 100 == 0:
                                self.logger.info(
                                    f"Request {requestsProcessed} finished in {time.time() - start_time:.2f}s")
                        else:
                            self.logger.info(resp.status, resp)
                        data = await resp.json()  # Use .json() for JSON responses
                        return url.split("/")[-1], data
                except (ClientConnectorError, asyncio.TimeoutError) as e:
                    retries += 1
                    await asyncio.sleep(RETRY_DELAY)
                except ClientResponseError as e:
                    return url, e.status  # Return specific HTTP error code
                except Exception as e:
                    self.logger.error(f"Unexpected error for {url}: {e}")
                    return url, 500  # General server error

        return url, 408  # Return timeout status after retries
