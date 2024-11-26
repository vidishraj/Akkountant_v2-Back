import os
import asyncio
import json
import time

from aiohttp import ClientSession, ClientConnectorError, TCPConnector, ClientResponseError

from datetime import datetime, timedelta
from bs4 import BeautifulSoup
import re  # For regex to match the timestamp
from utils.logger import Logger
import requests
import nsepython

start_time = None
# Limit the maximum number of concurrent requests
CONCURRENT_REQUESTS = 200  # Reduce to 50 for stability; adjust based on testing
# To count successful requests
requestsProcessed = 0
# Retry settings
MAX_RETRIES = 2  # Number of retries for failed requests
RETRY_DELAY = 0  # Seconds to wait between retries


class JSONDownloadService:
    _instance = None  # Singleton instance

    def __new__(cls, *args, **kwargs):
        if not cls._instance:
            cls._instance = super().__new__(cls)
        return cls._instance

    def __init__(self, save_directory):
        if not hasattr(self, 'initialized'):  # Prevent multiple initializations
            self.bas_directory = save_directory
            os.makedirs(save_directory, exist_ok=True)  # Ensure the directory exists
            self.initialized = True
        self.logger = Logger(__name__).get_logger()

    """
    GOLD SECTION
    """

    def handle_stocks(self):
        timestamp = datetime.now().strftime('%Y%m%d_%H%M%S')
        filename_prefix = "Stock"
        stockPrefix = "Stock_details"
        filePath = self.getLatestFile("lists", stockPrefix)
        # Compare time difference with 72 hours
        if filePath is not None:
            file_timestamp = self.extract_timestamp(filePath)
            time_diff = datetime.now() - file_timestamp

            # Define 72 hours as a timedelta
            seventyTwo_hours = timedelta(hours=72)
            if time_diff <= seventyTwo_hours:
                self.logger.info("Stock list present. Skipping")
                return

        try:
            codeList = nsepython.nse_eq_symbols()
            list_data = []
            for code in codeList:
                list_data.append({
                    'stockCode': code
                })
            filePath = self.getFilePath(stockPrefix, 'lists')
            self.save_json({'date': list_data}, filePath)
        except Exception as ex:
            self.logger.error(f"Error while updating stocks list {ex}")

    def handle_gold(self):
        """
        Scrapes and saves gold rates for Bangalore.
        """

        url = 'https://www.financialexpress.com/gold-rate-today/'
        timestamp = datetime.now().strftime('%Y%m%d_%H%M%S')
        filename_prefix = "Gold"
        filename = f"{filename_prefix}_{timestamp}.json"
        goldPrefix = "Gold_rate"
        filePath = self.getLatestFile("rates", goldPrefix)
        # Compare time difference with six_hours
        if filePath is not None:
            file_timestamp = self.extract_timestamp(filePath)
            time_diff = datetime.now() - file_timestamp

            # Define 6 hours as a timedelta
            six_hours = timedelta(hours=6)
            if time_diff <= six_hours:
                self.logger.info("Gold rates present. Skipping")
                return

        # Request the page content
        response = requests.get(url, headers={
            "User-Agent": "Mozilla/5.0 (Macintosh; Intel Mac OS X 10_15_7) AppleWebKit/537.36 (KHTML, like Gecko) "
                          "Chrome/130.0.0.0 Safari/537.36"
        })

        if response.status_code == 200:
            soup = BeautifulSoup(response.text, 'html.parser')
            tables = soup.find_all('table', {'class': 'common_list full-width'})

            for table in tables:
                rows = table.find_all('tr')
                for row in rows:
                    cells = row.find_all('td')
                    if len(cells) > 2:
                        city = cells[0].get_text(strip=True)
                        if 'Bangalore' in city:
                            # Extract and clean gold rate values
                            eighteen_carat = self.clean_rate(cells[1].get_text(strip=True))
                            twenty_two_carat = self.clean_rate(cells[2].get_text(strip=True))
                            twenty_four_carat = self.clean_rate(cells[3].get_text(strip=True))
                            if all([eighteen_carat, twenty_two_carat, twenty_four_carat]):
                                rate_data = {
                                    "18 Carat": eighteen_carat,
                                    "22 Carat": twenty_two_carat,
                                    "24 Carat": twenty_four_carat
                                }
                                filePath = self.getFilePath(goldPrefix, 'rates')
                                self.save_json(rate_data, filePath)


        else:
            self.logger.error(
                f"Failed to retrieve the page during gold management. Status code: {response.status_code}")

    @staticmethod
    def clean_rate(rate):
        """
        Cleans the gold rate by removing the currency symbol and commas.
        """
        if rate and '(' not in rate:
            return int(rate[1:].replace(",", ""))
        return None

    """
    NPS SECTION
    """

    def handle_nps(self):
        listPrefix = "NPS_details"
        navPrefix = "NPS_rate"
        listUrl = "https://nps.purifiedbytes.com/api/schemes.json"
        navUrl = "https://nps.purifiedbytes.com/api/nav/latest.json"
        if not self.checkJsonInDirectory('lists', listPrefix):
            self.logger.info("Either NPS list file doesn't exist or is older than 6 hours, updating it")
            jsonData = self.make_request(listUrl)
            filePath = self.getFilePath(listPrefix, 'lists')
            self.save_json(jsonData, filePath)
        else:
            self.logger.info("Nps List present. Skipping")
        if not self.checkJsonInDirectory('rates', navPrefix):
            self.logger.info("Either NPS rate file doesn't exist or is older than 6 hours, updating it")
            jsonData = self.make_request(navUrl)
            filePath = self.getFilePath(navPrefix, 'rates')
            self.save_json(jsonData, filePath)
        else:
            self.logger.info("Nps Rates present. Skipping")
        return

    def handle_mf(self):
        listPrefix = "MF_details"
        navPrefix = "MF_rate"
        listUrl = "https://api.mfapi.in/mf"
        if not self.checkJsonInDirectory('lists', listPrefix):
            self.logger.info("Either MF list file doesn't exist or is older than 6 hours, updating it")
            listFilePath = self.getFilePath(listPrefix, 'lists')
            jsonData = self.make_request(listUrl)
            self.save_json(jsonData, listFilePath)
        else:
            self.logger.info("MF List present. Skipping")
        if not self.checkJsonInDirectory('rates', navPrefix):
            self.logger.info("Either MF rate file doesn't exist or is older than 6 hours, updating it")
            latestListFile = self.getLatestFile('lists', listPrefix)
            jsonData = self.buildJsonForMF(listUrl, latestListFile)
            filePath = self.getFilePath(navPrefix, 'rates')
            self.save_json(jsonData, filePath)
        else:
            self.logger.info("MF Rates present. Skipping")
        return

    def buildJsonForMF(self, baseUrl, listPath):
        with open(listPath, 'r') as file:
            data = json.load(file)

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
                except Exception as ex:
                    self.logger.error(f"Error while adding response to the json {ex}")
                    self.logger.error(f"{response}")
            else:
                self.logger.error(f"Skipping invalid response: {response}")

        return {"data": result_data}

    def getFilePath(self, filename_prefix, type):
        new_timestamp = datetime.now().strftime('%Y%m%d_%H%M%S')
        return os.path.join(f"{self.bas_directory}/{type}",
                            f"{filename_prefix}_{new_timestamp}.json")

    def getLatestFile(self, type, filename_prefix):
        # Get all files in the save directory
        files_in_directory = os.listdir(f"{self.bas_directory}/{type}")
        matching_files = [f for f in files_in_directory if f.startswith(filename_prefix)]
        if matching_files:
            most_recent_file = max(matching_files, key=lambda f: self.extract_timestamp(f))
            return os.path.join(f"{self.bas_directory}/{type}/", most_recent_file)
        return None

    def checkJsonInDirectory(self, type, filename_prefix):
        """
                Check if a file with the same prefix exists and is less than 6 hours old.
        """
        try:
            # Get all files in the save directory
            files_in_directory = os.listdir(f"{self.bas_directory}/{type}")
            matching_files = [f for f in files_in_directory if f.startswith(filename_prefix)]
            if matching_files:
                # Sort files by timestamp and find the most recent one
                most_recent_file = max(matching_files, key=lambda f: self.extract_timestamp(f))
                most_recent_file_path = os.path.join(f"{self.bas_directory}/{type}/", most_recent_file)
                # Extract the timestamp from the most recent file's name
                file_timestamp = self.extract_timestamp(most_recent_file)
                time_diff = datetime.now() - file_timestamp

                # Define 6 hours as a timedelta
                six_hours = timedelta(hours=6)
                # Compare time difference with six_hours
                if time_diff <= six_hours:
                    # File is less than 6 hours old, do nothing it
                    return True
                else:
                    # Delete the file
                    os.remove(most_recent_file_path)
                    return False
            return False
        except Exception as e:
            self.logger.error(f"An unexpected error occurred: {str(e)}")
            return False

    @staticmethod
    def make_request(url):
        response = requests.get(url)
        response.raise_for_status()  # Raise an error for bad status codes
        return response.json()  # Parse the JSON response

    def save_json(self, data, file_path):
        """
        Saves the provided data to a JSON file.
        """
        try:
            with open(file_path, 'w', encoding='utf-8') as json_file:
                json.dump(data, json_file, ensure_ascii=False, indent=4)
            self.logger.info(f"File saved successfully at: {file_path}")
        except Exception as e:
            self.logger.error(f"Error saving JSON: {e}")

    @staticmethod
    def extract_timestamp(filename):
        """
        Extracts the timestamp from the filename in the format 'YYYYMMDD_HHMMSS'.
        """
        match = re.search(r'(\d{8}_\d{6})', filename)
        if match:
            return datetime.strptime(match.group(1), '%Y%m%d_%H%M%S')
        else:
            return datetime.min  # Return a very old date if no timestamp is found

    async def fetch_html(self, url: str, session: ClientSession, semaphore: asyncio.Semaphore, **kwargs):
        global requestsProcessed
        retries = 0
        while retries < MAX_RETRIES:
            async with semaphore:  # Control concurrency with semaphore
                try:
                    async with session.get(url, timeout=10, **kwargs) as resp:
                        if resp.status == 200:
                            requestsProcessed += 1
                            if requestsProcessed % 100 == 0:
                                self.logger.info(
                                    f"Request {requestsProcessed} finished in {time.time() - start_time:.2f}s")
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

    async def make_requests(self, urls: list, **kwargs):
        semaphore = asyncio.Semaphore(CONCURRENT_REQUESTS)  # Limit concurrent connections
        connector = TCPConnector(limit_per_host=CONCURRENT_REQUESTS)  # Control simultaneous connections per host
        async with ClientSession(connector=connector) as session:
            tasks = [self.fetch_html(url, session, semaphore, **kwargs) for url in urls]
            results = await asyncio.gather(*tasks, return_exceptions=True)

        return results
