import os
import asyncio
import json
import time

from aiohttp import ClientSession, ClientConnectorError, TCPConnector, ClientResponseError

from datetime import datetime, timedelta
from bs4 import BeautifulSoup
import re

from enums.EPGEnum import EPGEnum
from utils.logger import Logger
import requests
import nsepython

start_time = None
# Check this while deploying
CONCURRENT_REQUESTS = 200  # Reduce to 50 for stability; adjust based on testing
# To count successful requests
requestsProcessed = 0
# Retry settings
MAX_RETRIES = 2  # Number of retries for failed requests
RETRY_DELAY = 0  # Seconds to wait between retries


class JSONDownloadService:
    _instance = None  # Singleton instance
    MfListPrefix: str = "MF_details"
    MfRatePrefix: str = "MF_rate"
    StockListPrefix: str = "Stock_details"
    # StockRatePrefix: str = "NPS_rate"  #Doesn't exist
    NpsListPrefix: str = "NPS_details"
    NpsRatePrefix: str = "NPS_rate"
    GoldListPrefix: str = "Gold_details"
    GoldRatePrefix: str = "Gold_rate"
    PPFRatePrefix: str = "PPF_rate"
    EPFRatePrefix: str = "EPF_rate"
    listType: str = "lists"
    ratesType: str = "rates"

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

    """ Stocks methods """

    def handle_stocks(self):
        filePath = self.getLatestFile(self.listType, self.StockListPrefix)
        # Compare time difference with 72 hours
        if filePath is not None:
            file_timestamp = self.extract_timestamp(filePath)
            time_diff = datetime.now() - file_timestamp

            # Define 72 hours as a timedelta
            seventyTwo_hours = timedelta(hours=72)
            if time_diff <= seventyTwo_hours:
                return

        try:
            codeList = nsepython.nse_eq_symbols()
            list_data = []
            for code in codeList:
                list_data.append({
                    'stockCode': code
                })
            filePath = self.getFilePath(self.StockListPrefix, self.listType)
            self.save_json({'data': list_data}, filePath)
            self.deleteFile(filePath)
        except Exception as ex:
            self.logger.error(f"Error while updating stocks list {ex}")

    def getStockList(self):
        self.handle_stocks()
        fileCheck = self.checkJsonInDirectory(self.listType, self.StockListPrefix)
        if not fileCheck:
            raise FileNotFoundError("Stock file not available right now")
        filepath = self.getLatestFile(self.listType, self.StockListPrefix)
        with open(filepath, 'r') as f:
            jsonData = json.load(f)
        return jsonData

    """ Gold methods """

    def handle_gold(self):
        """
        Scrapes and saves gold rates for Bangalore.
        """

        url = 'https://www.financialexpress.com/gold-rate-today/'

        filePath = self.getLatestFile(self.ratesType, self.GoldRatePrefix)
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
                                filePath = self.getFilePath(self.GoldRatePrefix, self.ratesType)
                                lastFilePath = self.getLatestFile(self.ratesType,
                                                                  self.GoldRatePrefix)  # To delete later
                                self.save_json(rate_data, filePath)
                                self.deleteFile(lastFilePath)


        else:
            self.logger.error(
                f"Failed to retrieve the page during gold management. Status code: {response.status_code}")

    def getGoldList(self):
        fileCheck = self.checkJsonInDirectory(self.ratesType, self.GoldRatePrefix)
        if not fileCheck:
            raise FileNotFoundError("Gold Rate file not available right now")
        filepath = self.getLatestFile(self.ratesType, self.GoldRatePrefix)
        with open(filepath, 'r') as f:
            jsonData = json.load(f)
        rateList = jsonData
        return rateList

    def getGoldRate(self, schemeCode):
        fileCheck = self.checkJsonInDirectory(self.ratesType, self.GoldRatePrefix)
        if not fileCheck:
            raise FileNotFoundError("Gold Rate file not available right now")
        filepath = self.getLatestFile(self.ratesType, self.GoldRatePrefix)
        with open(filepath, 'r') as f:
            jsonData = json.load(f)
        rateList = jsonData
        for item in rateList:
            if item == schemeCode:
                return rateList[item]
        return 0

    @staticmethod
    def clean_rate(rate):
        """
        Cleans the gold rate by removing the currency symbol and commas.
        """
        if rate and '(' not in rate:
            return int(rate[1:].replace(",", ""))
        return None

    """ NPS methods """

    def handle_nps(self):
        listUrl = "https://nps.purifiedbytes.com/api/schemes.json"
        navUrl = "https://nps.purifiedbytes.com/api/nav/latest.json"
        if not self.checkJsonInDirectory(self.listType, self.NpsListPrefix):
            latestFilePath = self.getLatestFile(self.listType, self.NpsListPrefix)
            self.deleteFile(latestFilePath)
            self.logger.info("Either NPS list file doesn't exist or is older than 6 hours, updating it")
            jsonData = self.make_request(listUrl)
            filePath = self.getFilePath(self.NpsListPrefix, self.listType)
            self.save_json(jsonData, filePath)
        else:
            self.logger.info("Nps List present. Skipping")
        if not self.checkJsonInDirectory(self.ratesType, self.NpsRatePrefix):
            latestFilePath = self.getLatestFile(self.ratesType, self.NpsRatePrefix)
            self.deleteFile(latestFilePath)
            self.logger.info("Either NPS rate file doesn't exist or is older than 6 hours, updating it")
            jsonData = self.make_request(navUrl)
            navList = jsonData.get('data')
            for item in navList:
                scheme_id = item['scheme_id']
                historyAPI = f"https://nps.purifiedbytes.com/api/schemes/{scheme_id}/nav.json"
                historicalData = self.make_request(historyAPI)
                if historicalData is not None and isinstance(historicalData.get('data'), list) and len(
                        historicalData['data']) > 0:
                    item['yesterday'] = historicalData['data'][0]['nav']
                    if len(historicalData['data']) > 6:
                        item['lastWeek'] = historicalData['data'][6]['nav']
                    if len(historicalData['data']) > 179:
                        item['sixMonthsAgo'] = historicalData['data'][179]['nav']

            filePath = self.getFilePath(self.NpsRatePrefix, self.ratesType)
            self.save_json(jsonData, filePath)
        else:
            self.logger.info("Nps Rates present. Skipping")

    def getNPSList(self):
        fileCheck = self.checkJsonInDirectory(self.listType, self.NpsListPrefix)
        if not fileCheck:
            raise FileNotFoundError("MF file not available right now")
        filepath = self.getLatestFile(self.listType, self.NpsListPrefix)
        with open(filepath, 'r') as f:
            jsonData = json.load(f)
        return jsonData

    def getNPSListDetailsForScheme(self, schemeCode):
        fileCheck = self.checkJsonInDirectory(self.listType, self.NpsListPrefix)
        if not fileCheck:
            raise FileNotFoundError("MF file not available right now")
        filepath = self.getLatestFile(self.listType, self.NpsListPrefix)
        with open(filepath, 'r') as f:
            jsonData = json.load(f)
        detailsList = jsonData['data']
        for item in detailsList:
            if item['id'] == schemeCode:
                return item
        return {}

    def getNPSRate(self, schemeCode):
        fileCheck = self.checkJsonInDirectory(self.ratesType, self.NpsRatePrefix)
        if not fileCheck:
            raise FileNotFoundError("MF Rate file not available right now")
        filepath = self.getLatestFile(self.ratesType, self.NpsRatePrefix)
        with open(filepath, 'r') as f:
            jsonData = json.load(f)
        rateList = jsonData['data']
        for item in rateList:
            if item['scheme_id'] == schemeCode:
                return item
        return {}

    def getNpsSchemeCodeSchemeName(self, schemeName: str):
        jsonData = self.getNPSList()
        # We will be
        maxSimilarity = 0
        selected = None
        for item in jsonData['data']:
            similarity = self.compareStrings(item['name'], schemeName.upper())
            if maxSimilarity < similarity:
                maxSimilarity = similarity
                selected = item['id']
        return selected

    """ MF methods """

    def handle_mf(self):
        listUrl = "https://api.mfapi.in/mf"
        if not self.checkJsonInDirectory(self.listType, self.MfListPrefix):
            self.logger.info("Either MF list file doesn't exist or is older than 6 hours, updating it")
            listFilePath = self.getFilePath(self.MfListPrefix, self.listType)
            jsonData = self.make_request(listUrl)
            self.save_json({'data': jsonData}, listFilePath)
        else:
            self.logger.info("MF List present. Skipping")
        if not self.checkJsonInDirectory(self.ratesType, self.MfRatePrefix):
            self.logger.info("Either MF rate file doesn't exist or is older than 6 hours, updating it")
            latestListFile = self.getLatestFile(self.listType, self.MfListPrefix)
            jsonData = self.buildJsonForMF(listUrl, latestListFile)
            filePath = self.getFilePath(self.MfRatePrefix, self.ratesType)
            self.save_json(jsonData, filePath)
        else:
            self.logger.info("MF Rates present. Skipping")

    def getMfList(self):
        fileCheck = self.checkJsonInDirectory(self.listType, self.MfListPrefix)
        if not fileCheck:
            raise FileNotFoundError("MF file not available right now")
        filepath = self.getLatestFile(self.listType, self.MfListPrefix)
        with open(filepath, 'r') as f:
            jsonData = json.load(f)
        return jsonData

    def getMFRate(self, schemeCode):
        fileCheck = self.checkJsonInDirectory(self.ratesType, self.MfRatePrefix)
        if not fileCheck:
            raise FileNotFoundError("MF Rate file not available right now")
        filepath = self.getLatestFile(self.ratesType, self.MfRatePrefix)
        with open(filepath, 'r') as f:
            jsonData = json.load(f)
        rateList = jsonData['data']
        for item in rateList:
            if item['scheme_id'] == schemeCode:
                return item
        return {}

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
                except Exception as ex:
                    self.logger.error(f"Error while adding response to the json {ex}")
                    self.logger.error(f"{response}")
            else:
                self.logger.error(f"Skipping invalid response: {response}")

        return {"data": result_data}

    """ PPF methods """

    def handle_ppf(self):
        # Fetch the HTML content from the URL
        try:
            url = "https://www.nsiindia.gov.in/InternalPage.aspx?Id_Pk=178"  # Replace with the actual URL
            if not self.checkJsonInDirectory(self.ratesType, self.PPFRatePrefix):
                response = requests.get(url, verify=False)
                if response.status_code != 200:
                    raise ClientResponseError(f"Failed to fetch page, status code: {response.status_code}")
                # Parse the HTML content using BeautifulSoup
                soup = BeautifulSoup(response.content, 'html.parser')

                # Find the table with relevant data
                table = soup.find('table', attrs={'border': '1', 'cellspacing': "1",
                                                  'cellpadding': "8"})  # Targeting the table with border="1"

                # Extract table rows
                rows = table.find_all('tr')

                # Extract data from each row
                interest_data = []
                for row in rows[1:]:  # Skip the header row
                    columns = row.find_all('td')
                    year = columns[0].get_text(strip=True)  # Extract the year period
                    rate = columns[1].get_text(strip=True)  # Extract the interest rate
                    interest_data.append({"Year": year, "Interest Rate": rate})
                interest_data = self.parse_interest_data(interest_data)
                filePath = self.getFilePath(self.PPFRatePrefix, self.ratesType)
                self.save_json({'data': interest_data}, filePath)
                self.logger.info("PPF rates saved successfully.")
            else:
                self.logger.info("PPF List present. Skipping")
        except Exception as ex:
            self.logger.error(f"Error while handling ppf {ex}")

    def parse_interest_data(self, data):
        result = []

        for entry in data:
            year_range = entry['Year']
            interest_rate = float(entry['Interest Rate'])
            try:
                # Case 1: Date range format like "01.04.1999 TO 14.01.2000"
                if 'TO' in year_range and '.' in year_range:
                    start_str, end_str = year_range.split(' TO ')
                    # Weird ass government website. Fucking 31 days in June
                    if end_str == "31.06.2019":
                        end_str = "30.06.2019"
                    start_date = datetime.strptime(start_str, "%d.%m.%Y")
                    end_date = datetime.strptime(end_str, "%d.%m.%Y")

                    # Process each month within the range
                    current_date = start_date
                    while current_date <= end_date:
                        if current_date.year >= 1999:  # Filter based on year
                            result.append({
                                'Year': current_date.strftime("%Y-%m"),
                                'Interest Rate': interest_rate
                            })
                        # Move to the first day of the next month
                        next_month = current_date.replace(day=28) + timedelta(days=4)
                        current_date = next_month.replace(day=1)

                # Case 2: Fiscal year format like "1986-87 TO 1998-99"
                elif 'TO' in year_range and '-' in year_range:
                    start_range, end_range = year_range.split(' TO ')
                    start_year = int(start_range.split('-')[0])
                    end_year = int(end_range.split('-')[1]) + (start_year // 100) * 100

                    for year in range(start_year, end_year + 1):
                        start_date = datetime(year, 4, 1)  # Fiscal year starts in April
                        end_date = datetime(year + 1, 3, 31)

                        current_date = start_date
                        while current_date <= end_date:
                            if current_date.year >= 1999:
                                result.append({
                                    'Year': current_date.strftime("%Y-%m"),
                                    'Interest Rate': interest_rate
                                })
                            next_month = current_date.replace(day=28) + timedelta(days=4)
                            current_date = next_month.replace(day=1)

                # Case 3: Single fiscal year like "1983-84"
                elif '-' in year_range:
                    start_year = int(year_range.split('-')[0])
                    end_year = start_year + 1

                    start_date = datetime(start_year, 4, 1)
                    end_date = datetime(end_year, 3, 31)

                    current_date = start_date
                    while current_date <= end_date:
                        if current_date.year >= 1999:
                            result.append({
                                'Year': current_date.strftime("%Y-%m"),
                                'Interest Rate': interest_rate
                            })
                        next_month = current_date.replace(day=28) + timedelta(days=4)
                        current_date = next_month.replace(day=1)
            except Exception as ex:
                self.logger.error(f'Error parsing this date in PPF handling {ex}- {year_range}')
        return result

    # Common method to EPF and PPF rate
    def getRateForMonth(self, monthString, serviceType):
        if not serviceType or serviceType not in EPGEnum.__members__:
            raise ValueError("Invalid or missing serviceType parameter")
        service_type = EPGEnum[serviceType]
        filepath = None
        if service_type == EPGEnum.EPF:
            filepath = self.getLatestFile(self.ratesType, self.EPFRatePrefix)
            fileCheck = self.checkJsonInDirectory(self.ratesType, self.EPFRatePrefix)
        elif service_type == EPGEnum.PF:
            filepath = self.getLatestFile(self.ratesType, self.PPFRatePrefix)
            fileCheck = self.checkJsonInDirectory(self.ratesType, self.PPFRatePrefix)
        if not fileCheck:
            raise FileNotFoundError("EPF or PF rate file not available right now")
        with open(filepath, 'r') as f:
            jsonData = json.load(f)
        rateList = jsonData['data']
        for item in rateList:
            if item['Year'] == monthString:
                return item['Interest Rate']
        return {}

    def getPPFRateFile(self):
        filepath = self.getLatestFile(self.ratesType, self.PPFRatePrefix)
        fileCheck = self.checkJsonInDirectory(self.ratesType, self.PPFRatePrefix)
        if not fileCheck:
            raise FileNotFoundError("PPF Rate file not available right now")
        with open(filepath, 'r') as f:
            jsonData = json.load(f)
        rateList = jsonData['data']
        return rateList

    def getEPFRateFile(self):
        filepath = self.getLatestFile(self.ratesType, self.EPFRatePrefix)
        fileCheck = self.checkJsonInDirectory(self.ratesType, self.EPFRatePrefix)
        if not fileCheck:
            raise FileNotFoundError("EPF Rate file not available right now")
        with open(filepath, 'r') as f:
            jsonData = json.load(f)
        rateList = jsonData['data']
        return rateList

    """ Utility methods """

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
                td = None
                # Define timedelta based on the investment type
                if filename_prefix == self.PPFRatePrefix:
                    # Update PPF rate once in 3 months
                    td = timedelta(days=89)
                if filename_prefix == self.GoldRatePrefix:
                    td = timedelta(days=1)
                if filename_prefix == self.NpsListPrefix or filename_prefix == self.NpsRatePrefix:
                    td = timedelta(days=1)
                if filename_prefix == self.MfListPrefix or filename_prefix == self.MfRatePrefix:
                    td = timedelta(days=30)
                if filename_prefix == self.StockListPrefix:
                    td = timedelta(days=20)
                # Compare time difference
                if time_diff <= td:
                    # do nothing it
                    return True
                else:
                    # Delete the file
                    self.deleteFile(most_recent_file_path)
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

    @staticmethod
    def compareStrings(str1, str2):
        words1 = str1.split()
        words2 = str2.split()

        # Check if the first words match
        if not words1 or not words2 or words1[-1] != words2[-1]:
            return False

        # Calculate Jaccard similarity for the rest of the words
        set1 = set(words1)
        set2 = set(words2)
        intersection = set1.intersection(set2)
        union = set1.union(set2)

        jaccard_similarity = len(intersection) / len(union)
        return jaccard_similarity

    """ Asynchronous methods for MF rate fetching """

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

    @staticmethod
    def deleteFile(filePath):
        if filePath is not None:
            os.remove(filePath)
