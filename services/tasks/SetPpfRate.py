import os
from datetime import datetime, timedelta

import requests
from aiohttp import ClientResponseError
from bs4 import BeautifulSoup

from services.tasks.baseTask import BaseTask
from utils.logger import Logger


class SetPPFRate(BaseTask):
    _instance = None

    def __new__(cls, *args, **kwargs):
        if not cls._instance:
            cls._instance = super(SetPPFRate, cls).__new__(cls)
        return cls._instance

    def __init__(self, title, priority):
        if not hasattr(self, 'initialized'):  # Prevent multiple initializations
            super().__init__(title, priority)
            self.logger = Logger(__name__).get_logger()
            # 4 hours
            self.interval = 6000

    def run(self):
        try:
            # Delete existing file if it exists, else
            jsonData = self.getPPFRates()
            if len(jsonData) == 0:
                return "Failed to get PPF Rates", "Failed", self.interval
            else:
                jsonData = {'data': jsonData}
            try:
                filePath = self.tmp_dir + 'PPFRate.json'
                # delete file if it exists
                try:
                    os.remove(filePath)
                except OSError:
                    pass
                self.save_json(jsonData, filePath)

                # get the latest rate file in assets
                latestFile = self.jsonService.getLatestFile(self.jsonService.ratesType, self.jsonService.PPFRatePrefix)

                latestFilePath = self.jsonService.getFilePath(self.jsonService.PPFRatePrefix,
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

    def getPPFRates(self):
        try:
            url = "https://www.nsiindia.gov.in/InternalPage.aspx?Id_Pk=178"  # Replace with the actual URL
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
            self.logger.info("PPF rates saved successfully.")
            return interest_data
        except Exception as ex:
            self.logger.error(f"Error while handling ppf {ex}")
            return []

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
