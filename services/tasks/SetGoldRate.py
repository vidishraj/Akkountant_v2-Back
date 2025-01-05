import os
from datetime import datetime, timedelta

import requests
from bs4 import BeautifulSoup

from services.tasks.baseTask import BaseTask
from utils.logger import Logger


class SetGoldRate(BaseTask):
    _instance = None

    def __new__(cls, *args, **kwargs):
        if not cls._instance:
            cls._instance = super(SetGoldRate, cls).__new__(cls)
        return cls._instance

    def __init__(self, title, priority):
        if not hasattr(self, 'initialized'):  # Prevent multiple initializations
            super().__init__(title, priority)
            self.logger = Logger(__name__).get_logger()
            # 4 hours
            self.interval = 300

    def run(self):
        try:
            # Delete existing file if it exists, else
            jsonData = self.getGoldData()
            try:
                filePath = self.tmp_dir + 'GOLDRATE.json'
                # delete file if it exists
                try:
                    os.remove(filePath)
                except OSError:
                    pass
                self.save_json(jsonData, filePath)

                # get the latest rate file in assets
                latestFile = self.jsonService.getLatestFile(self.jsonService.ratesType, self.jsonService.GoldRatePrefix)

                latestFilePath = self.jsonService.getFilePath(self.jsonService.GoldRatePrefix,
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

    def getGoldData(self):
        """
                Scrapes and saves gold rates for Bangalore.
                """

        url = 'https://www.financialexpress.com/gold-rate-today/'
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
                                return rate_data

        else:
            self.logger.error(
                f"Failed to retrieve the page during gold management. Status code: {response.status_code}")
            raise ConnectionError("Gold Rate Network Error")

    @staticmethod
    def clean_rate(rate):
        """
        Cleans the gold rate by removing the currency symbol and commas.
        """
        if rate and '(' not in rate:
            return int(rate[1:].replace(",", ""))
        return None
