import json
import os

from services.tasks.baseTask import BaseTask
from dotenv import load_dotenv
import pandas as pd
import subprocess

from utils.logger import Logger

load_dotenv()
if os.getenv('ENV') == "PROD":
    import nsepythonserver as nsepython
else:
    import nsepython


class SetStocksOldDetails(BaseTask):
    _instance = None

    def __new__(cls, *args, **kwargs):
        if not cls._instance:
            cls._instance = super(SetStocksOldDetails, cls).__new__(cls)
        return cls._instance

    def __init__(self, title, priority):
        if not hasattr(self, 'initialized'):  # Prevent multiple initializations
            super().__init__(title, priority)
            self.logger = Logger(__name__).get_logger()
            # 4 hours
            self.interval = 360

    def run(self):
        try:
            # Delete existing file if it exists, else
            csvFilePath = os.path.join(self.tmp_dir, 'symbolchange.csv')
            status = self.downloadOldSymbolFile("https://nsearchives.nseindia.com/content/equities/symbolchange.csv",
                                                csvFilePath)
            if not status:
                return "Error saving csv file", "Failed"
            try:
                filePath = self.tmp_dir + 'StocksOldCode.json'
                # delete file if it exists
                try:
                    os.remove(filePath)
                except OSError:
                    pass

                self.saveStocksOldSymbolJson(csvFilePath, filePath, 1, 2)

                # get the latest rate file in assets
                latestFile = self.jsonService.getLatestFile(self.jsonService.listType, self.jsonService.StockOldDetails)

                latestFilePath = self.jsonService.getFilePath(self.jsonService.StockOldDetails, self.jsonService.listType)

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

    def downloadOldSymbolFile(self, url, save_path):
        """
        Downloads a CSV file from the given URL and saves it locally.

        :param url: URL of the CSV file.
        :param save_path: Path to save the downloaded file.
        """
        try:
            # Build the curl command
            curl_headers = nsepython.curl_headers
            payload_var = f'curl -b cookies.txt "{url}" {curl_headers}'

            # Execute the curl command
            result = subprocess.run(payload_var, shell=True, stdout=subprocess.PIPE, stderr=subprocess.PIPE)

            # Check for errors in execution
            if result.returncode != 0:
                self.logger.error(f"Error during curl execution: {result.stderr.decode(errors='ignore')}")
                return False

            # Save the output as binary data
            with open(save_path, 'wb') as file:
                file.write(result.stdout)

            self.logger.info(f"CSV file downloaded successfully from {url} to {save_path}.")
            return True
        except Exception as e:
            self.logger.error(f"Error downloading CSV: {e}")
            return False

    def saveStocksOldSymbolJson(self, csv_file_path, json_file_path, key_col, value_col, encoding='ISO-8859-1'):
        """
        Converts a CSV file into a JSON file with specified columns as key-value pairs.

        :param csv_file_path: Path to the input CSV file.
        :param json_file_path: Path to the output JSON file.
        :param key_col: Index of the column to be used as keys.
        :param value_col: Index of the column to be used as values.
        :param encoding: Encoding of the CSV file.
        """
        try:
            # Read the CSV file
            data = pd.read_csv(csv_file_path, encoding=encoding)

            # Create a dictionary from the specified columns
            result = dict(zip(data.iloc[:, key_col], data.iloc[:, value_col]))

            # Write the dictionary to a JSON file
            with open(json_file_path, 'w') as json_file:
                json.dump(result, json_file, indent=4)

            self.logger.info(f"JSON file created successfully at {json_file_path}.")
        except Exception as e:
            self.logger.error(f"Error: {e}")
