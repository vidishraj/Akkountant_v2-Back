"""
This file holds the Base class to manage Mutual funds, stocks and NPS data.

"""
from abc import abstractmethod

from werkzeug.routing import ValidationError

from utils.DateTimeUtil import DateTimeUtil
from utils.GenericUtils import GenericUtil
from flask_sqlalchemy import SQLAlchemy
from utils.logger import Logger
from flask import g

import os
import datetime
import json
import requests


class Base_MSN:
    baseAPIURL: str
    db: SQLAlchemy
    logger: Logger
    genericUtil: GenericUtil
    baseDirectory: str = f'{os.getcwd()}/services/'

    @property
    def db(self):
        """Retrieve the database session from the Flask global `g`."""
        return g.db

    def __init__(self):
        """
        Each will be fetched from a base API url
        """
        self.genericUtil = GenericUtil()
        self.dateTimeUtil = DateTimeUtil()

    @abstractmethod
    def fetchAllSecurities(self):
        """

        :return: List of securities without any pagiantion
        """
        pass

    @abstractmethod
    def findSecurity(self, securityCode):
        """
        :return: Security without any pagiantion
        """
        pass

    def buySecurity(self, security_data, filePath: str, key: str, userId):
        """

        :param security_data: Code of security to buy
        :param filePath: path to json
        :param userId: User_ID to add info for
        :param key: key in json to check
        :return: Success or throws error
        """
        pass

    def sellSecurity(self, securityCode, userId):
        """

        :param securityCode: Code of security to sell
        :param userId: User_ID to add info for
        :return: Success or throws error
        """
        pass

    def updateSecurity(self, securityCode, userId):
        """

        :param securityCode: Code of security to update
        :param userId: User_ID to add info for
        :return: Success or throws error
        """
        pass

    @staticmethod
    def validate_security_in_json(filePath: str, key: str, securityCode: str):
        try:
            # Step 1: Load the JSON file
            with open(filePath, 'r') as file:
                json_data = json.load(file)

            # Step 2: Check if the securityCode exists under the specified key
            security_found = any(item.get(key) == securityCode for item in json_data)

            if not security_found:
                raise ValidationError(f"Security code '{securityCode}' not found in the provided JSON file.")

        except FileNotFoundError:
            raise ValidationError(f"File at '{filePath}' not found.")
        except json.JSONDecodeError:
            raise ValidationError("Invalid JSON format in the provided file.")

    def check_and_update_file(self, directory, base_filename, endpoint_url):
        try:
            # Find the file with the base filename and timestamp
            file_path = self.find_file_with_timestamp(directory, base_filename)

            # If the file exists
            if file_path:
                # Extract the timestamp from the filename
                file_name = os.path.basename(file_path)
                timestamp_str = self.extract_timestamp(file_name)

                # Convert the timestamp string to a datetime object
                file_timestamp = datetime.datetime.strptime(timestamp_str, '%Y%m%d%H%M%S')

                # Check if the file is older than a day
                if datetime.datetime.now() - file_timestamp > datetime.timedelta(days=1):
                    # Update the file with new data from the endpoint
                    return self.update_file(directory, base_filename, endpoint_url)
                else:
                    # File is recent, return its contents
                    with open(file_path, 'r') as file:
                        return json.load(file)
            else:
                # No file exists, fetch and save new data
                return self.update_file(directory, base_filename, endpoint_url)

        except Exception as e:
            return {"error": str(e)}

    @staticmethod
    def find_file_with_timestamp(directory, base_filename):
        # List all files in the directory and search for the base filename
        for file in os.listdir(directory):
            if file.startswith(base_filename) and file.endswith('.json'):
                return os.path.join(directory, file)
        return None

    @staticmethod
    def extract_timestamp(file_name):
        # Assuming the filename format is 'base_filename_YYYYMMDDHHMMSS.json'
        return file_name.split('_')[-1].split('.')[0]

    @staticmethod
    def update_file(directory, base_filename, endpoint_url):
        response = requests.get(endpoint_url)
        if response.status_code == 200:
            # Create the new file with the current timestamp in the filename
            current_timestamp = datetime.datetime.now().strftime('%Y%m%d%H%M%S')
            new_file_path = os.path.join(directory, f"{base_filename}_{current_timestamp}.json")

            # Save the response data to the file
            with open(new_file_path, 'w') as file:
                json.dump(response.json(), file)

            return response.json()
        else:
            raise Exception(f"Failed to update file. Status code: {response.status_code}")
