import os
import json

from datetime import datetime, timedelta
import re

from enums.EPGEnum import EPGEnum
from utils.logger import Logger


class JSONDownloadService:
    _instance = None  # Singleton instance
    MfListPrefix: str = "MF_details"
    MfRatePrefix: str = "MF_rate"
    StockListPrefix: str = "Stock_details"
    StockOldDetails: str = "Stock_old_codes"
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

    def getStockList(self):
        fileCheck = self.checkJsonInDirectory(self.listType, self.StockListPrefix)
        if not fileCheck:
            raise FileNotFoundError("Stock file not available right now")
        filepath = self.getLatestFile(self.listType, self.StockListPrefix)
        with open(filepath, 'r') as f:
            jsonData = json.load(f)
        return jsonData

    def checkSymbolChange(self, oldFileName):
        if oldFileName == "SUZLON-BE":
            return "SUZLON" # Corner case
        fileCheck = self.checkJsonInDirectory(self.listType, self.StockOldDetails)
        if not fileCheck:
            raise FileNotFoundError("Stock old symbol not available right now")
        filepath = self.getLatestFile(self.listType, self.StockOldDetails)
        with open(filepath, 'r') as f:
            jsonData = json.load(f)
        return jsonData.get(oldFileName)

    """ Gold methods """

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

    """ NPS methods """

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
            raise FileNotFoundError("NPS Rate file not available right now")
        filepath = self.getLatestFile(self.ratesType, self.NpsRatePrefix)
        with open(filepath, 'r') as f:
            jsonData = json.load(f)
        rateList = jsonData['data']
        result = {}
        for item in rateList:
            if item['scheme_id'] == schemeCode:
                result.update(item)
        npsList = self.getNPSList()['data']
        for item in npsList:
            if item['id'] == schemeCode:
                result.update(item)
        return result

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

    def getMfNameForSchemeId(self, scheme_id):
        mfList = self.getMfList()
        mfList = mfList['data']
        for item in mfList:
            if str(item['schemeCode']) == scheme_id:
                return item['schemeName']
        return ""

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

    """ PPF methods """

    # Common method to EPF and PPF rate
    def getRateForMonth(self, monthString, serviceType):
        if not serviceType or serviceType not in EPGEnum.__members__:
            raise ValueError("Invalid or missing serviceType parameter")
        service_type = EPGEnum[serviceType]
        filepath = None
        if service_type == EPGEnum.EPF:
            filepath = self.getLatestFile(self.ratesType, self.PPFRatePrefix)
            fileCheck = False
            if filepath is not None:
                fileCheck = True

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

    def getTimeStampsOfAllFiles(self):
        return {
            'NPSRate': self.getTimeStamp(self.ratesType, self.NpsRatePrefix),
            'GoldRate': self.getTimeStamp(self.ratesType, self.GoldRatePrefix),
            'MFRate': self.getTimeStamp(self.ratesType, self.MfRatePrefix),
            'EPFRate': self.getTimeStamp(self.ratesType, self.EPFRatePrefix),
            'PPFRate': self.getTimeStamp(self.ratesType, self.PPFRatePrefix),
            'MFDetails': self.getTimeStamp(self.listType, self.MfListPrefix),
            'NPSDetails': self.getTimeStamp(self.listType, self.NpsListPrefix),
            'StockDetails': self.getTimeStamp(self.listType, self.StockListPrefix),
            'StocksOldCode': self.getTimeStamp(self.listType, self.StockOldDetails),
        }

    def getTimeStamp(self, type, filename_prefix):
        try:
            files_in_directory = os.listdir(f"{self.bas_directory}/{type}")
            matching_files = [f for f in files_in_directory if f.startswith(filename_prefix)]
            if matching_files:
                most_recent_file = max(matching_files, key=lambda f: self.extract_timestamp(f))
                file_timestamp = self.extract_timestamp(most_recent_file)
                return file_timestamp
        except Exception as ex:
            self.logger.error(f"Error while getting timestamp {ex}")
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
                if filename_prefix == self.EPFRatePrefix:
                    return True
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
                if filename_prefix == self.StockListPrefix or filename_prefix == self.StockOldDetails:
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

    @staticmethod
    def deleteFile(filePath):
        if filePath is not None:
            os.remove(filePath)
