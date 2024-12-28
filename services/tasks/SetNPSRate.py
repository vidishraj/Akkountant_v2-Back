import os

from services.tasks.baseTask import BaseTask
from utils.logger import Logger


class SetNPSRate(BaseTask):
    _instance = None

    def __new__(cls, *args, **kwargs):
        if not cls._instance:
            cls._instance = super(SetNPSRate, cls).__new__(cls)
        return cls._instance

    def __init__(self, title, priority):
        if not hasattr(self, 'initialized'):  # Prevent multiple initializations
            super().__init__(title, priority)
            self.logger = Logger(__name__).get_logger()
            # 4 hours
            self.interval = 60

    def run(self):
        try:
            # Delete existing file if it exists, else
            navUrl = "https://nps.purifiedbytes.com/api/nav/latest.json"
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
            try:
                filePath = self.tmp_dir + 'NPSRATE.json'
                # delete file if it exists
                try:
                    os.remove(filePath)
                except OSError:
                    pass
                self.save_json(jsonData, filePath)

                # get the latest rate file in assets
                latestFile = self.jsonService.getLatestFile(self.jsonService.ratesType, self.jsonService.NpsRatePrefix)

                latestFilePath = self.jsonService.getFilePath(self.jsonService.NpsRatePrefix, self.jsonService.ratesType)

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
