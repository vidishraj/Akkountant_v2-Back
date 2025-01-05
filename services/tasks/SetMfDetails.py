import os

from services.tasks.baseTask import BaseTask
from utils.logger import Logger


class SetMFDetails(BaseTask):
    _instance = None

    def __new__(cls, *args, **kwargs):
        if not cls._instance:
            cls._instance = super(SetMFDetails, cls).__new__(cls)
        return cls._instance

    def __init__(self, title, priority):
        if not hasattr(self, 'initialized'):  # Prevent multiple initializations
            super().__init__(title, priority)

            self.logger = Logger(__name__).get_logger()
            # 4 hours
            self.interval = 600

    def run(self):
        try:
            # Delete existing file if it exists, else
            listUrl = "https://api.mfapi.in/mf"
            jsonData = self.make_request(listUrl)
            jsonData = {'data': jsonData}
            try:
                filePath = self.tmp_dir + 'MFDetails.json'
                # delete file if it exists
                try:
                    os.remove(filePath)
                except OSError:
                    pass
                self.save_json(jsonData, filePath)

                # get the latest rate file in assets
                latestFile = self.jsonService.getLatestFile(self.jsonService.listType, self.jsonService.MfListPrefix)

                latestFilePath = self.jsonService.getFilePath(self.jsonService.MfListPrefix, self.jsonService.listType)

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
