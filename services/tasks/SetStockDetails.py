import os

from services.tasks.baseTask import BaseTask

from dotenv import load_dotenv

from utils.logger import Logger

load_dotenv()
if os.getenv('ENV') == "PROD":
    import nsepythonserver as nsepython
else:
    import nsepython


class SetStockDetails(BaseTask):
    _instance = None

    def __new__(cls, *args, **kwargs):
        if not cls._instance:
            cls._instance = super(SetStockDetails, cls).__new__(cls)
        return cls._instance

    def __init__(self, title, priority):
        if not hasattr(self, 'initialized'):  # Prevent multiple initializations
            super().__init__(title, priority)

            self.logger = Logger(__name__).get_logger()
            # 4 hours
            self.interval = 320

    def run(self):
        try:
            codeList = nsepython.nse_eq_symbols()
            list_data = []
            for code in codeList:
                list_data.append({
                    'stockCode': code
                })
            jsonData = {'data': list_data}
            try:
                filePath = self.tmp_dir + 'StockDetails.json'
                # delete file if it exists
                try:
                    os.remove(filePath)
                except OSError:
                    pass
                self.save_json(jsonData, filePath)

                # get the latest rate file in assets
                latestFile = self.jsonService.getLatestFile(self.jsonService.listType, self.jsonService.StockListPrefix)

                latestFilePath = self.jsonService.getFilePath(self.jsonService.StockListPrefix,
                                                              self.jsonService.listType)

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
