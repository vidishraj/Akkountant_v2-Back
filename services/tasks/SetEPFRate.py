import os

from services.tasks.AIRateTask import AIRateTask
from utils.logger import Logger


class SetEPFRate(AIRateTask):
    _instance = None

    def __new__(cls, *args, **kwargs):
        if not cls._instance:
            cls._instance = super(SetEPFRate, cls).__new__(cls)
        return cls._instance

    def __init__(self, title, priority):
        if not hasattr(self, 'initialized'):
            super().__init__(title, priority)
            self.logger = Logger(__name__).get_logger()
            # 7 days in minutes
            self.interval = 10080
            self.initialized = True

    def run(self):
        try:
            prompt = """Search the web for the current EPF (Employee Provident Fund) interest rate in India,
and find the historical EPF interest rates from financial year 2014-15 onwards.

EPF rates are set annually by the EPFO for each financial year (April to March).
The rate applies to all months within that financial year.

Return a JSON object with this EXACT structure — expand each financial year into monthly entries
from April (YYYY-04) through March (next year, YYYY-03):

{
  "data": [
    {"Year": "2014-04", "Interest Rate": 8.75},
    {"Year": "2014-05", "Interest Rate": 8.75},
    ...through to...
    {"Year": "2015-03", "Interest Rate": 8.75},
    {"Year": "2015-04", "Interest Rate": 8.80},
    ...and so on for each FY up to the current one...
  ]
}

Rules:
- "Year" is in "YYYY-MM" format
- "Interest Rate" is a float (e.g. 8.25, not "8.25%")
- Each financial year's rate applies from April to March of the next year
- Include ALL months from April 2014 through the current month
- Use the most recent official EPFO rate for the current financial year"""

            jsonData = self.fetch_rates_via_ai(prompt)
            if not jsonData or 'data' not in jsonData or len(jsonData['data']) == 0:
                return "Failed to get EPF Rates via AI", "Failed", self.interval

            filePath = os.path.join(self.tmp_dir, 'EPFRate.json')
            try:
                os.remove(filePath)
            except OSError:
                pass
            self.save_json(jsonData, filePath)

            latestFile = self.jsonService.getLatestFile(self.jsonService.ratesType, self.jsonService.EPFRatePrefix)
            latestFilePath = self.jsonService.getFilePath(self.jsonService.EPFRatePrefix,
                                                          self.jsonService.ratesType)
            fileMoved = self.move_file(filePath, latestFilePath)

            if fileMoved:
                self.jsonService.deleteFile(latestFile)
            else:
                return 'Failed to move file', "Failed", self.interval
            return 'Completed successfully', "Completed", self.interval
        except Exception as ex:
            return ex.__str__(), "Failed", self.interval
