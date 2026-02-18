import os

from services.tasks.AIRateTask import AIRateTask
from utils.logger import Logger


class SetPPFRate(AIRateTask):
    _instance = None

    def __new__(cls, *args, **kwargs):
        if not cls._instance:
            cls._instance = super(SetPPFRate, cls).__new__(cls)
        return cls._instance

    def __init__(self, title, priority):
        if not hasattr(self, 'initialized'):
            super().__init__(title, priority)
            self.logger = Logger(__name__).get_logger()
            # 4 hours
            self.interval = 6000
            self.initialized = True

    def run(self):
        try:
            prompt = """Search the web for the current and historical PPF (Public Provident Fund) interest rates
in India from 1999 onwards. The official source is the National Savings Institute (NSI India) website.

PPF rates change quarterly or at specific date ranges. For each period a rate was in effect,
expand it into monthly entries.

Return a JSON object with this EXACT structure:

{
  "data": [
    {"Year": "1999-04", "Interest Rate": 12.0},
    {"Year": "1999-05", "Interest Rate": 12.0},
    ...
    {"Year": "2025-01", "Interest Rate": 7.1},
    ...up to the current month...
  ]
}

Rules:
- "Year" is in "YYYY-MM" format
- "Interest Rate" is a float (e.g. 7.1, not "7.1%")
- Start from April 1999 (1999-04) or the earliest available date from 1999
- Include ALL months from the start date through the current month
- Each rate should be applied to every month it was in effect
- Use the most recent official government rate for the current quarter
- PPF rates are typically announced quarterly by the Ministry of Finance"""

            jsonData = self.fetch_rates_via_ai(prompt)
            if not jsonData or 'data' not in jsonData or len(jsonData['data']) == 0:
                return "Failed to get PPF Rates via AI", "Failed", self.interval

            filePath = os.path.join(self.tmp_dir, 'PPFRate.json')
            try:
                os.remove(filePath)
            except OSError:
                pass
            self.save_json(jsonData, filePath)

            latestFile = self.jsonService.getLatestFile(self.jsonService.ratesType, self.jsonService.PPFRatePrefix)
            latestFilePath = self.jsonService.getFilePath(self.jsonService.PPFRatePrefix,
                                                          self.jsonService.ratesType)
            fileMoved = self.move_file(filePath, latestFilePath)

            if fileMoved:
                self.jsonService.deleteFile(latestFile)
            else:
                return 'Failed to move file', "Failed", self.interval
            return 'Completed successfully', "Completed", self.interval
        except Exception as ex:
            return ex.__str__(), "Failed", self.interval
