import os
from datetime import datetime

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
            # 4 days in minutes
            self.interval = 6000
            self.initialized = True

    @staticmethod
    def _expand_periods_to_monthly(periods):
        """Expand rate change periods into monthly entries."""
        data = []
        now = datetime.now()
        current_ym = f"{now.year}-{now.month:02d}"

        for i, period in enumerate(periods):
            rate = period['rate']
            start = period['from']  # "YYYY-MM"

            if i + 1 < len(periods):
                end = periods[i + 1]['from']  # next period's start
            else:
                # Last period: extend to current month (inclusive)
                end = None

            year, month = int(start[:4]), int(start[5:7])
            while True:
                ym = f"{year}-{month:02d}"
                if end and ym >= end:
                    break
                if not end and ym > current_ym:
                    break
                data.append({"Year": ym, "Interest Rate": rate})
                month += 1
                if month > 12:
                    month = 1
                    year += 1

        return data

    def run(self):
        try:
            prompt = """Search the web for PPF (Public Provident Fund) interest rate history in India.
Find when the rate changed and what it changed to, from 1999 onwards.

Return a JSON object listing ONLY the rate change points (NOT monthly entries):

{
  "periods": [
    {"from": "1999-04", "rate": 12.0},
    {"from": "2000-04", "rate": 11.0},
    {"from": "2001-04", "rate": 9.5},
    {"from": "2002-04", "rate": 9.0},
    {"from": "2003-04", "rate": 8.0},
    ...continue with each rate change...
    {"from": "2025-04", "rate": 7.1}
  ]
}

Rules:
- "from" is the month the new rate took effect, in "YYYY-MM" format
- "rate" is a float (e.g. 7.1, not "7.1%")
- Include every rate change from April 1999 to the current quarter
- PPF rates are set by the Ministry of Finance, typically quarterly since 2016
- Before 2016, rates changed annually or less frequently
- Use official government/RBI sources
- There should be roughly 20-40 rate change entries total"""

            jsonData = self.fetch_rates_via_ai(prompt)
            if not jsonData or 'periods' not in jsonData or len(jsonData['periods']) == 0:
                return "Failed to get PPF Rates via AI", "Failed", self.interval

            # Expand rate change periods into monthly entries
            monthly_data = self._expand_periods_to_monthly(jsonData['periods'])
            if not monthly_data:
                return "Failed to expand PPF rate periods", "Failed", self.interval

            self.logger.info(f"Expanded {len(jsonData['periods'])} rate periods into {len(monthly_data)} monthly entries")
            output = {"data": monthly_data}

            filePath = os.path.join(self.tmp_dir, 'PPFRate.json')
            try:
                os.remove(filePath)
            except OSError:
                pass
            self.save_json(output, filePath)

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
