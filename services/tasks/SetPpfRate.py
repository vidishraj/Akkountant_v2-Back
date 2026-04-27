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
            prompt = (
                "Search the web for PPF (Public Provident Fund) interest rate history in India "
                "from 1999 to present. Find each date when the rate changed and the new rate. "
                "Return JSON with key \"periods\" containing an array of objects, each with "
                "\"from\" (YYYY-MM format, the month the rate took effect) and \"rate\" (float, e.g. 7.1). "
                "Only include rate change points, not monthly entries. "
                "PPF rates changed annually before 2016, then quarterly. "
                "There should be roughly 20-40 entries from 1999-04 to the current quarter."
            )

            jsonData, ai_err = self.fetch_rates_via_ai(prompt)
            if jsonData is None:
                return f"Failed to get PPF Rates via AI: {ai_err}"[:800], "Failed", self.interval
            if 'periods' not in jsonData or len(jsonData['periods']) == 0:
                got_keys = list(jsonData.keys())[:10]
                return f"PPF Rates AI response missing/empty 'periods'. Got top-level keys: {got_keys}"[:800], "Failed", self.interval

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

            ok, err = self.safe_replace_file(filePath, self.jsonService.PPFRatePrefix, self.jsonService.ratesType)
            if not ok:
                return err, "Failed", self.interval
            return 'Completed successfully', "Completed", self.interval
        except Exception as ex:
            return ex.__str__(), "Failed", self.interval
