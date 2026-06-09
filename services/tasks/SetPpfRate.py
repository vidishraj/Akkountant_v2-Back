import os
from datetime import datetime

from services.tasks.AIRateTask import AIRateTask
from utils.logger import Logger


# PPF historical interest rates. Wikipedia's "Public Provident Fund (India)"
# page maintains a rate history table that's updated within days of any
# rate change. PPF rates changed annually before 2016 then quarterly after
# the rate-reset framework took effect.
PPF_URL = "https://en.wikipedia.org/wiki/Public_Provident_Fund_(India)"

PPF_SYSTEM_PROMPT = """You are a data extraction assistant. You will receive
the HTML of a Wikipedia page with a table of historical PPF (Public Provident
Fund) interest rates in India from 1999 through the present.

Extract each date when the PPF rate changed, plus the new rate. Return only
the rate-change points (not monthly entries).

Output format for each entry:
- "from" is in "YYYY-MM" format — the first month the rate took effect
- "rate" is a float (e.g. 7.1)

PPF rate cadence:
- Annual changes before 2016 (~one entry per fiscal year)
- Quarterly changes from 2016 onwards (~four entries per year, in
  Apr/Jul/Oct/Jan)

You should produce roughly 20-40 entries from 1999-04 through the current
quarter.

IMPORTANT: Do not use any tools to search or filter the page content. The
entire relevant HTML is already provided in the user message below. Read it
directly and emit the structured output."""


PPF_SCHEMA = {
    "type": "object",
    "properties": {
        "periods": {
            "type": "array",
            "items": {
                "type": "object",
                "properties": {
                    "from": {"type": "string"},
                    "rate": {"type": "number"},
                },
                "required": ["from", "rate"],
            },
        }
    },
    "required": ["periods"],
}


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
        """Expand rate change periods into monthly entries.

        Pure data transform; lifted from v1 unchanged so the on-disk
        PPFRate.json contract (consumed by the read path) stays identical.
        """
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
            jsonData, ai_err = self.fetch_rates_via_ai(
                url=PPF_URL,
                system_prompt=PPF_SYSTEM_PROMPT,
                schema=PPF_SCHEMA,
                agent="rate.ppf",
            )
            if jsonData is None:
                return f"Failed to get PPF Rates: {ai_err}"[:800], "Failed", self.interval
            if 'periods' not in jsonData or len(jsonData['periods']) == 0:
                got_keys = list(jsonData.keys())[:10]
                return f"PPF Rates response missing/empty 'periods'. Got top-level keys: {got_keys}"[:800], "Failed", self.interval

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
