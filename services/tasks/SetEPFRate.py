import os

from services.tasks.AIRateTask import AIRateTask
from utils.logger import Logger


# EPF historical interest rates. The Wikipedia page has a stable table going
# back to FY 1952-53 with the rate per financial year — the most stable
# upstream available. EPFO's own site occasionally moves the page; Wikipedia
# is updated within days of the EPFO declaration.
EPF_URL = "https://en.wikipedia.org/wiki/Employees%27_Provident_Fund_Organisation"

EPF_SYSTEM_PROMPT = """You are a data extraction assistant. You will receive the
HTML of a Wikipedia page that contains a table of historical EPF (Employee
Provident Fund) interest rates declared by the EPFO for each financial year.

Extract every financial year from 2014-15 onwards (inclusive) through the
most recent year on the page. Each financial year runs April YYYY through
March YYYY+1 — expand each financial-year rate into monthly entries for
April through March (inclusive) of the next year.

Output format:
- "Year" is in "YYYY-MM" format
- "interest_rate" is a float (e.g. 8.25, not "8.25%")
- Each financial year's rate applies to all 12 months from April to next March
- Include entries from April 2014 through the most recent month covered by
  the latest declared rate
- If the page only declares the rate for FY YYYY-YY+1 (not partial year),
  fill all 12 months with that rate"""


# v4 (hq-wisp-il849): the per-entry key "Interest Rate" contains a space
# and violates the Anthropic API regex `^[a-zA-Z0-9_.-]{1,64}$`, which 400s
# the request as `tools.N.custom.input_schema.properties: invalid property
# key`. Schema uses compliant snake_case (`interest_rate`); entries are
# re-mapped back to "Interest Rate" before persisting (EPFRate.json on-disk
# contract is unchanged — frontend reads with the original key).
EPF_SCHEMA = {
    "type": "object",
    "properties": {
        "data": {
            "type": "array",
            "items": {
                "type": "object",
                "properties": {
                    "Year": {"type": "string"},
                    "interest_rate": {"type": "number"},
                },
                "required": ["Year", "interest_rate"],
            },
        }
    },
    "required": ["data"],
}


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
            jsonData, ai_err = self.fetch_rates_via_ai(
                url=EPF_URL,
                system_prompt=EPF_SYSTEM_PROMPT,
                schema=EPF_SCHEMA,
                agent="rate.epf",
            )
            if jsonData is None:
                return f"Failed to get EPF Rates: {ai_err}"[:800], "Failed", self.interval
            if 'data' not in jsonData or len(jsonData['data']) == 0:
                got_keys = list(jsonData.keys())[:10]
                return f"EPF Rates response missing/empty 'data'. Got top-level keys: {got_keys}"[:800], "Failed", self.interval

            # Re-map LLM-schema key "interest_rate" → on-disk display key
            # "Interest Rate". The on-disk EPFRate.json contract is
            # unchanged; only the API-boundary schema uses the compliant
            # snake_case form to satisfy the Anthropic property-key regex.
            for entry in jsonData['data']:
                if 'interest_rate' in entry:
                    entry['Interest Rate'] = entry.pop('interest_rate')

            filePath = os.path.join(self.tmp_dir, 'EPFRate.json')
            try:
                os.remove(filePath)
            except OSError:
                pass
            self.save_json(jsonData, filePath)

            ok, err = self.safe_replace_file(filePath, self.jsonService.EPFRatePrefix, self.jsonService.ratesType)
            if not ok:
                return err, "Failed", self.interval
            return 'Completed successfully', "Completed", self.interval
        except Exception as ex:
            return ex.__str__(), "Failed", self.interval
