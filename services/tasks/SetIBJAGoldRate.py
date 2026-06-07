import os

from services.tasks.AIRateTask import AIRateTask
from utils.logger import Logger


# Upstream URL for IBJA daily gold/silver rates.
# IBJA publishes the rate table at this page; sonnet extracts the AM/PM
# prices per purity from the HTML.
GOLD_URL = "https://ibjarates.com/"

GOLD_SYSTEM_PROMPT = """You are a data extraction assistant. You will receive
the HTML of the IBJA (India Bullion and Jewellers Association) daily rate page.
Extract today's gold (purities 999, 995, 916, 750, 585) and silver (999) AM
and PM prices.

Gold prices on the page are per 10 grams. Silver is per 1 kg.

For each purity, compute:
- avg_price = round((am_price + pm_price) / 2)
- avg_with_gst = int(avg_price * 1.03)  # 3% GST, truncated

Top-level "24 Carat", "22 Carat", "18 Carat" are the avg_with_gst values for
gold 999, 916, and 750 respectively.

If today's rates are not available yet (e.g. before market open), use the
most recent available day's rates and use that day's date in ibja_data.date.

Return ALL prices as integers in INR. Date format: "DD-Mon-YY" (e.g. "16-Feb-26")."""


# JSON schema for output_format. Strict enough to validate that prices came
# through as integers and the date is present, while letting the model fill in
# zero-or-missing purities gracefully.
GOLD_SCHEMA = {
    "type": "object",
    "properties": {
        "24 Carat": {"type": "integer"},
        "22 Carat": {"type": "integer"},
        "18 Carat": {"type": "integer"},
        "ibja_data": {
            "type": "object",
            "properties": {
                "date": {"type": "string"},
                "gold": {
                    "type": "object",
                    "properties": {
                        "999": {"type": "object"},
                        "995": {"type": "object"},
                        "916": {"type": "object"},
                        "750": {"type": "object"},
                        "585": {"type": "object"},
                    },
                },
                "silver": {
                    "type": "object",
                    "properties": {"999": {"type": "object"}},
                },
                "currency": {"type": "string"},
                "source": {"type": "string"},
            },
            "required": ["date", "gold", "currency", "source"],
        },
    },
    "required": ["24 Carat", "22 Carat", "18 Carat", "ibja_data"],
}


class SetIBJAGoldRate(AIRateTask):
    _instance = None

    def __new__(cls, *args, **kwargs):
        if not cls._instance:
            cls._instance = super(SetIBJAGoldRate, cls).__new__(cls)
        return cls._instance

    def __init__(self, title, priority):
        if not hasattr(self, 'initialized'):
            super().__init__(title, priority)
            self.logger = Logger(__name__).get_logger()
            # 5 hours
            self.interval = 300
            self.initialized = True

    def run(self):
        try:
            # Haiku probe (hq-wisp-ezveu): distinguishes sonnet-alias-flip from
            # account-wide failure. rate.ppf intentionally stays on sonnet as
            # the control. Revert once verdict is in.
            jsonData, ai_err = self.fetch_rates_via_ai(
                url=GOLD_URL,
                system_prompt=GOLD_SYSTEM_PROMPT,
                schema=GOLD_SCHEMA,
                agent="rate.gold",
                model="haiku",
            )
            if jsonData is None:
                return f"Failed to get Gold Rates: {ai_err}"[:800], "Failed", self.interval
            if '24 Carat' not in jsonData:
                got_keys = list(jsonData.keys())[:10]
                return f"Gold Rates response missing '24 Carat'. Got top-level keys: {got_keys}"[:800], "Failed", self.interval

            filePath = os.path.join(self.tmp_dir, 'GOLDRATE.json')
            try:
                os.remove(filePath)
            except OSError:
                pass
            self.save_json(jsonData, filePath)

            ok, err = self.safe_replace_file(filePath, self.jsonService.GoldRatePrefix, self.jsonService.ratesType)
            if not ok:
                return err, "Failed", self.interval
            return 'Completed successfully', "Completed", self.interval
        except Exception as ex:
            return ex.__str__(), "Failed", self.interval
