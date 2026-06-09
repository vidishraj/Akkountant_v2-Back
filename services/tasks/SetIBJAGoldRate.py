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

Top-level "carat_24", "carat_22", "carat_18" are the avg_with_gst values for
gold 999, 916, and 750 respectively.

If today's rates are not available yet (e.g. before market open), use the
most recent available day's rates and use that day's date in ibja_data.date.

Return ALL prices as integers in INR. Date format: "DD-Mon-YY" (e.g. "16-Feb-26").

IMPORTANT: Do not use any tools to search or filter the page content. The
entire relevant HTML is already provided in the user message below. Read it
directly and emit the structured output."""


# JSON schema for output_format. Strict enough to validate that prices came
# through as integers and the date is present, while letting the model fill in
# zero-or-missing purities gracefully.
#
# v4 (hq-wisp-il849): top-level keys MUST satisfy the Anthropic API regex
# `^[a-zA-Z0-9_.-]{1,64}$` — display keys "24 Carat" / "22 Carat" / "18 Carat"
# contain spaces and 400 the request as
# `tools.N.custom.input_schema.properties: invalid property key`.
# Schema uses compliant snake_case keys (`carat_24` etc.); they are re-mapped
# to display form before persisting (frontend reads GoldRate.json with the
# original "24 Carat" keys — that on-disk contract is unchanged).
GOLD_SCHEMA = {
    "type": "object",
    "properties": {
        "carat_24": {"type": "integer"},
        "carat_22": {"type": "integer"},
        "carat_18": {"type": "integer"},
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
    "required": ["carat_24", "carat_22", "carat_18", "ibja_data"],
}


# LLM-schema key → on-disk display key. Pairs in the order the persisted JSON
# should list them so the frontend renders 24 → 22 → 18 in stable order.
_GOLD_DISPLAY_KEYS = [
    ("carat_24", "24 Carat"),
    ("carat_22", "22 Carat"),
    ("carat_18", "18 Carat"),
]


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
            # v4 (hq-wisp-il849): reverted from the pinned-sonnet diag probe
            # back to the default bare "sonnet" alias. The pinned probe was a
            # diagnostic to isolate alias-flip from a deeper sonnet issue;
            # actual root cause turned out to be the invalid property keys
            # in GOLD_SCHEMA (see schema comment above), not the model
            # string. Bare alias = no per-call override = `fetch_and_extract`
            # default ("sonnet").
            jsonData, ai_err = self.fetch_rates_via_ai(
                url=GOLD_URL,
                system_prompt=GOLD_SYSTEM_PROMPT,
                schema=GOLD_SCHEMA,
                agent="rate.gold",
            )
            if jsonData is None:
                return f"Failed to get Gold Rates: {ai_err}"[:800], "Failed", self.interval
            if 'carat_24' not in jsonData:
                got_keys = list(jsonData.keys())[:10]
                return f"Gold Rates response missing 'carat_24'. Got top-level keys: {got_keys}"[:800], "Failed", self.interval

            # Re-map LLM-schema-compliant keys → on-disk display keys. The
            # frontend reads GoldRate.json with the historical "24 Carat" /
            # "22 Carat" / "18 Carat" top-level keys (see
            # JsonDownloadService.getGoldRate, GoldService consumers); that
            # on-disk contract is unchanged by v4.
            for k_schema, k_display in _GOLD_DISPLAY_KEYS:
                if k_schema in jsonData:
                    jsonData[k_display] = jsonData.pop(k_schema)

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
