import os

from services.tasks.AIRateTask import AIRateTask
from utils.logger import Logger


# EPF historical interest rates.
#
# v6 (hq-wisp-3andt): pivoted off the Wikipedia EPFO page because that page
# no longer carries the historical FY-rate table — infra's audit confirmed
# only 5 tables remain on the page and none of them are the rate history
# (just citation banners + a single prose mention of "March 2022, EPFO
# lowered the interest rate of 8.10%..."). The source went dry. Sonnet
# correctly emitted `{"data": []}` against an empty source post-v5, which
# is the correct behavior but produces zero-coverage rate files.
#
# Cleartax was chosen as primary after infra's source-audit:
#   - cleartax: 19 FY rows (2009-2025 cleanly), 68 rate matches  ← PRIMARY
#   - groww:    6 FY rows, 39 rate matches                       (fallback)
#   - epfindia.gov.in: canonical but WAF-blocks bots             (unreachable)
#   - bankbazaar: 404                                            (dead)
#
# Cleartax is a tax-blog page (not a wiki), so the system prompt below is
# tuned for blog-table structure rather than the wiki-infobox format the
# v5 prompt anchored on.
EPF_URL = "https://cleartax.in/s/epf-interest-rate"

EPF_SYSTEM_PROMPT = """You are a data extraction assistant. You will receive the
HTML of a tax-blog article (cleartax.in) that contains one or more tables of
historical EPF (Employee Provident Fund) interest rates declared by the EPFO
for each financial year.

The page is a blog article — the rate table is embedded inside the article
body alongside prose, headings, related-article links, ads, and navigation
chrome. Locate the table whose rows are financial years and rates (rather
than e.g. a comparison-of-schemes table). The rate-history table typically
has columns like "Financial Year" (e.g. "2024-25") and "EPF Interest Rate"
(e.g. "8.25%") and runs back to roughly 2009-10. Ignore any unrelated tables
on the page (deposit limits, scheme comparisons, withdrawal slabs, etc.).

Extract every financial year from 2014-15 onwards (inclusive) through the
most recent year on the page. Each financial year runs April YYYY through
March YYYY+1 — expand each financial-year rate into monthly entries for
April through March (inclusive) of the next year.

Output format:
- "Year" is in "YYYY-MM" format
- "interest_rate" is a float (e.g. 8.25, not "8.25%" — strip the percent sign)
- Each financial year's rate applies to all 12 months from April to next March
- Include entries from April 2014 through the most recent month covered by
  the latest declared rate
- If the page only declares the rate for FY YYYY-YY+1 (not partial year),
  fill all 12 months with that rate

IMPORTANT: Do not use any tools to search or filter the page content. The
entire relevant HTML is already provided in the user message below. Read it
directly and emit the structured output."""


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
