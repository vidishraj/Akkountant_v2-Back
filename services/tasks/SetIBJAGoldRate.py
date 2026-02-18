import os

from services.tasks.AIRateTask import AIRateTask
from utils.logger import Logger


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
            prompt = """Search the web for today's IBJA (India Bullion and Jewellers Association) gold and silver rates.
Look for the latest daily opening and closing rates from ibjarates.com or other reliable Indian bullion rate sources.

I need rates for these gold purities (per 10 grams) and silver (per 1 kg):
- Gold 999 (24 Carat) - AM and PM prices
- Gold 995 - AM and PM prices
- Gold 916 (22 Carat) - AM and PM prices
- Gold 750 (18 Carat) - AM and PM prices
- Gold 585 - AM and PM prices
- Silver 999 - AM and PM prices per kg

Return a JSON object with this EXACT structure (all prices in INR as integers):

{
  "24 Carat": <gold_999_avg_with_3pct_gst>,
  "22 Carat": <gold_916_avg_with_3pct_gst>,
  "18 Carat": <gold_750_avg_with_3pct_gst>,
  "ibja_data": {
    "date": "<DD-Mon-YY>",
    "gold": {
      "999": {
        "am_price_10g": <int>,
        "pm_price_10g": <int>,
        "avg_price_10g": <int>,
        "avg_with_gst": <int>
      },
      "995": {
        "am_price_10g": <int>,
        "pm_price_10g": <int>,
        "avg_price_10g": <int>,
        "avg_with_gst": <int>
      },
      "916": {
        "am_price_10g": <int>,
        "pm_price_10g": <int>,
        "avg_price_10g": <int>,
        "avg_with_gst": <int>
      },
      "750": {
        "am_price_10g": <int>,
        "pm_price_10g": <int>,
        "avg_price_10g": <int>,
        "avg_with_gst": <int>
      },
      "585": {
        "am_price_10g": <int>,
        "pm_price_10g": <int>,
        "avg_price_10g": <int>,
        "avg_with_gst": <int>
      }
    },
    "silver": {
      "999": {
        "am_price_1kg": <int>,
        "pm_price_1kg": <int>,
        "avg_price_1kg": <int>,
        "avg_with_gst": <int>
      }
    },
    "currency": "INR",
    "source": "IBJA"
  }
}

Calculation rules:
- avg_price = (am_price + pm_price) / 2 (integer division)
- avg_with_gst = avg_price * 1.03 (truncated to integer)
- "24 Carat" = gold 999 avg_with_gst
- "22 Carat" = gold 916 avg_with_gst
- "18 Carat" = gold 750 avg_with_gst
- Date format: "DD-Mon-YY" (e.g. "16-Feb-26")
- If today's rates aren't available yet (e.g. before market open), use the most recent available day's rates"""

            jsonData = self.fetch_rates_via_ai(prompt)
            if not jsonData or '24 Carat' not in jsonData:
                return "Failed to get Gold Rates via AI", "Failed", self.interval

            filePath = os.path.join(self.tmp_dir, 'GOLDRATE.json')
            try:
                os.remove(filePath)
            except OSError:
                pass
            self.save_json(jsonData, filePath)

            latestFile = self.jsonService.getLatestFile(self.jsonService.ratesType, self.jsonService.GoldRatePrefix)
            latestFilePath = self.jsonService.getFilePath(self.jsonService.GoldRatePrefix,
                                                          self.jsonService.ratesType)
            fileMoved = self.move_file(filePath, latestFilePath)

            if fileMoved:
                self.jsonService.deleteFile(latestFile)
            else:
                return 'Failed to move file', "Failed", self.interval
            return 'Completed successfully', "Completed", self.interval
        except Exception as ex:
            return ex.__str__(), "Failed", self.interval
