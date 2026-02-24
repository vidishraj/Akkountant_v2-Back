import csv
import io
import os
import requests

from services.tasks.baseTask import BaseTask
from utils.logger import Logger

NSE_SYMBOL_CHANGE_URL = "https://nsearchives.nseindia.com/content/equities/symbolchange.csv"


class SetStockOldCodes(BaseTask):
    _instance = None

    def __new__(cls, *args, **kwargs):
        if not cls._instance:
            cls._instance = super(SetStockOldCodes, cls).__new__(cls)
        return cls._instance

    def __init__(self, title, priority):
        if not hasattr(self, 'initialized'):
            super().__init__(title, priority)
            self.logger = Logger(__name__).get_logger()
            # Refresh every 20 days (symbols rarely change)
            self.interval = 28800

    def run(self):
        try:
            resp = requests.get(
                NSE_SYMBOL_CHANGE_URL,
                headers={"User-Agent": "Mozilla/5.0"},
                timeout=30,
            )
            resp.raise_for_status()

            reader = csv.reader(io.StringIO(resp.text))
            direct_map = {}
            for row in reader:
                if len(row) >= 4:
                    old_sym = row[1].strip()
                    new_sym = row[2].strip()
                    if old_sym and new_sym:
                        direct_map[old_sym] = new_sym

            # Resolve chains: A→B→C becomes A→C
            def resolve(sym, visited=None):
                if visited is None:
                    visited = set()
                if sym in visited:
                    return sym
                visited.add(sym)
                if sym in direct_map:
                    return resolve(direct_map[sym], visited)
                return sym

            final_map = {}
            for old_sym in direct_map:
                resolved = resolve(old_sym)
                if resolved != old_sym:
                    final_map[old_sym] = resolved

            # Save to tmp then move to assets
            tmp_path = os.path.join(self.tmp_dir, "Stock_old_codes.json")
            try:
                os.remove(tmp_path)
            except OSError:
                pass
            self.save_json(final_map, tmp_path)

            latest_file = self.jsonService.getLatestFile(
                self.jsonService.listType, self.jsonService.StockOldDetails
            )
            new_path = self.jsonService.getFilePath(
                self.jsonService.StockOldDetails, self.jsonService.listType
            )

            if self.move_file(tmp_path, new_path):
                self.jsonService.deleteFile(latest_file)
            else:
                return "Failed to move file", "Failed", self.interval

            self.logger.info(f"Generated {len(final_map)} old→new symbol mappings from NSE")
            return "Completed successfully", "Completed", self.interval

        except Exception as ex:
            return str(ex), "Failed", self.interval
