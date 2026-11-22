from decimal import Decimal
from io import BytesIO
from datetime import datetime, timedelta

import pandas as pd
import requests

from services.tasks.BaseRateTask import BaseRateTask
from utils.logger import Logger


# ak-2r8: NPS enumerated universe = 14 PFMs × 16 schemes = 224 candidate
# combinations. Historically many combinations don't exist as real schemes
# (the fetch returns None for those) so `successful_downloads` typically
# lands well below 224 even on a healthy run. That means the MF-defaults
# (98% / 50%) would trip on every healthy NPS run and preserve last-good
# for no reason.
#
# Q2 GO'd option (a): apply the SAME gate machinery with LOW thresholds
# (0.05 / 0.05). This catches the specific bug ak-2r8 targets — an
# empty-response run that would clobber last-good with an all-null file
# — while allowing normal partial coverage through. The count-deviation
# baseline (which knows what "normal coverage" is for the current PFM×
# scheme universe) is Phase 2 work, tracked in follow-up bead
# ak-2r8-fb-nps-count-deviation.
_NPS_MIN_SUCCESS_RATIO = 0.05
_NPS_COVERAGE_HARD_FLOOR = 0.05


class SetNPSRate(BaseRateTask):
    _instance = None

    # ak-2r8: BaseRateTask contract.
    _rate_filename = 'NPSRATE.json'
    _rate_prefix_attr = 'NpsRatePrefix'

    # ak-2r8 Q2 option (a): low thresholds pending Phase 2 count-deviation.
    _MIN_SUCCESS_RATIO = _NPS_MIN_SUCCESS_RATIO
    _COVERAGE_HARD_FLOOR = _NPS_COVERAGE_HARD_FLOOR

    def __new__(cls, *args, **kwargs):
        if not cls._instance:
            cls._instance = super(SetNPSRate, cls).__new__(cls)
        return cls._instance

    def __init__(self, title, priority):
        if not hasattr(self, 'initialized'):  # Prevent multiple initializations
            super().__init__(title, priority)
            self.logger = Logger(__name__).get_logger()
            # 4 hours
            self.interval = 60

    def _fetch_all(self):
        """ak-2r8: BaseRateTask contract.

        NPS enumerated universe = 14 PFMs × 16 schemes = 224 candidate
        combinations. `fetchNPSRatesFromExcel` iterates all 224 and
        returns the ones that produced parseable NAV rows.

        Returns (data, 224, len(all_rates), {}, {}).

        `permanent_skips` is intentionally empty (`{}`) — NPS currently
        has no way to distinguish "PFM×scheme combination doesn't
        exist" from "transient upstream error". Every non-response
        counts against the ratio. The low thresholds (5% each) mean
        this is fine in practice: an empty run (0/224 = 0%) trips
        both gates → preserve last-good.
        """
        jsonData = self.fetchNPSRatesFromExcel()
        # Universe is fixed at 224; ok = actual downloaded schemes.
        total = 14 * 16
        ok = int(jsonData.get('total_schemes', 0) or 0)
        return jsonData, total, ok, {}, {}

    def fetchNPSRatesFromExcel(self):
        """
        Fetch NPS rates by downloading Excel files from npstrust.org.in
        Returns data in the same format as the old API with historical data.

        ak-v8z1 (ak-ofir Phase 2A): also persist the 12-month history
        the upstream URL already returns into ``npsHistoricalNavs``.
        Pre-ak-v8z1, ``extractRatesFromExcel`` kept only the latest
        row + 3 discrete points and discarded the rest — this
        extension captures all rows so the Phase 2B synthesis script
        can reconstruct past-date NPS values. Persistence is
        best-effort: a DB failure during history-upsert does NOT
        block the current-NAV fetch (the primary responsibility of
        this task).
        """
        all_rates = []
        total_hist_upserted = 0
        hist_errors = 0

        # Iterate through all PFM IDs (PFM001 to PFM014)
        for pfm_num in range(1, 15):
            pfm_id = f"PFM0{pfm_num:02d}"

            # Try different schemes for this PFM (001 to 016)
            for scheme_num in range(1, 17):
                scheme_id = f"SM0{pfm_num:02d}{scheme_num:03d}"

                try:
                    excel_data = self.downloadNPSExcel(pfm_id, scheme_id)
                    if excel_data is not None and not excel_data.empty:
                        # Extract rates from this scheme
                        scheme_rates = self.extractRatesFromExcel(excel_data, scheme_id)
                        if scheme_rates:
                            all_rates.append(scheme_rates)
                            self.logger.info(f"Successfully processed rates for {scheme_id}")

                        # ak-v8z1: persist the 12-month history every
                        # row — best-effort, isolated per-scheme so a
                        # DB blip on one scheme doesn't poison the
                        # current-NAV write for the others.
                        try:
                            upserted = self._persist_historical_rows(
                                excel_data, scheme_id,
                            )
                            total_hist_upserted += upserted
                        except Exception as hist_exc:
                            hist_errors += 1
                            self.logger.warning(
                                f"ak-v8z1 history persist failed for "
                                f"{scheme_id}: {hist_exc}"
                            )

                except Exception as e:
                    self.logger.error(f"Error processing rates for {scheme_id}: {str(e)}")
                    continue

        successful_downloads = len(all_rates)
        self.logger.info(f"Total scheme rates collected: {successful_downloads}")
        if total_hist_upserted or hist_errors:
            self.logger.info(
                f"ak-v8z1 NPS history: upserted={total_hist_upserted} "
                f"errors={hist_errors}"
            )
            # Flush historical UPSERTs. Isolated from the rate-file
            # write pipeline (which doesn't touch the DB).
            try:
                self.db.session.commit()
            except Exception as commit_exc:
                self.db.session.rollback()
                self.logger.warning(
                    f"ak-v8z1 NPS history commit failed: {commit_exc}"
                )

        return {
            "data": all_rates,
            "total_schemes": successful_downloads
        }

    def _persist_historical_rows(self, excel_data, scheme_id):
        """ak-v8z1: UPSERT every (date, NAV) row for the scheme into
        ``npsHistoricalNavs``. Returns rows attempted (= len(df)
        when all accept).

        Idempotent via the (scheme_id, nav_date) UK. Re-running
        over the same 12-month window produces the same rows and
        the UPSERT is a near-noop.

        Isolated from the on-disk rate-file write — a DB failure
        here does NOT clobber MFRate/NPSRATE.json; the caller logs
        + swallows the exception.
        """
        from sqlalchemy.dialects.mysql import insert as mysql_insert
        from models.npsHistoricalNav import NPSHistoricalNav

        # Column presence already asserted by extractRatesFromExcel;
        # defensive re-check so this helper is safe-to-call
        # independently of that upstream guard.
        if not all(
            col in excel_data.columns
            for col in ("DATE OF NAV", "NAV VALUE")
        ):
            return 0

        rows = []
        for _, excel_row in excel_data.iterrows():
            try:
                raw_date = excel_row["DATE OF NAV"]
                raw_nav = excel_row["NAV VALUE"]
                if pd.isna(raw_date) or pd.isna(raw_nav):
                    continue
                # pandas to_datetime was already called in
                # extractRatesFromExcel → DATE OF NAV is Timestamp.
                # Be defensive in case this helper is called before
                # that conversion lands on the DF.
                if hasattr(raw_date, "date"):
                    nav_date = raw_date.date()
                else:
                    nav_date = pd.to_datetime(raw_date).date()
                nav = Decimal(str(raw_nav))
                if nav == 0:
                    continue
            except (ValueError, TypeError, ArithmeticError):
                continue
            rows.append(
                {
                    "scheme_id": scheme_id,
                    "nav_date": nav_date,
                    "nav": nav,
                }
            )

        if not rows:
            return 0

        stmt = mysql_insert(NPSHistoricalNav).values(rows)
        upsert = stmt.on_duplicate_key_update(nav=stmt.inserted.nav)
        self.db.session.execute(upsert)
        return len(rows)
    
    def downloadNPSExcel(self, pfm_id, scheme_id):
        """
        Download Excel file for a specific PFM and scheme
        """
        url = f"https://npstrust.org.in/scheme-wise-nav-report-excel?navcatdataxls={pfm_id}&navyearselxls=12&navsubdataxls={scheme_id}"
        
        try:
            response = requests.get(url, timeout=30, verify=False)
            response.raise_for_status()
            
            # Check if response contains Excel data
            if 'application/vnd' in response.headers.get('content-type', '') or len(response.content) > 1000:
                # The API returns TSV data despite the Excel content-type header
                tsv_data = pd.read_csv(BytesIO(response.content), sep='\t')
                return tsv_data
            else:
                # Likely an error page or no data
                return None
                
        except Exception as e:
            self.logger.debug(f"No data available for {pfm_id}/{scheme_id}: {str(e)}")
            return None
    
    def extractRatesFromExcel(self, excel_data, scheme_id):
        """
        Extract NAV rates with historical data from Excel
        Returns scheme rate data in the format expected by the system
        """
        try:
            # Ensure we have the required columns
            if not all(col in excel_data.columns for col in ['DATE OF NAV', 'NAV VALUE', 'SCHEME ID', 'SCHEME NAME']):
                self.logger.error(f"Missing required columns in TSV data for {scheme_id}")
                return None
            
            # Sort by date to get chronological order (newest first)
            excel_data['DATE OF NAV'] = pd.to_datetime(excel_data['DATE OF NAV'])
            excel_data = excel_data.sort_values('DATE OF NAV', ascending=False)
            
            if excel_data.empty:
                return None
                
            # Get the latest row
            latest_row = excel_data.iloc[0]
            
            # Build the rate object similar to old API format
            rate_data = {
                "scheme_id": scheme_id,
                "scheme_name": latest_row['SCHEME NAME'],
                "nav": float(latest_row['NAV VALUE']),
                "date": latest_row['DATE OF NAV'].strftime('%Y-%m-%d'),
                "pfm_id": latest_row.get('PFM ID', ''),
                "pfm_name": latest_row.get('PFM NAME', '')
            }
            
            # Add historical data if available
            try:
                # Yesterday's NAV (previous business day)
                if len(excel_data) > 1:
                    rate_data['yesterday'] = float(excel_data.iloc[1]['NAV VALUE'])
                
                # Last week (7 days ago, or closest available)
                week_ago_data = excel_data[excel_data['DATE OF NAV'] <= (latest_row['DATE OF NAV'] - timedelta(days=7))]
                if not week_ago_data.empty:
                    rate_data['lastWeek'] = float(week_ago_data.iloc[0]['NAV VALUE'])
                
                # Six months ago (180 days ago, or closest available)
                six_months_ago_data = excel_data[excel_data['DATE OF NAV'] <= (latest_row['DATE OF NAV'] - timedelta(days=180))]
                if not six_months_ago_data.empty:
                    rate_data['sixMonthsAgo'] = float(six_months_ago_data.iloc[0]['NAV VALUE'])
                    
            except Exception as e:
                self.logger.warning(f"Error adding historical data for {scheme_id}: {str(e)}")
            
            return rate_data
                
        except Exception as e:
            self.logger.error(f"Error extracting rates from Excel data for {scheme_id}: {str(e)}")
            return None
