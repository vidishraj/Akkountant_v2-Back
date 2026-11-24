import os

from services.tasks.baseTask import BaseTask
import time

from services.tasks.npsSchemeDownload import (
    DEAD_SCHEME_IDS, OK, DownloadTally, download_scheme_tsv,
    full_probe_due, merge_scheme_lists, scheme_ids,
)
from utils.logger import Logger


# ak-4caa: the probe clock, injectable for tests (patching the global
# time.time also hits logging's LogRecord and other callers).
_now = time.time


class SetNPSDetails(BaseTask):
    _instance = None
    # ak-4caa: when the dead ids were last re-probed (process lifetime;
    # 0 = never, so the first run after a restart probes everything).
    _last_full_probe = 0.0

    def __new__(cls, *args, **kwargs):
        if not cls._instance:
            cls._instance = super(SetNPSDetails, cls).__new__(cls)
        return cls._instance

    def __init__(self, title, priority):
        if not hasattr(self, 'initialized'):  # Prevent multiple initializations
            super().__init__(title, priority)
            self.logger = Logger(__name__).get_logger()
            # 4 hours
            self.interval = 90

    def run(self):
        try:
            # Generate NPS scheme details from Excel downloads
            jsonData = self.fetchNPSDetailsFromExcel()
            # ak-4caa: never replace the scheme list with an empty one
            # (that completed green while NPS ingest was broken).
            if not jsonData.get("data"):
                msg = ("No NPS schemes collected — keeping the previous "
                       f"list. {jsonData.get('download_summary', '')}")
                self.logger.error(f"[ak-4caa] SetNPSDetails: {msg}")
                return msg, "Failed", self.interval

            # ak-vibs: a partial outage (some downloads ERROR'd) must not
            # shrink the published list. Carry previous entries over for
            # it, and refuse any big shrink outright.
            accept_shrink = os.environ.get("NPS_ACCEPT_SHRINK") == "1"
            schemes, carried, refuse = merge_scheme_lists(
                jsonData["data"], self._previous_schemes(),
                error_ids=jsonData.get("download_error_ids") or (),
                error_pfms=jsonData.get("download_error_pfms") or (),
                absent_ids=jsonData.get("download_absent_ids") or (),
                accept_shrink=accept_shrink)
            if refuse:
                # Review minor 2: "Completed", not "Failed". Repeated
                # Failed stops the scheduler rescheduling the job, which
                # would kill it on a genuine NPS restructure. Keep the
                # previous list, log ERROR; an operator accepts a real
                # restructure once with NPS_ACCEPT_SHRINK=1.
                msg = (f"{refuse}: kept the previous list (set NPS_ACCEPT_SHRINK=1 for one "
                       f"run to accept a genuine restructure). {jsonData.get('download_summary', '')}")
                self.logger.error(f"[ak-vibs] SetNPSDetails: {msg}")
                return msg, "Completed", self.interval
            if accept_shrink:
                self.logger.warning("[ak-vibs] SetNPSDetails: NPS_ACCEPT_SHRINK=1 set; shrink guard bypassed")
            if carried:
                self.logger.warning(
                    f"[ak-vibs] SetNPSDetails: {carried} scheme(s) kept from the previous "
                    f"list because some downloads failed this run")
            jsonData = dict(jsonData, data=schemes, total_schemes=len(schemes))

            filePath = os.path.join(self.tmp_dir, 'NPSDetails.json')
            try:
                os.remove(filePath)
            except OSError:
                pass
            self.save_json(jsonData, filePath)

            ok, err = self.safe_replace_file(filePath, self.jsonService.NpsListPrefix, self.jsonService.listType)
            if not ok:
                return err, "Failed", self.interval
            return 'Completed successfully', "Completed", self.interval
        except Exception as ex:
            return ex.__str__(), "Failed", self.interval

    def _previous_schemes(self):
        """The currently published scheme list, or [] (never raises)."""
        try:
            data = self.jsonService.getNPSList()
            return data.get("data", []) if isinstance(data, dict) else []
        except Exception as e:
            self.logger.warning(f"[ak-vibs] previous NPS list unreadable ({type(e).__name__})")
            return []

    def fetchNPSDetailsFromExcel(self):
        """
        Fetch NPS scheme details by downloading Excel files from npstrust.org.in
        Returns data in the same format as the old API
        """
        all_schemes = []
        successful_downloads = 0
        # ak-4caa: the known-dead ids are skipped except on a full probe
        # at most once per FULL_PROBE_INTERVAL_S, so a revived scheme is
        # still discovered (tally WARNs to drop it from the list).
        now = _now()
        full_probe = full_probe_due(self._last_full_probe, now)
        if full_probe:
            self._last_full_probe = now
        tally = DownloadTally(skipped_dead=0 if full_probe else len(DEAD_SCHEME_IDS))
        by_pfm = {}
        for pfm_id, scheme_id in scheme_ids(include_dead=full_probe):
            by_pfm.setdefault(pfm_id, []).append(scheme_id)

        for pfm_id, pfm_scheme_ids in by_pfm.items():
            pfm_schemes_found = []

            for scheme_id in pfm_scheme_ids:
                try:
                    excel_data, outcome, reason = download_scheme_tsv(
                        pfm_id, scheme_id, self.logger)
                    tally.add(pfm_id, scheme_id, outcome, reason)
                    if outcome == OK:
                        # Extract schemes from this file
                        schemes = self.extractSchemesFromExcel(excel_data, pfm_id)
                        
                        # Add any new schemes we haven't seen before
                        for scheme in schemes:
                            if not any(existing['id'] == scheme['id'] for existing in pfm_schemes_found):
                                pfm_schemes_found.append(scheme)
                        
                        self.logger.debug(f"Found data for {scheme_id}")

                except Exception as e:
                    tally.add(pfm_id, scheme_id, "error", type(e).__name__)
                    self.logger.warning(f"Error processing {scheme_id}: {e}")
                    continue
            
            if pfm_schemes_found:
                all_schemes.extend(pfm_schemes_found)
                successful_downloads += 1
                self.logger.info(f"Successfully processed {pfm_id} with {len(pfm_schemes_found)} schemes")
            else:
                self.logger.debug(f"No schemes found for {pfm_id}")
        
        # Remove any duplicate schemes across PFMs (based on scheme ID)
        unique_schemes = []
        seen_ids = set()
        for scheme in all_schemes:
            if scheme['id'] not in seen_ids:
                unique_schemes.append(scheme)
                seen_ids.add(scheme['id'])
            else:
                self.logger.debug(f"Removing duplicate scheme: {scheme['id']}")
        
        self.logger.info(f"Total unique schemes collected: {len(unique_schemes)} from {successful_downloads} PFMs")
        tally.log(self.logger, "SetNPSDetails")

        return {
            "data": unique_schemes,
            "total_schemes": len(unique_schemes),
            "successful_pfms": successful_downloads,
            "download_summary": tally.summary(),
            "download_errors": len(tally.error_ids),
            "download_error_ids": sorted(tally.error_ids),
            "download_error_pfms": sorted(tally.error_pfms),
            "download_absent_ids": sorted(tally.absent_ids),
        }
    
    def extractSchemesFromExcel(self, excel_data, pfm_id):
        """
        Extract unique scheme details from Excel data
        Returns list of schemes in the format expected by the system
        """
        schemes = []
        
        try:
            # Get unique schemes from the TSV data
            unique_schemes = excel_data[['SCHEME ID', 'SCHEME NAME', 'PFM ID', 'PFM NAME']].drop_duplicates()
            
            for _, row in unique_schemes.iterrows():
                scheme = {
                    "id": row['SCHEME ID'],
                    "name": row['SCHEME NAME'],
                    "pfm_id": row['PFM ID'], 
                    "pfm_name": row['PFM NAME']
                }
                schemes.append(scheme)
                
        except Exception as e:
            self.logger.error(f"Error extracting schemes from Excel data: {str(e)}")
            
        return schemes
