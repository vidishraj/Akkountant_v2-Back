import json
import os

from services.tasks.baseTask import BaseTask
from utils.logger import Logger


# ak-lp6 diagnostic thresholds. WARNING when the fetched scheme count
# drifts more than ±20% off the previous successful run — that's the
# level at which we want an operator to eyeball, not necessarily block.
# ERROR (with sample codes) when the count spikes above 2× — that's the
# level at which downstream SetMFRate will almost certainly get SIGKILLed
# under the current 4h interval, so we treat it as a hard alarm.
_MF_DETAILS_WARN_DEVIATION = 0.20  # ±20% off previous run
_MF_DETAILS_ERR_MULTIPLIER = 2.0   # >2× previous → error
_MF_DETAILS_DUP_SAMPLE_LIMIT = 10  # cap sample size for logs


class SetMFDetails(BaseTask):
    _instance = None

    def __new__(cls, *args, **kwargs):
        if not cls._instance:
            cls._instance = super(SetMFDetails, cls).__new__(cls)
        return cls._instance

    def __init__(self, title, priority):
        if not hasattr(self, 'initialized'):  # Prevent multiple initializations
            super().__init__(title, priority)

            self.logger = Logger(__name__).get_logger()
            # 4 hours
            self.interval = 600

    def run(self):
        try:
            # Delete existing file if it exists, else
            listUrl = "https://api.mfapi.in/mf"

            # ak-lp6: snapshot the previous-run scheme count BEFORE we
            # overwrite anything, so the diagnostic below can compare
            # current vs previous. Non-fatal on any read/parse error —
            # the diagnostic is best-effort and must not block the job.
            prev_count = self._previous_scheme_count()

            jsonData = self.make_request(listUrl)

            # ak-lp6: log counts + surface duplicate/spike classes BEFORE
            # SetMFRate consumes the file. The 2026-08-03 balloon
            # (37,713 → 113,139) hit prod silently because there was no
            # count sanity check on this side; SetMFRate then took the
            # 3× URL list and got SIGKILLed at ~41min. This diagnostic
            # is the smoking gun for future recurrences.
            self._log_scheme_counts(jsonData, prev_count)

            jsonData = {'data': jsonData}
            filePath = os.path.join(self.tmp_dir, 'MFDetails.json')
            try:
                os.remove(filePath)
            except OSError:
                pass
            self.save_json(jsonData, filePath)

            ok, err = self.safe_replace_file(filePath, self.jsonService.MfListPrefix, self.jsonService.listType)
            if not ok:
                return err, "Failed", self.interval
            return 'Completed successfully', "Completed", self.interval
        except Exception as ex:
            return ex.__str__(), "Failed", self.interval

    def _previous_scheme_count(self):
        """Read the scheme count from the most recent MFDetails file on
        disk. Returns None on any error — this is a best-effort input to
        the diagnostic; the job MUST NOT fail if the previous file is
        missing or malformed."""
        try:
            prev_path = self.jsonService.getLatestFile(
                self.jsonService.listType,
                self.jsonService.MfListPrefix,
            )
            if not prev_path or not os.path.exists(prev_path):
                return None
            with open(prev_path, 'r', encoding='utf-8') as fh:
                prev = json.load(fh)
            prev_data = prev.get('data') if isinstance(prev, dict) else None
            if isinstance(prev_data, list):
                return len(prev_data)
        except Exception as ex:
            self.logger.debug(
                f"MF details previous-count read failed (non-fatal): {ex}"
            )
        return None

    def _log_scheme_counts(self, jsonData, prev_count):
        """ak-lp6: surface breakage classes on the SetMFDetails side
        before SetMFRate ever sees the file. Three signals, in order of
        severity:

          - INFO  : always — current count + previous count for grep.
          - ERROR : duplicate schemeCodes present within the current
                    fetch (real MFAPI response can only contain each
                    code once by contract; dupes = API regression or
                    our parsing bug).
          - ERROR : >2× count spike vs previous run (SetMFRate is
                    likely to SIGKILL — treat as hard alarm).
          - WARN  : ±20% count deviation vs previous (operator
                    eyeball threshold, not blocking).
        """
        if not isinstance(jsonData, list):
            self.logger.error(
                f"MF details response is not a list — "
                f"got {type(jsonData).__name__}; skipping diagnostic"
            )
            return

        current = len(jsonData)
        self.logger.info(
            f"MF details: current={current} "
            f"previous={prev_count if prev_count is not None else 'none'}"
        )

        # Duplicate detection within the current fetch. This is the
        # smoking gun for the Aug 3 balloon — the response either had
        # dupes at the API layer OR our upstream reshaping introduced
        # them, and either way we want to surface it loudly here.
        # We track items-with-code separately from raw total so a fetch
        # that mixes real dupes with missing-code items doesn't muddle
        # the dup count.
        seen = {}
        dup_samples = []
        missing_code = 0
        for item in jsonData:
            if not isinstance(item, dict):
                continue
            sc = item.get('schemeCode')
            if sc is None:
                missing_code += 1
                continue
            if sc in seen:
                if len(dup_samples) < _MF_DETAILS_DUP_SAMPLE_LIMIT:
                    dup_samples.append(sc)
                seen[sc] += 1
            else:
                seen[sc] = 1
        unique = len(seen)
        counted = sum(seen.values())  # items that HAD a schemeCode
        dup_count = counted - unique
        if dup_count > 0:
            self.logger.error(
                f"MF details: DUPLICATE schemeCodes in fetched list — "
                f"total={current} with_code={counted} unique={unique} "
                f"duplicates={dup_count} "
                f"sample_first_{_MF_DETAILS_DUP_SAMPLE_LIMIT}={dup_samples}"
            )
        if missing_code > 0:
            # Not necessarily fatal (an odd malformed item won't break
            # SetMFRate — the URL just becomes '<base>/None' and gets
            # deduped by the ak-lp6 dedup in buildJsonForMF), but it's
            # a schema-shift smell worth surfacing.
            self.logger.warning(
                f"MF details: {missing_code} item(s) had no schemeCode "
                f"(skipped from dedup accounting)"
            )

        # Count-deviation vs previous run. Skip on first run (no baseline)
        # or on a zero baseline (avoid divide-by-zero).
        if not prev_count:
            return
        deviation_ratio = (current - prev_count) / prev_count
        if current > _MF_DETAILS_ERR_MULTIPLIER * prev_count:
            # Show the first N schemeCodes so an operator can eyeball
            # whether this is an unexpected API shape change, real
            # growth, or dupe-driven inflation.
            first_codes = [
                item.get('schemeCode')
                for item in jsonData[:_MF_DETAILS_DUP_SAMPLE_LIMIT]
                if isinstance(item, dict)
            ]
            self.logger.error(
                f"MF details: COUNT SPIKE >{_MF_DETAILS_ERR_MULTIPLIER:g}× — "
                f"current={current} previous={prev_count} "
                f"ratio={deviation_ratio:+.1%}. "
                f"sample_first_{_MF_DETAILS_DUP_SAMPLE_LIMIT}_codes={first_codes}"
            )
        elif abs(deviation_ratio) > _MF_DETAILS_WARN_DEVIATION:
            self.logger.warning(
                f"MF details: count deviation {deviation_ratio:+.1%} "
                f"vs previous (current={current}, previous={prev_count}). "
                f"Threshold ±{int(_MF_DETAILS_WARN_DEVIATION * 100)}%."
            )
