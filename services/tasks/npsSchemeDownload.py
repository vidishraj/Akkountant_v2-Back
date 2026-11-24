"""ak-4caa (2026-11-24): one NPS scheme-NAV download, classified.

npstrust.org.in serves a TSV (despite the vnd.ms-excel content type) for
every PFM x scheme combination that exists. The tasks probe a fixed
14 x 16 = 224 universe, and for a combination that does NOT exist the
site answers HTTP 200, text/html, with an EMPTY body (measured
2026-11-24: 149 data / 75 empty). SetNPSDetails used to pd.read_csv
every body and log pandas' "No columns to parse" at ERROR for each of
those 75 on every run, which buried any real failure.

download_scheme_tsv returns (DataFrame | None, outcome, reason):
  * "ok"     : TSV with the required columns and at least one row;
  * "absent" : 200 with an empty body / no rows (no such scheme), DEBUG;
  * "error"  : HTTP/network failure, unparsable body, or a body whose
               columns changed (format moved), logged by the caller in
               aggregate so a real outage is loud.
"""
from __future__ import annotations

from io import BytesIO

import pandas as pd
import requests

NPS_SCHEME_URL = (
    "https://npstrust.org.in/scheme-wise-nav-report-excel"
    "?navcatdataxls={pfm_id}&navyearselxls=12&navsubdataxls={scheme_id}"
)
REQUIRED_COLUMNS = ("DATE OF NAV", "PFM ID", "PFM NAME",
                    "SCHEME ID", "SCHEME NAME", "NAV VALUE")
OK, ABSENT, ERROR = "ok", "absent", "error"

# Combinations that answered 200 + EMPTY body on 2026-11-24, from both the
# fleet and the prod host (all of PFM004/006/009/012 + 11 sub-schemes).
# Routine runs skip them; SetNPSDetails re-probes them once per
# FULL_PROBE_INTERVAL and WARNs if one starts serving data again.
DEAD_SCHEME_IDS = frozenset({
    "SM001013", "SM002010", "SM002013", "SM003013", "SM004001", "SM004002",
    "SM004003", "SM004004", "SM004005", "SM004006", "SM004007", "SM004008",
    "SM004009", "SM004010", "SM004011", "SM004012", "SM004013", "SM004014",
    "SM004015", "SM004016", "SM005009", "SM006001", "SM006002", "SM006003",
    "SM006004", "SM006005", "SM006006", "SM006007", "SM006008", "SM006009",
    "SM006010", "SM006011", "SM006012", "SM006013", "SM006014", "SM006015",
    "SM006016", "SM007007", "SM007009", "SM007014", "SM008007", "SM008009",
    "SM009001", "SM009002", "SM009003", "SM009004", "SM009005", "SM009006",
    "SM009007", "SM009008", "SM009009", "SM009010", "SM009011", "SM009012",
    "SM009013", "SM009014", "SM009015", "SM009016", "SM010008", "SM012001",
    "SM012002", "SM012003", "SM012004", "SM012005", "SM012006", "SM012007",
    "SM012008", "SM012009", "SM012010", "SM012011", "SM012012", "SM012013",
    "SM012014", "SM012015", "SM012016",
})
FULL_PROBE_INTERVAL_S = 24 * 3600


def scheme_ids(include_dead=False):
    """The probed universe: (pfm_id, scheme_id) for 14 PFMs x 16 schemes,
    minus DEAD_SCHEME_IDS unless include_dead."""
    for pfm_num in range(1, 15):
        pfm_id = f"PFM0{pfm_num:02d}"
        for scheme_num in range(1, 17):
            scheme_id = f"SM0{pfm_num:02d}{scheme_num:03d}"
            if include_dead or scheme_id not in DEAD_SCHEME_IDS:
                yield pfm_id, scheme_id


def download_scheme_tsv(pfm_id, scheme_id, logger, get=requests.get):
    url = NPS_SCHEME_URL.format(pfm_id=pfm_id, scheme_id=scheme_id)
    try:
        response = get(url, timeout=30, verify=False)
        response.raise_for_status()
    except Exception as exc:
        logger.debug(f"NPS {pfm_id}/{scheme_id}: request failed: {exc}")
        return None, ERROR, f"request failed: {type(exc).__name__}"

    body = response.content or b""
    if not body.strip():
        logger.debug(f"NPS {pfm_id}/{scheme_id}: empty response (no such scheme)")
        return None, ABSENT, None
    ctype = response.headers.get("content-type", "")
    if "html" in ctype.lower():
        # A NON-empty HTML page (maintenance / error page) is not a TSV:
        # don't hand it to pandas; count it as a real error.
        return None, ERROR, f"html page instead of TSV ({len(body)} bytes)"
    try:
        df = pd.read_csv(BytesIO(body), sep="\t")
    except pd.errors.EmptyDataError:
        return None, ABSENT, None
    except Exception as exc:
        ctype = response.headers.get("content-type", "")
        return None, ERROR, f"unparsable body ({type(exc).__name__}, {ctype}, {len(body)} bytes)"
    missing = [c for c in REQUIRED_COLUMNS if c not in df.columns]
    if missing:
        ctype = response.headers.get("content-type", "")
        return None, ERROR, (f"format changed: missing columns {missing} "
                             f"({ctype}, {len(body)} bytes)")
    if df.empty:
        return None, ABSENT, None
    return df, OK, None


class DownloadTally:
    """Per-run outcome counts plus the first few error reasons."""

    def __init__(self, skipped_dead=0):
        self.counts = {OK: 0, ABSENT: 0, ERROR: 0}
        self.errors = []
        self.revived = []
        self.skipped_dead = skipped_dead

    def add(self, pfm_id, scheme_id, outcome, reason):
        self.counts[outcome] += 1
        if outcome == ERROR and len(self.errors) < 5:
            self.errors.append(f"{pfm_id}/{scheme_id}: {reason}")
        if outcome == OK and scheme_id in DEAD_SCHEME_IDS:
            self.revived.append(scheme_id)

    def summary(self):
        c = self.counts
        s = f"ok={c[OK]} absent={c[ABSENT]} error={c[ERROR]} skipped_dead={self.skipped_dead}"
        if self.errors:
            s += f" (first errors: {'; '.join(self.errors)})"
        return s

    def log(self, logger, title):
        """INFO when clean; WARNING when some downloads errored; ERROR
        when nothing usable came back (the run must not look green)."""
        c = self.counts
        if self.revived:
            logger.warning(f"[ak-4caa] {title}: scheme(s) listed as dead now "
                           f"serve data: {sorted(self.revived)} — remove them "
                           f"from DEAD_SCHEME_IDS so every run fetches them")
        if c[OK] == 0:
            logger.error(f"[ak-4caa] {title}: NO usable NPS scheme files — "
                         f"source down or format/URL changed. {self.summary()}")
        elif c[ERROR]:
            logger.warning(f"[ak-4caa] {title}: some NPS downloads failed. {self.summary()}")
        else:
            logger.info(f"[ak-4caa] {title}: NPS downloads {self.summary()}")
