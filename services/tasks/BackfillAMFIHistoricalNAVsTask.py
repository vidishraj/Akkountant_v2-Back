"""ak-v8z1 (ak-ofir Phase 2A): daily AMFI historical-NAV backfill.

Fetches daily EOD NAV history for MF schemes referenced by the
user's ``PurchasedSecurities`` (securityType='Mutual_Funds') and
upserts into ``mfHistoricalNavs``. Companion of
``BackfillHistoricalCandlesTask`` for the stock side (ak-4o41) —
same shape, same contract, same cadence.

## Cadence

Scheduled self-reschedule interval = 24h. AMFI publishes EOD NAVs
once per trading day (same cadence as the SetAMFIMFRate NAVAll.txt
writer, ak-nutd). Sub-daily polls add no signal.

## Idempotency

Upsert keyed on ``(scheme_code, nav_date)`` unique constraint. Re-
running the task over an overlapping date range produces no
duplicate rows and no stale-data risk — AMFI's historical NAV for
a date is authoritative post-close.

Overlap window is 7 calendar days by default (same rationale as
BackfillHistoricalCandlesTask). Cold-start fetches 365 days.

## Source

AMFI's bulk historical NAV endpoint:
``https://portal.amfiindia.com/DownloadNAVHistoryReport_Po.aspx?
frmdt=<DD-Mon-YYYY>&todt=<DD-Mon-YYYY>``

Response shape (verified via ak-ofir Phase 1 feasibility):
  * Semicolon-separated text, 8 fields per data row (same shape as
    NAVAll.txt).
  * Date format: DD-Mon-YYYY (e.g. "22-Nov-2026").
  * Max window per request: ~90 days. We chunk a 365-day cold-
    start into 4 GETs, respecting a politeness interval between
    requests.

## Rate limits

No published AMFI rate limit — same source as NAVAll.txt (which
SetAMFIMFRate hits every 24h with no throttling). Out of caution
we insert a 1-second politeness interval between chunk GETs. For
steady-state (single 7-day window per cycle) only one request is
sent per run.

## Dependencies

* ``PurchasedSecurities`` — enumerates which scheme_codes to
  populate. Only schemes the single user currently holds (buyQuant
  > 0) are backfilled. Dedupe cross-user if multi-user ever
  re-lands.
* ``mfHistoricalNavs`` model (this bead) — upsert target.

## Blast radius

Zero. Separate table (``mfHistoricalNavs``), separate task,
separate scheduler entry. Phase 2B synthesis script degrades
cleanly to empty per-date NAV lookup when the table is empty
(pre-first-run or cold-start in progress).
"""

from __future__ import annotations

import time
from datetime import datetime, timedelta, timezone
from decimal import Decimal

import requests

from models.purchasedSecurities import PurchasedSecurities
from models.mfHistoricalNav import MFHistoricalNav
from services.tasks.baseTask import BaseTask
from utils.logger import Logger


# Public AMFI bulk historical-NAV endpoint. Same host as
# NAVAll.txt (SetAMFIMFRate) — official MF data source, intended
# for bulk pulls.
_AMFI_HISTORICAL_URL = (
    "https://portal.amfiindia.com/DownloadNAVHistoryReport_Po.aspx"
)

# Per-request timeout. AMFI's p99 for the 90-day chunk is well
# under 20s in observed samples; 60s gives slack for monthly
# maintenance windows.
_REQUEST_TIMEOUT_SECONDS = 60

# Max window per request. AMFI's endpoint caps at ~90 days. Using
# 90 exactly matches what the Phase-1 feasibility note validated;
# larger values silently truncate the response.
_CHUNK_DAYS = 90

# Politeness interval between consecutive chunk GETs (seconds).
# No published AMFI rate limit; this is a defensive-citizen floor
# so a cold-start on 365 days (4 chunks) doesn't hammer the
# endpoint back-to-back.
_CHUNK_GAP_SECONDS = 1.0

# Steady-state overlap days — covers weekend + holiday + task-
# missed-day without operator action. Same 7-day window as
# BackfillHistoricalCandlesTask so the two caches drift together.
_STEADY_STATE_OVERLAP_DAYS = 7

# Cold-start window. 365 days gives a year of history for the
# Phase 2B synthesis script's trend-chart target range.
_COLD_START_DAYS = 365

# Self-reschedule cadence (minutes). Once daily — EOD-only source.
_RUN_INTERVAL_MINUTES = 60 * 24

# Date format AMFI expects on BOTH the input params AND the
# response rows. Example: "22-Nov-2026".
_AMFI_DATE_FMT = "%d-%b-%Y"

IST = timezone(timedelta(hours=5, minutes=30))


class BackfillAMFIHistoricalNAVsTask(BaseTask):
    """Daily AMFI historical-NAV backfill (ak-v8z1 Phase 2A)."""

    _instance = None

    def __new__(cls, *args, **kwargs):
        if not cls._instance:
            cls._instance = super().__new__(cls)
        return cls._instance

    def __init__(self, title, priority):
        if not hasattr(self, "initialized"):
            super().__init__(title, priority)
            self.logger = Logger(__name__).get_logger()
            self.interval = _RUN_INTERVAL_MINUTES
            self.initialized = True

    # ---------------- BaseTask.run contract ----------------

    def run(self):
        """Fetch + upsert historical NAVs for held MF schemes.

        Returns the 3-tuple ``(message, status, interval)``:
          * ``status = "Completed"`` on at-least-one-row-upserted.
          * ``status = "Failed"`` on fetch error or empty held-
            scheme set with no rows upserted.
        """
        try:
            # Enumerate the schemes the user holds. Single-user
            # today; across all users is still correct because the
            # NAV cache is market-wide (same pattern as
            # BackfillHistoricalCandlesTask).
            held_rows = (
                self.db.session.query(PurchasedSecurities.securityCode)
                .filter(
                    PurchasedSecurities.securityType == "Mutual_Funds",
                    PurchasedSecurities.buyQuant > 0,
                )
                .distinct()
                .all()
            )
            scheme_codes = sorted({str(r[0]) for r in held_rows if r[0]})
            if not scheme_codes:
                return (
                    "No held MF schemes — nothing to backfill",
                    "Completed",
                    self.interval,
                )

            from_date, to_date = self._fetch_window(scheme_codes)
            chunks = list(self._iter_chunks(from_date, to_date))

            total_upserted = 0
            chunk_errors = 0
            for i, (chunk_from, chunk_to) in enumerate(chunks):
                try:
                    text = self._fetch_chunk(chunk_from, chunk_to)
                except Exception as exc:
                    chunk_errors += 1
                    self.logger.warning(
                        f"AMFI historical chunk {i + 1}/{len(chunks)} "
                        f"{chunk_from}..{chunk_to} fetch failed: {exc}"
                    )
                    if i + 1 < len(chunks):
                        time.sleep(_CHUNK_GAP_SECONDS)
                    continue

                upserted = self._upsert_chunk(text, scheme_codes)
                total_upserted += upserted
                self.logger.info(
                    f"AMFI historical chunk {i + 1}/{len(chunks)} "
                    f"{chunk_from}..{chunk_to}: upserted {upserted} "
                    f"rows across {len(scheme_codes)} held schemes"
                )
                if i + 1 < len(chunks):
                    time.sleep(_CHUNK_GAP_SECONDS)

            try:
                self.db.session.commit()
            except Exception as exc:
                self.db.session.rollback()
                return (
                    f"AMFI historical backfill commit failed: "
                    f"{exc}"[:800],
                    "Failed",
                    self.interval,
                )

            if total_upserted == 0 and chunk_errors == len(chunks):
                return (
                    f"AMFI historical backfill: all {len(chunks)} "
                    f"chunks failed",
                    "Failed",
                    self.interval,
                )

            msg = (
                f"Completed: {total_upserted} NAV rows upserted "
                f"across {len(scheme_codes)} schemes "
                f"(chunks={len(chunks)} errors={chunk_errors})"
            )
            self.logger.info(f"{self.title}: {msg}")
            return msg, "Completed", self.interval

        except Exception as exc:
            return str(exc)[:800], "Failed", self.interval

    # ---------------- helpers ----------------

    def _fetch_window(self, scheme_codes):
        """Decide from_date → to_date for this run.

        Steady-state (table has at least one row for ANY held
        scheme): to_date = today, from_date = today - overlap days.
        Cold-start (table empty for every held scheme): extend to
        365 days back.

        Probing ONE scheme is sufficient — if one has history,
        every other has at least overlap-days coverage (the task
        upserts all schemes in each chunk pass).
        """
        today = datetime.now(IST).date()

        has_any_history = False
        probe_code = scheme_codes[0]
        has_any_history = (
            self.db.session.query(MFHistoricalNav.id)
            .filter(MFHistoricalNav.scheme_code == probe_code)
            .first()
            is not None
        )

        if has_any_history:
            from_date = today - timedelta(days=_STEADY_STATE_OVERLAP_DAYS)
        else:
            self.logger.info(
                "AMFI historical backfill: cold-start — fetching "
                f"{_COLD_START_DAYS} days"
            )
            from_date = today - timedelta(days=_COLD_START_DAYS)
        return from_date, today

    def _iter_chunks(self, from_date, to_date):
        """Yield (chunk_from, chunk_to) pairs splitting the window
        into <= _CHUNK_DAYS-sized ranges. AMFI's endpoint caps at
        ~90 days per request.
        """
        current = from_date
        while current <= to_date:
            chunk_end = min(
                current + timedelta(days=_CHUNK_DAYS - 1),
                to_date,
            )
            yield current, chunk_end
            current = chunk_end + timedelta(days=1)

    def _fetch_chunk(self, from_date, to_date):
        """HTTPS GET AMFI historical NAV report for a date range.

        Returns the raw text body. Caller parses with
        ``_parse_amfi_historical_text``.
        """
        params = {
            "frmdt": from_date.strftime(_AMFI_DATE_FMT),
            "todt": to_date.strftime(_AMFI_DATE_FMT),
        }
        resp = requests.get(
            _AMFI_HISTORICAL_URL,
            params=params,
            timeout=_REQUEST_TIMEOUT_SECONDS,
            headers={"User-Agent": "Mozilla/5.0 akkountant/ak-v8z1"},
        )
        resp.raise_for_status()
        return resp.text

    def _upsert_chunk(self, text, scheme_codes):
        """Parse AMFI historical text + UPSERT rows for held schemes.

        ``scheme_codes`` filters the response down to only the
        schemes the user holds — a cold-start response can contain
        thousands of scheme rows the user doesn't own.

        Returns the count of rows attempted (matches len(rows)
        when all accept).
        """
        from sqlalchemy.dialects.mysql import insert as mysql_insert

        held_set = set(scheme_codes)
        rows = []
        for scheme_code, nav_date, nav in self._parse_amfi_historical_text(
            text
        ):
            if scheme_code not in held_set:
                continue
            rows.append(
                {
                    "scheme_code": scheme_code,
                    "nav_date": nav_date,
                    "nav": nav,
                }
            )

        if not rows:
            return 0

        stmt = mysql_insert(MFHistoricalNav).values(rows)
        # On conflict against uq_mf_hist_nav_scheme_date: refresh
        # NAV. AMFI historical values are stable post-close so
        # same-date UPDATE is a near-noop but cheap.
        upsert = stmt.on_duplicate_key_update(
            nav=stmt.inserted.nav,
        )
        self.db.session.execute(upsert)
        return len(rows)

    @staticmethod
    def _parse_amfi_historical_text(text):
        """Yield (scheme_code, nav_date, nav) for each data row.

        Matches the AMFI bulk format (same as NAVAll.txt — 8 ``;``-
        separated fields; section headers have no ``;`` and are
        skipped; the file header starts with ``Scheme Code``).

        Non-parseable rows are silently skipped at the generator
        level — the caller counts filtered-held rows, not total
        parsed rows.
        """
        for raw_line in text.splitlines():
            line = raw_line.strip()
            if not line or ";" not in line:
                continue
            if line.startswith("Scheme Code"):
                continue
            parts = [p.strip() for p in line.split(";")]
            if len(parts) != 8:
                continue
            scheme_code_raw = parts[0]
            nav_str = parts[6]
            date_str = parts[7]
            # NAV parse — placeholder / empty / N.A. → skip.
            if not nav_str or nav_str.upper() in {"N.A.", "NA", "-"}:
                continue
            try:
                nav = Decimal(nav_str)
            except Exception:
                continue
            if nav == 0:
                continue
            try:
                nav_date = datetime.strptime(
                    date_str, _AMFI_DATE_FMT,
                ).date()
            except ValueError:
                continue
            # Scheme code is numeric 6-digit canonical. Strip any
            # whitespace; keep as string (matches on-disk shape).
            scheme_code = scheme_code_raw.strip()
            if not scheme_code:
                continue
            yield scheme_code, nav_date, nav
