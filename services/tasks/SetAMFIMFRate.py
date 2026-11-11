"""ak-nutd Phase 2: AMFI NAVAll.txt as MF NAV source (replaces mfapi.in path).

## Why this task exists

Overseer directive 2026-11-11: pivot MF rate source from mfapi.in's
38K-URL batch (145K 429s observed Nov 2026, ~18min wall-clock per cycle)
to AMFI's official single-file NAV publication. AMFI publishes end-of-day
post-market-close at ``https://portal.amfiindia.com/spages/NAVAll.txt``.

Shape:
  * Single HTTPS GET (~1.5MB semicolon-separated text)
  * Trivial parse time vs ~18min mfapi.in batch (~1000× latency win)
  * Zero throttling (official bulk source — intended for this use)
  * Zero auth required; standard public endpoint

## lastNav / lastDate persistence (Option A — Overseer-chosen 2026-11-11)

mfapi.in returned each scheme's NAV history. ``_process_responses``
extracted today (nav_data[0]) and yesterday (nav_data[1]) → MFRate.json
entries carried ``lastNav``/``lastDate`` consumed by
``InvestmentService.fetchUserSecurities`` L322-329 for per-MF change
computation.

AMFI's NAVAll.txt is a snapshot — today only. Option A (Overseer Q1
answer): persist today's parsed map as both ``MFRate.json`` (today's
values via BaseRateTask.save_json → safe_replace_file) AND
``MFRate.prior.json`` (a flat-copy rotated AFTER the swap so the NEXT
cycle can read yesterday's values).

Per-cycle order of operations:
  1. Read ``MFRate.prior.json`` if present → yesterday's map
     {scheme_id → {nav, date}}
  2. HTTPS GET + parse AMFI → today's map
  3. For each scheme in today's map, lookup prior-map by
     ``scheme_id`` and attach ``lastNav``/``lastDate`` when present
  4. Return the 5-tuple to BaseRateTask.run() for the standard
     write-and-swap pipeline
  5. On Completed status (hard-floor + 98% gates passed, file
     successfully swapped into place), rotate just-written
     ``MFRate.json`` → ``MFRate.prior.json`` for next cycle

Cold-start transient: first run after a prior-file reset has no
``lastNav``/``lastDate`` for any scheme. FE change computation falls
back to 0/0% until the second cycle warms the cache. One-day transient
per deploy — accepted by Overseer Q1 as the Option-A trade-off.

## Scheduler dispatch (env-var gated)

TASK_MAPPING in services/tasks/scheduler.py routes the ``SetMFRate``
job title based on env var ``MF_RATE_SOURCE``:
  * ``MF_RATE_SOURCE=mfapi`` (default, Phase 2 pre-cutover): SetMFRate
  * ``MF_RATE_SOURCE=amfi`` (Phase 2 observation / Phase 3+): SetAMFIMFRate

Job title stays ``SetMFRate`` so cronAgent, bead references, UI, and
``jobsObject`` labels remain stable. Only the implementation class
behind the title shifts.

Phase 3 cutover (post-observation): flip default to ``amfi`` in
TASK_MAPPING; SetMFRate task class stays available as fallback but
not scheduled. Phase 4 removes SetMFRate module entirely.

## Scheduler cadence (Overseer Q2)

Interval shifts from mfapi.in's 150-min self-reschedule to once-daily
post-market-close. AMFI publishes EOD; no gain from intraday polls.
``self.interval = 60 * 24  # 24 hours``.

## Divergence observation (Phase 2 — operator responsibility)

Phase 2 dual-run comparison happens OFFLINE, not in-process:
  * Operator deploys with ``MF_RATE_SOURCE=amfi`` on staging / monitored prod
  * Observe ~1 week of production output (``MFRate.json``)
  * Diff against archived ``MFRate.json`` from mfapi.in path (BaseTask
    retains ``_KEEP_HISTORICAL_FILES=3`` previous versions on disk)
  * Overseer Q3 threshold: 0.5% delta on >10 schemes OR 0.1% delta on
    >100 schemes flags cutover-blocking divergence

In-process cross-fetch of both sources would defeat AMFI's latency win
(would re-run the 38K-URL mfapi.in batch each cycle). Offline diff via
operator tooling is the correct shape.

## Coverage-gate semantics (BaseRateTask contract)

The ``_fetch_all`` 5-tuple treats EACH DATA ROW in the file as a fetch
universe member:
  * ``total`` = parseable-shape rows (passed ``;``-split + field-count
    + date-parse checks)
  * ``permanent_skips`` = {'stale': count_of_pre_window_rows} —
    excluded from denominator like ``permanent_404`` for mfapi.in
    (legitimately not in today's answerable universe)
  * ``ok`` = non-zero, non-NA, valid-NAV rows that landed in result_map
  * ``answerable = total - stale`` → ratio = ok / answerable

Expected shape on a normal day: ratio ≈ 0.99+ (most non-wound-down
schemes publish today's NAV). Gate activations:
  * ``answerable == 0`` (everything stale → dead AMFI feed): degenerate
    → last-good preserved
  * ratio < 0.5 (many zero/NA responses — upstream regression): hard
    floor → last-good preserved
  * 0.5 ≤ ratio < 0.98: partial-success → file written, status=Failed
  * ratio ≥ 0.98: Completed

Same shape SetMFRate uses post-ak-539. No behavior change for the gate
machinery — only the underlying fetch semantics shift from HTTP-batch
to single-file-parse.
"""

from __future__ import annotations

import json
import os
import shutil
from datetime import datetime, timedelta

import requests

from services.tasks.BaseRateTask import BaseRateTask
from utils.logger import Logger


# Public AMFI bulk-NAV endpoint. Documented as the canonical source for
# daily MF NAV data; used by every Indian MF data consumer.
_AMFI_URL = "https://portal.amfiindia.com/spages/NAVAll.txt"

# Per-request timeout. AMFI's p99 over 30-day sample is well under 15s;
# 60s gives slack for monthly maintenance windows without stretching a
# bad cycle into minutes.
_REQUEST_TIMEOUT_SECONDS = 60

# Stale-filter threshold (calendar days). AMFI lists wound-down /
# segregated portfolios with ancient NAV dates. Filter these at parse
# time so the result_map reflects the ACTIVE scheme universe. 7 days
# gives headroom for weekends + minor holidays while rejecting
# truly-stale schemes.
_STALE_THRESHOLD_DAYS = 7

# Prior-snapshot filename. Written AFTER a successful swap each cycle;
# read at the start of the NEXT cycle to populate lastNav/lastDate.
# Lives under task_tmp (not under rates/ with history rotation) so
# BaseTask's _KEEP_HISTORICAL_FILES retention does NOT apply — we want
# exactly one prior file, overwritten each cycle.
_PRIOR_SNAPSHOT_FILENAME = "MFRate.prior.json"

# NAV-value strings that AMFI emits for placeholder / absent rows.
# Historically observed: "N.A.", "NA", literal "-" in nav column
# (rare), leading/trailing whitespace. Zero NAV (bare "0" or "0.0000")
# also indicates placeholder / wound-down; filter alongside.
_NAV_PLACEHOLDER_STRINGS = frozenset({"N.A.", "NA", "NAN", "-", ""})


class SetAMFIMFRate(BaseRateTask):
    """AMFI-NAV-file MF rate task (ak-nutd Phase 2).

    Inherits BaseRateTask's hard-floor / 98% / partial-success gates
    (identical to SetMFRate's post-ak-539 shape). Overrides ``run()``
    to rotate the prior-snapshot post-swap.
    """

    _instance = None

    # Same on-disk contract as SetMFRate — writes MFRate.json via the
    # MfRatePrefix rotation. Consumers (JsonDownloadService.getMFRate,
    # InvestmentService.fetchUserSecurities) are unchanged.
    _rate_filename = "MFRate.json"
    _rate_prefix_attr = "MfRatePrefix"

    # Default BaseRateTask gates (0.98 / 0.5) — same thresholds as
    # SetMFRate. Explicit here (not just inheriting) so the ak-539
    # provenance stays discoverable at the task itself.
    # (Imported from the BaseRateTask module via its own module-level
    # defaults; explicit duplication preserved for grep continuity
    # across SetMFRate + SetAMFIMFRate.)
    _MIN_SUCCESS_RATIO = 0.98
    _COVERAGE_HARD_FLOOR = 0.5

    def __new__(cls, *args, **kwargs):
        if not cls._instance:
            cls._instance = super().__new__(cls)
        return cls._instance

    def __init__(self, title, priority):
        if not hasattr(self, "initialized"):
            super().__init__(title, priority)
            self.logger = Logger(__name__).get_logger()
            # Overseer Q2 (2026-11-11): EOD-only cadence.
            # AMFI publishes once per day post-market-close; no gain
            # from sub-daily polls, and a 24h interval reduces load
            # on the shared scheduler loop.
            self.interval = 60 * 24  # minutes → 24h self-reschedule
            self.initialized = True

    # ---------------- BaseRateTask contract ----------------

    def _fetch_all(self):
        """Return ``(data, total, ok, permanent_skips, extras)``.

        Fetch failure → ``(None, 0, 0, {}, {'error': msg})``;
        BaseRateTask.run() short-circuits to Failed without clobbering
        last-good.

        Successful fetch → parse NAVAll.txt into per-scheme entries
        with ``lastNav``/``lastDate`` merged from the prior snapshot.
        """
        try:
            resp = requests.get(
                _AMFI_URL,
                timeout=_REQUEST_TIMEOUT_SECONDS,
                headers={"User-Agent": "Mozilla/5.0 akkountant/ak-nutd"},
            )
            resp.raise_for_status()
            text = resp.text
        except Exception as exc:
            msg = f"AMFI fetch failed: {exc}"[:800]
            self.logger.error(f"{self.title}: {msg}")
            return None, 0, 0, {}, {"error": msg}

        prior_map = self._read_prior_snapshot()

        parsed_map, total_rows, stale_skipped, zero_skipped, malformed_skipped = (
            self._parse_amfi_text(text, prior_map=prior_map)
        )

        self.logger.info(
            f"AMFI parse: text_bytes={len(text)} "
            f"total_rows={total_rows} parsed={len(parsed_map)} "
            f"stale_skipped={stale_skipped} zero_skipped={zero_skipped} "
            f"malformed_skipped={malformed_skipped} "
            f"prior_snapshot_schemes={len(prior_map)}"
        )

        # Preserve the mfapi.in on-disk shape: {"data": [entry, ...]}.
        # Consumer (JsonDownloadService.getMFRate) iterates ['data']
        # and matches on scheme_id — contract unchanged.
        data = {"data": list(parsed_map.values())}

        # Coverage math:
        #   total = all rows that passed structural parse (looked like
        #           a data row; date parseable)
        #   stale = rows older than _STALE_THRESHOLD_DAYS (permanent
        #           skip — not in today's answerable universe,
        #           analogous to mfapi.in permanent_404)
        #   answerable = total - stale
        #   ok = len(parsed_map) = non-stale, non-zero, non-NA, valid
        return (
            data,
            total_rows,
            len(parsed_map),
            {"stale": stale_skipped},
            {
                "stale": stale_skipped,
                "zero": zero_skipped,
                "malformed": malformed_skipped,
            },
        )

    def _completed_msg(self, ok, answerable, permanent_skips, extras):
        """Override default Completed message to surface the parse-time
        stats (stale/zero/malformed counts). Grep-friendly key=value
        form for log analysis.
        """
        return (
            f"Completed successfully ({ok}/{answerable} answerable; "
            f"stale={extras.get('stale', 0)} "
            f"zero={extras.get('zero', 0)} "
            f"malformed={extras.get('malformed', 0)})"
        )

    # ---------------- Override run() for post-swap prior rotation ----------------

    def run(self):
        """Wrap BaseRateTask.run with prior-snapshot rotation.

        Rotation is a POST-COMMIT step: only rotate if the new
        MFRate.json actually landed on disk (passed hard-floor + swap
        gates → status=Completed). Partial-success / hard-floor-fail
        states must NOT advance the prior — doing so would make the
        NEXT cycle's lastNav point at a degraded snapshot.
        """
        result, status, interval = super().run()
        if status == "Completed":
            self._rotate_prior_snapshot()
        return result, status, interval

    # ---------------- Prior-snapshot helpers ----------------

    def _read_prior_snapshot(self):
        """Read MFRate.prior.json for lastNav/lastDate lookup.

        Returns ``{scheme_id (str) → {'nav': str, 'date': str}}``.
        On missing file (cold start) or malformed JSON: returns ``{}``
        and logs INFO (cold-start) or WARN (malformed). Cold start is
        a normal first-run condition, not an error.
        """
        prior_path = os.path.join(self.tmp_dir, _PRIOR_SNAPSHOT_FILENAME)
        if not os.path.isfile(prior_path):
            self.logger.info(
                f"AMFI prior-snapshot not present at {prior_path} — "
                f"cold start (lastNav/lastDate will be absent this cycle)"
            )
            return {}
        try:
            with open(prior_path, "r") as f:
                prior = json.load(f)
        except (OSError, json.JSONDecodeError) as exc:
            self.logger.warning(
                f"AMFI prior-snapshot read failed ({prior_path}): {exc} — "
                f"degrading to cold-start (lastNav/lastDate will be absent)"
            )
            return {}
        prior_map = {}
        for entry in prior.get("data", []):
            sid = entry.get("scheme_id")
            if sid is None:
                continue
            prior_map[str(sid)] = {
                "nav": entry.get("nav"),
                "date": entry.get("date"),
            }
        return prior_map

    def _rotate_prior_snapshot(self):
        """Copy just-written MFRate.json → MFRate.prior.json.

        Called from ``run()`` ONLY on Completed status (post-successful
        swap). Non-fatal on failure: next cycle's lastNav will be stale
        but current file write already succeeded.
        """
        prior_path = os.path.join(self.tmp_dir, _PRIOR_SNAPSHOT_FILENAME)
        try:
            current_path = self.jsonService.getLatestFile(
                self.jsonService.ratesType, self.jsonService.MfRatePrefix
            )
            if not current_path or not os.path.isfile(current_path):
                self.logger.warning(
                    f"AMFI prior-snapshot rotation skipped: current "
                    f"MFRate.json not found at expected location"
                )
                return
            shutil.copy2(current_path, prior_path)
            self.logger.info(
                f"AMFI prior-snapshot rotated: {current_path} → {prior_path}"
            )
        except Exception as exc:
            # Non-fatal — the primary write already succeeded. Next
            # cycle will have stale lastNav (same effect as cold start)
            # but all current values are correct.
            self.logger.warning(
                f"AMFI prior-snapshot rotation failed: {exc} — "
                f"next cycle will cold-start lastNav lookups"
            )

    # ---------------- AMFI text parser ----------------

    def _parse_amfi_text(self, text, *, prior_map):
        """Parse AMFI NAVAll.txt body into ``{scheme_code: entry}``.

        File shape (verified 2026-11-11 WebFetch):
          * Header line: ``Scheme Code;ISIN Div Payout/ISIN Growth;...;Date``
          * Section markers (no ``;``): fund-house names (e.g. ``Axis
            Mutual Fund``) and category labels (e.g. ``Open Ended
            Schemes(Debt Scheme - Gilt Fund)``)
          * Blank lines between groups
          * Data rows: 8 semicolon-separated fields

        Returns:
          ``(parsed_map, total_rows, stale_skipped, zero_skipped,
             malformed_skipped)``

        Skipping rules (count toward malformed_skipped unless stated):
          * No ``;`` in line → section header, silent skip (NOT counted)
          * Line starts with ``Scheme Code`` → file header, silent skip
          * Field count != 8 → malformed
          * Scheme code not canonicalizable → malformed
          * Date not parseable as DD-Mon-YYYY → malformed
          * Date older than _STALE_THRESHOLD_DAYS → counted as stale
            (permanent_skip bucket — excluded from denominator)
          * NAV is placeholder ("N.A.", "NA", "0", "0.0000", empty) →
            counted as zero (in denominator but not in result)
          * NAV not float-parseable → counted as zero

        State tracking during parse:
          * ``current_fund_house``: most-recently-seen non-``;`` line
            matching fund-house heuristic (ends with ``Mutual Fund`` or
            similar). Attached to every subsequent data row.
          * ``current_category``: most-recently-seen non-``;`` line
            containing ``Scheme`` keyword. Attached as ``schemeType``.

          Both default to empty string if the file starts with data
          rows (unexpected; AMFI's format reliably emits headers first).
        """
        parsed_map = {}
        total_rows = 0
        stale_skipped = 0
        zero_skipped = 0
        malformed_skipped = 0

        now = datetime.now()
        stale_threshold = now - timedelta(days=_STALE_THRESHOLD_DAYS)

        current_fund_house = ""
        current_category = ""

        for raw_line in text.splitlines():
            line = raw_line.strip()
            if not line:
                continue
            if ";" not in line:
                # Section marker: fund-house name or category label.
                # Simple heuristic classification — overlap is possible
                # (some fund-house names might contain "Scheme"), but
                # the ordering in AMFI's file is reliably:
                #   category → fund-house → data rows
                # Treating any Scheme-containing line as category and
                # everything else as fund-house gives correct
                # attribution on the observed format.
                if "Scheme" in line or line.startswith("Open Ended") \
                        or line.startswith("Close Ended") \
                        or line.startswith("Interval"):
                    current_category = line
                else:
                    current_fund_house = line
                continue
            if line.startswith("Scheme Code"):
                # File header — skip silently (not counted as malformed).
                continue

            parts = [p.strip() for p in line.split(";")]
            if len(parts) != 8:
                malformed_skipped += 1
                continue
            (
                scheme_code_raw,
                _isin_div_or_growth,
                _isin_div_reinvest,
                scheme_name,
                _plan,
                _option,
                nav_str,
                date_str,
            ) = parts

            # Scheme code canonicalization (matches SetMFRate's L303).
            try:
                scheme_code = self.jsonService.normalize_scheme_code(
                    scheme_code_raw
                )
            except ValueError:
                malformed_skipped += 1
                continue

            # Date parse — AMFI's DD-Mon-YYYY (e.g. "09-Nov-2026").
            try:
                date_obj = datetime.strptime(date_str, "%d-%b-%Y")
            except ValueError:
                malformed_skipped += 1
                continue

            # Row passed structural parse — counts toward total.
            total_rows += 1

            # Stale filter (permanent-skip bucket): wound-down /
            # segregated portfolios list ancient NAV dates. Excluded
            # from the answerable denominator so a legitimate 100% of
            # answerable schemes publishing today still ratios to 1.0.
            if date_obj < stale_threshold:
                stale_skipped += 1
                continue

            # NAV placeholder / zero filter (in denominator but not
            # result — drops ratio if AMFI emits placeholder waves).
            nav_cleaned = nav_str.strip()
            if nav_cleaned.upper() in _NAV_PLACEHOLDER_STRINGS:
                zero_skipped += 1
                continue
            try:
                nav_float = float(nav_cleaned)
            except ValueError:
                zero_skipped += 1
                continue
            if nav_float == 0:
                zero_skipped += 1
                continue

            # Normalize date to DD-MM-YYYY to match mfapi.in on-disk
            # contract (consumer: InvestmentService L322-329).
            date_canonical = date_obj.strftime("%d-%m-%Y")

            entry = {
                "scheme_id": scheme_code,
                "nav": nav_cleaned,  # string shape matches mfapi.in
                "date": date_canonical,
                "fundHouse": current_fund_house,
                "schemeType": current_category,
            }

            # Merge lastNav/lastDate from prior snapshot (Option A).
            # Absent for cold-start cycles; FE falls back to 0/0% change
            # until next cycle warms the cache.
            prior_entry = prior_map.get(scheme_code)
            if prior_entry:
                prior_nav = prior_entry.get("nav")
                prior_date = prior_entry.get("date")
                if prior_nav is not None:
                    entry["lastNav"] = prior_nav
                if prior_date is not None:
                    entry["lastDate"] = prior_date

            parsed_map[scheme_code] = entry

        return (
            parsed_map,
            total_rows,
            stale_skipped,
            zero_skipped,
            malformed_skipped,
        )
