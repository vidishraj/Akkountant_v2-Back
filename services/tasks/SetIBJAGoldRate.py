import os
from datetime import datetime

import requests
from bs4 import BeautifulSoup

from services.tasks.AIRateTask import AIRateTask
from utils.logger import Logger


# Upstream URL for IBJA daily gold/silver rates.
# Pre-ak-ad1: LLM-extracted (sonnet reads the HTML and returns structured
# output). Post-ak-ad1 Phase 2 (2026-11-11): optional direct-scrape path
# via env var GOLD_SOURCE=scrape; see _fetch_rates_via_scrape below.
GOLD_URL = "https://ibjarates.com/"

# ak-ad1 Phase 2 (2026-11-11): env-var dispatch between AI and direct-
# scrape paths.
#   * GOLD_SOURCE unset / `ai`  — LLM extraction (pre-ak-ad1 behavior)
#   * GOLD_SOURCE=scrape        — direct BeautifulSoup scrape (ak-ad1 Phase 2 primary)
#
# Default stays `ai` so Phase 2 rollout is opt-in. Phase 3 cutover flips
# default to `scrape`. Phase 4 removes the AI path entirely per
# Overseer Q4 answer (2026-11-11): "drop LLM ENTIRELY from gold path
# post-cutover, NOT keep-as-fallback".
_GOLD_SOURCE_ENV = "GOLD_SOURCE"

# Scrape-path HTTP settings.
_SCRAPE_TIMEOUT_SECONDS = 30

# ak-ad1 Phase 2: GST multiplier applied post-extract. Identical to the
# LLM prompt's inline formula (`avg_with_gst = int(avg_price * 1.03)`);
# hoisted here so BOTH paths use the same constant. Phase 3 cleanup
# removes the duplicate inline value from GOLD_SYSTEM_PROMPT once the
# AI path is dropped.
_GST_MULTIPLIER = 1.03

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
#
# ak-znk Fix-B1: minimum/maximum bounds on carat_24/22/18 catch LLM
# scale-drift (recovery-run bug: sonnet returned ~1% of actual, e.g. ₹80
# instead of ₹8000 — a per-gram value where per-10-gram was expected).
# Prior schema was `{"type": "integer"}` with no bounds → schema-valid
# but implausible integers passed all downstream checks and hit disk.
# With bounds, `output_format` retries within max_turns=3; if all 3 miss,
# `structured_output is None` → BaseRateTask.run() data-is-None guard
# preserves last-good on disk (ak-2r8 semantics).
#
# Bounds derived from IBJA historical INR-per-10g ranges (2015-2026):
#   * carat_24 (999-purity): historical range ~₹25k (2015 low) → ~₹90k
#     (2024 high). Setting strict floor ₹20k (below 2015 low with margin)
#     + ceiling ₹250k (2.5× current peak — catches a real market
#     dislocation without rejecting a legitimate spike). 24k is the
#     REQUIRED purity per the schema design — never legitimately 0.
#   * carat_22 (916): typically ~92% of 24k. Ceiling ₹230k.
#   * carat_18 (750): typically ~75% of 24k. Ceiling ₹190k.
#
# ak-znk v2 Fix-1 (reviewer Focus-5): carat_22 and carat_18 are OPTIONAL
# purities per the schema comment ("zero-or-missing purities gracefully").
# v1 imposed strict min ₹18k/₹15k which contradicted that contract —
# harmless today (IBJA publishes all 3 daily) but becomes a real bite
# under ak-ad1 pivot: scrape/API sources may legitimately emit 0 for
# absent purities → single valid carat_24 run gets false-rejected →
# gold sticks on last-good forever. v2 accepts zero as "not published
# this cycle" at BOTH layers (schema minimum=0 here + B3 skip-on-zero
# below). Ceiling stays as sanity guard; scale-drift on carat_22/18
# gets caught by the ceiling and by the same guard fires against
# non-zero values below the historical floor if any real dislocation
# ever happens (though the ceiling case is the one Fix-B1 is really
# aimed at).
#
# Note: ak-ad1 pivot will replace this schema entirely; bounds are
# throwaway once the AI path is retired for Gold. Kept here as the
# tactical fix for ak-znk per Lead's GO.
GOLD_SCHEMA = {
    "type": "object",
    "properties": {
        "carat_24": {"type": "integer", "minimum": 20000, "maximum": 250000},
        # v2 Fix-1: minimum=0 permits "not-published-this-cycle"; ceiling
        # preserved as sanity guard against scale-drift.
        "carat_22": {"type": "integer", "minimum": 0, "maximum": 230000},
        "carat_18": {"type": "integer", "minimum": 0, "maximum": 190000},
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

# ak-znk Fix-B3: post-extract plausibility bands.
# Defense-in-depth over Fix-B1's schema minimum/maximum — schema
# enforcement via `output_format` has been observed as flaky under
# model degradation (e.g. sonnet emitting integer-valued but
# semantically implausible values that STILL satisfied the JSON schema
# in some SDK edge cases). This band-check runs post-extract, INSIDE
# _fetch_all, and returns (None, ...) on breach → BaseRateTask.run()
# data-is-None guard preserves last-good.
#
# Bands are IDENTICAL to Fix-B1's schema minimum/maximum by design so
# operators only have one set of bounds to reason about. The two layers
# are redundant on purpose:
#   * Schema layer (B1): catches issues at model/SDK boundary; more
#     defensive against future schema-tightening.
#   * Post-extract layer (B3): catches issues even if schema
#     enforcement flakes; survives the ak-ad1 pivot (deterministic
#     sources — direct scrape / API — can ALSO produce out-of-range
#     values if the upstream drifts, e.g., a decimal-shift bug in
#     mmtcpamp.com's HTML → same class of failure).
# Keep them in sync when tuning; comment above `_GOLD_PLAUSIBLE_BANDS`
# in this file + the schema block above both need to be updated
# together.
_GOLD_PLAUSIBLE_BANDS = {
    "carat_24": (20000, 250000),
    "carat_22": (18000, 230000),
    "carat_18": (15000, 190000),
}

# ak-znk v2 Fix-2 (reviewer): sync-guard between GOLD_SCHEMA bounds and
# _GOLD_PLAUSIBLE_BANDS. Import-time assert catches accidental drift
# where a future edit tunes one layer without the other. Cheap drift-
# insurance for the two-layer defense-in-depth pattern (Fix-B1 schema
# + Fix-B3 post-extract).
#
# Asymmetry note: after v2 Fix-1, the schema minimum for optional
# purities (carat_22/18) is 0 (accept "not-published-this-cycle"); the
# band-lo in _GOLD_PLAUSIBLE_BANDS is the true nonzero floor (18000 /
# 15000). We assert only the CEILING (schema maximum vs band hi) for
# the optional purities — the true guarantee both layers make is
# "reject scale-drift above the ceiling"; the floor is intentionally
# different because Fix-B3 skips zero for optionals. carat_24 asserts
# both minimum + maximum since it's the required purity with
# symmetric strict bounds at both layers.
for _k, (_lo, _hi) in _GOLD_PLAUSIBLE_BANDS.items():
    _schema_hi = GOLD_SCHEMA["properties"][_k].get("maximum")
    assert _schema_hi == _hi, (
        f"GOLD_SCHEMA['{_k}'].maximum ({_schema_hi}) != "
        f"_GOLD_PLAUSIBLE_BANDS['{_k}'][1] ({_hi}) — "
        f"ak-znk Fix-B1/B3 sync broken (v2)"
    )
    if _k == "carat_24":
        _schema_lo = GOLD_SCHEMA["properties"][_k].get("minimum")
        assert _schema_lo == _lo, (
            f"GOLD_SCHEMA['{_k}'].minimum ({_schema_lo}) != "
            f"_GOLD_PLAUSIBLE_BANDS['{_k}'][0] ({_lo}) — "
            f"ak-znk Fix-B1/B3 sync broken (v2, required-purity floor)"
        )
    # ak-znk v3 (reviewer Focus-1(c)): guard the schema.minimum==0
    # invariant for optional purities. Fix-1's zero-passthrough in B3
    # only works if the LLM is ALLOWED to emit 0 at the schema layer.
    # A future "helpful" re-addition of a floor (e.g. back to 18000
    # for carat_22) would block LLM zero emission → output_format
    # retries fail → structured_output None → last-good preserved
    # forever → the exact Focus-5 bug silently returns at the schema
    # layer. Fail loud at import time instead of silently on prod.
    if _k in {"carat_22", "carat_18"}:
        _schema_lo_optional = GOLD_SCHEMA["properties"][_k].get("minimum")
        assert _schema_lo_optional == 0, (
            f"GOLD_SCHEMA['{_k}'].minimum ({_schema_lo_optional}) != 0 — "
            f"ak-znk Fix-1 optional-purity zero-passthrough broken (v3, "
            f"re-adding a floor blocks LLM from emitting legit zeros)"
        )


class SetIBJAGoldRate(AIRateTask):
    _instance = None

    # ak-2r8: BaseRateTask contract.
    _rate_filename = 'GOLDRATE.json'
    _rate_prefix_attr = 'GoldRatePrefix'

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

    def _fetch_all(self):
        """ak-2r8: BaseRateTask contract. Universe-of-1 semantics
        (either the whole extract succeeds or the whole thing fails).
        Failure paths preserve pre-refactor error-message shape via
        ``extras['error']``.

        ak-ad1 Phase 2 (2026-11-11): dispatches between AI and direct-
        scrape paths on env var ``GOLD_SOURCE``. Both paths produce the
        SAME jsonData shape (``carat_24``/``22``/``18`` + ``ibja_data``)
        so the downstream Fix-B3 band check + ``_GOLD_DISPLAY_KEYS``
        remap apply identically regardless of source.

        v4 (hq-wisp-il849, pre-ak-ad1): reverted from the pinned-sonnet
        diag probe back to the default bare "sonnet" alias for the AI
        path. Pre-ak-ad1 default = AI; Phase 3 cutover flips default
        to scrape; Phase 4 removes AI path entirely.
        """
        source = os.getenv(_GOLD_SOURCE_ENV, "ai").lower()

        if source == "scrape":
            # ak-ad1 Phase 2 primary. Deterministic BeautifulSoup
            # extraction against stable `span#lblGold<Purity>_<AM|PM>`
            # selectors. No per-cycle LLM turn cost; no schema-valid-
            # but-implausible failure class (ak-znk root cause).
            jsonData, fetch_err = self._fetch_rates_via_scrape()
            source_label = "scrape"
        else:
            # Pre-ak-ad1 default. Phase 4 will remove this branch.
            jsonData, fetch_err = self.fetch_rates_via_ai(
                url=GOLD_URL,
                system_prompt=GOLD_SYSTEM_PROMPT,
                schema=GOLD_SCHEMA,
                agent="rate.gold",
            )
            source_label = "ai"

        if jsonData is None:
            msg = f"Failed to get Gold Rates ({source_label}): {fetch_err}"[:800]
            return None, 1, 0, {}, {'error': msg}
        if 'carat_24' not in jsonData:
            got_keys = list(jsonData.keys())[:10]
            msg = (
                f"Gold Rates response missing 'carat_24'. Got "
                f"top-level keys: {got_keys}"
            )[:800]
            return None, 1, 0, {}, {'error': msg}

        # ak-znk Fix-B3: post-extract plausibility band check.
        # Defense-in-depth over Fix-B1's schema minimum/maximum.
        # On out-of-range: return same (None, 1, 0, {}, {'error': ...})
        # shape used by the missing-key branch above so BaseRateTask.run()'s
        # data-is-None guard fires exactly once (no double emit, no
        # skipped state divergence). Uses the SAME bands as Fix-B1's
        # schema — see _GOLD_PLAUSIBLE_BANDS.
        #
        # ak-znk v2 Fix-1 (reviewer Focus-5): carat_22 and carat_18 are
        # OPTIONAL purities per the schema `zero-or-missing purities
        # gracefully` design note. Skip band-check on ZERO for those
        # (mirrors the schema's minimum=0 relaxation for optional
        # purities). carat_24 stays strict — it's the required purity
        # and can never legitimately be 0.
        _ZERO_ALLOWED = {"carat_22", "carat_18"}
        for k_schema, (band_lo, band_hi) in _GOLD_PLAUSIBLE_BANDS.items():
            v = jsonData.get(k_schema)
            if v is None:
                # Purity absent from response — presence-check for
                # carat_24 above catches the strict-required case; the
                # other purities being missing (rare) is acceptable per
                # the schema's `zero-or-missing purities gracefully`
                # design note. Do NOT reject on missing here; only
                # reject on present-but-out-of-range.
                continue
            try:
                v_num = float(v)
            except (TypeError, ValueError):
                msg = (
                    f"Gold Rates {k_schema}={v!r} is not numeric — "
                    f"schema-valid-but-implausible response class "
                    f"(ak-znk Fix-B3); preserving last-good on disk"
                )[:800]
                return None, 1, 0, {}, {'error': msg}
            # v2 Fix-1: zero is "not-published-this-cycle" for optional
            # purities; skip band-check to preserve the graceful-
            # missing contract.
            if v_num == 0 and k_schema in _ZERO_ALLOWED:
                continue
            if not (band_lo <= v_num <= band_hi):
                msg = (
                    f"Gold Rates {k_schema}={v_num} outside plausible "
                    f"band [{band_lo}, {band_hi}] INR per 10g — "
                    f"scale-drift / unit-drift class (ak-znk Fix-B3); "
                    f"preserving last-good on disk"
                )[:800]
                return None, 1, 0, {}, {'error': msg}

        # Re-map LLM-schema-compliant keys → on-disk display keys. The
        # frontend reads GoldRate.json with the historical "24 Carat" /
        # "22 Carat" / "18 Carat" top-level keys (see
        # JsonDownloadService.getGoldRate, GoldService consumers); that
        # on-disk contract is unchanged by v4.
        #
        # ak-ad1 Phase 2 (2026-11-11): the scrape path emits the same
        # carat_24/22/18 key shape as the AI path, so this remap
        # applies identically regardless of source.
        for k_schema, k_display in _GOLD_DISPLAY_KEYS:
            if k_schema in jsonData:
                jsonData[k_display] = jsonData.pop(k_schema)

        # ak-ad1 Phase 2 (2026-11-11): structured observability log for
        # dual-run divergence detection. Lead flag for the 1-week /
        # 14-consecutive-cycle observation window: operator / cron
        # tooling greps `gold_rate_cycle ` to chart per-cycle values
        # per source. Enables early-warning "no streak forming by
        # cycle 10" detection.
        display_24 = jsonData.get("24 Carat")
        display_22 = jsonData.get("22 Carat")
        display_18 = jsonData.get("18 Carat")
        self.logger.info(
            f"gold_rate_cycle source={source_label} "
            f"carat_24={display_24} carat_22={display_22} "
            f"carat_18={display_18}"
        )

        return jsonData, 1, 1, {}, {}

    # ────────────── ak-ad1 Phase 2 scrape path ──────────────────────

    def _fetch_rates_via_scrape(self):
        """Direct BeautifulSoup extraction of IBJA today-rates table.

        Returns ``(jsonData, err)`` matching ``fetch_rates_via_ai``'s
        shape:
          * ``jsonData``: dict with ``carat_24``/``22``/``18`` (ints,
            GST-included) + ``ibja_data`` (AM/PM per-purity + date +
            currency + source).
          * ``err``: None on success; string on failure (returned
            jsonData=None).

        Selectors (locked against 2026-11-11 IBJA HTML, test fixture at
        ``test_fixtures/ibja_today_rates_2026_11_11.html``):
          * ``span#lblGold<Purity>_AM``   → AM price (int, INR per 10g)
          * ``span#lblGold<Purity>_PM``   → PM price
          * ``span#lblSilver999_<AM|PM>`` → silver per kg

        Same math as ``GOLD_SYSTEM_PROMPT``:
          * avg_price = round((am + pm) / 2)
          * carat_N  = int(avg_price * _GST_MULTIPLIER)  # truncate

        Failure classes:
          * Network / HTTP error → returns (None, err_str)
          * Required selector absent → returns (None, "selector miss: ...")
          * Selector value not int → returns (None, "value not int: ...")

        All failures fall through to the None-tuple from
        ``_fetch_all``; BaseRateTask.run() preserves last-good on disk.
        """
        try:
            resp = requests.get(
                GOLD_URL,
                timeout=_SCRAPE_TIMEOUT_SECONDS,
                headers={"User-Agent": "Mozilla/5.0 akkountant/ak-ad1"},
            )
            resp.raise_for_status()
            html = resp.text
        except Exception as exc:
            return None, f"IBJA scrape fetch failed: {exc}"

        try:
            return self._parse_ibja_html(html), None
        except ValueError as exc:
            # Parser-detected selector miss / value coerce failure.
            # Preserve the specific message so operators see WHICH
            # selector drifted (not just "IBJA scrape failed").
            return None, f"IBJA scrape parse failed: {exc}"

    def _parse_ibja_html(self, html):
        """Pure-function parser — takes HTML string, returns jsonData
        dict shaped exactly like the AI path output.

        Extracted as a separate method so the golden-fixture regression
        test can drive the parser without touching ``requests.get``.
        """
        soup = BeautifulSoup(html, "html.parser")

        # Extract all 12 price spans. Missing span or non-integer value
        # raises ValueError (caught by caller + surfaced as scrape
        # parse-failure for operator triage).
        g_999_am = self._extract_span_int(soup, "lblGold999_AM")
        g_999_pm = self._extract_span_int(soup, "lblGold999_PM")
        g_995_am = self._extract_span_int(soup, "lblGold995_AM")
        g_995_pm = self._extract_span_int(soup, "lblGold995_PM")
        g_916_am = self._extract_span_int(soup, "lblGold916_AM")
        g_916_pm = self._extract_span_int(soup, "lblGold916_PM")
        g_750_am = self._extract_span_int(soup, "lblGold750_AM")
        g_750_pm = self._extract_span_int(soup, "lblGold750_PM")
        g_585_am = self._extract_span_int(soup, "lblGold585_AM")
        g_585_pm = self._extract_span_int(soup, "lblGold585_PM")
        s_999_am = self._extract_span_int(soup, "lblSilver999_AM")
        s_999_pm = self._extract_span_int(soup, "lblSilver999_PM")

        # Average + 3% GST — matches GOLD_SYSTEM_PROMPT formula
        # exactly (avg_price = round((am + pm) / 2); carat_N =
        # int(avg_price * 1.03)). Phase 3 cleanup hoists this to a
        # shared helper once the AI path is removed.
        carat_24 = int(round((g_999_am + g_999_pm) / 2) * _GST_MULTIPLIER)
        carat_22 = int(round((g_916_am + g_916_pm) / 2) * _GST_MULTIPLIER)
        carat_18 = int(round((g_750_am + g_750_pm) / 2) * _GST_MULTIPLIER)

        # Date format matches the AI path's GOLD_SYSTEM_PROMPT
        # directive ("Date format: DD-Mon-YY"). IBJA's HTML doesn't
        # expose an explicit date for today's rates; use today's date
        # per the "If today's rates are not available yet, use the
        # most recent available day" fallback — scrape always sees
        # the most recent, so today's calendar date is correct.
        today = datetime.now().strftime("%d-%b-%y")

        return {
            "carat_24": carat_24,
            "carat_22": carat_22,
            "carat_18": carat_18,
            "ibja_data": {
                "date": today,
                "gold": {
                    "999": {"am": g_999_am, "pm": g_999_pm},
                    "995": {"am": g_995_am, "pm": g_995_pm},
                    "916": {"am": g_916_am, "pm": g_916_pm},
                    "750": {"am": g_750_am, "pm": g_750_pm},
                    "585": {"am": g_585_am, "pm": g_585_pm},
                },
                "silver": {"999": {"am": s_999_am, "pm": s_999_pm}},
                "currency": "INR",
                "source": "ibjarates.com (direct scrape, ak-ad1)",
            },
        }

    @staticmethod
    def _extract_span_int(soup, span_id):
        """Extract an integer price from a ``<span id="...">N</span>``.

        Raises ValueError with a specific message identifying which
        selector failed, so operators triaging a PARSE-FAILED cycle see
        WHICH span drifted (not just "parse failed"). Empty text /
        non-integer text both raise.
        """
        span = soup.find("span", id=span_id)
        if span is None:
            raise ValueError(f"selector miss: span#{span_id} not in HTML")
        text = span.get_text(strip=True)
        if not text:
            raise ValueError(f"selector empty: span#{span_id}")
        try:
            return int(text)
        except ValueError:
            raise ValueError(
                f"selector non-int: span#{span_id} = {text!r}"
            )
