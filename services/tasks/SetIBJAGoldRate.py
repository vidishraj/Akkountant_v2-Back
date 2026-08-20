from services.tasks.AIRateTask import AIRateTask
from utils.logger import Logger


# Upstream URL for IBJA daily gold/silver rates.
# IBJA publishes the rate table at this page; sonnet extracts the AM/PM
# prices per purity from the HTML.
GOLD_URL = "https://ibjarates.com/"

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
        (LLM extract either succeeds or fails). Failure paths preserve
        the pre-refactor error messages via extras['error'].

        v4 (hq-wisp-il849): reverted from the pinned-sonnet diag probe
        back to the default bare "sonnet" alias. The pinned probe was a
        diagnostic to isolate alias-flip from a deeper sonnet issue;
        actual root cause turned out to be the invalid property keys
        in GOLD_SCHEMA (see schema comment above), not the model
        string. Bare alias = no per-call override = `fetch_and_extract`
        default ("sonnet").
        """
        jsonData, ai_err = self.fetch_rates_via_ai(
            url=GOLD_URL,
            system_prompt=GOLD_SYSTEM_PROMPT,
            schema=GOLD_SCHEMA,
            agent="rate.gold",
        )
        if jsonData is None:
            msg = f"Failed to get Gold Rates: {ai_err}"[:800]
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
        for k_schema, k_display in _GOLD_DISPLAY_KEYS:
            if k_schema in jsonData:
                jsonData[k_display] = jsonData.pop(k_schema)

        return jsonData, 1, 1, {}, {}
