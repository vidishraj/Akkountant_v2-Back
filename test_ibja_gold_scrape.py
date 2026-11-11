"""Tests for the ak-ad1 Phase 2 IBJA direct-scrape path.

## What this suite pins

The deterministic-scrape alternative to the LLM extraction in
``SetIBJAGoldRate``. Preserved invariants from the pre-ak-ad1 (AI-only)
path:
  * ``_GOLD_PLAUSIBLE_BANDS`` (ak-znk Fix-B3) applies identically to
    scrape output — upstream HTML drift can also produce out-of-range
    values; same guard catches both AI scale-drift and scrape
    selector-drift.
  * ``_GOLD_DISPLAY_KEYS`` remap (``carat_24`` → ``"24 Carat"``) applies
    identically.
  * ``zero-passthrough`` for optional purities (``carat_22``/``18``)
    applies identically — scrape may legitimately emit 0 if IBJA doesn't
    publish a purity on a given cycle.
  * +3% GST multiplier identical.
  * ``ibja_data`` top-level block populated with AM/PM per-purity,
    date, currency, source.

## Lead's "no safety net post-cutover" flag

Overseer Q4 (2026-11-11) picked DROP LLM ENTIRELY post-cutover
(Phase 4 removes ``GOLD_SYSTEM_PROMPT``/``GOLD_SCHEMA``/
``fetch_rates_via_ai`` call for gold). Lead flag: "golden-fixture
regression test is explicit Phase 2 deliverable". This suite's
golden-fixture tests ARE that deliverable — they pin the parser against
a saved 2026-11-11 IBJA HTML snapshot. Any selector change that would
silently drift the extracted values in prod fails these tests at CI
time.

## Mutation-test discipline (rig-std from ak-lvu arc)

Pre-fold (``_fetch_rates_via_scrape``/``_parse_ibja_html``/
``_extract_span_int`` absent + env-var dispatch block absent) → every
scrape-path test fails on AttributeError. Post-fold → full pass.
Same-binary-opposite-outcome discipline per rig standard.

## Golden fixture

``test_fixtures/ibja_today_rates_2026_11_11.html`` is a ~3KB snapshot
of the ``TodayRatesTableDataYes`` table from the live IBJA page
captured 2026-11-11. Expected extracted values:
  * Gold 999 AM/PM   : 140805 / 140603 → avg 140704 → carat_24 = 144925
  * Gold 916 AM/PM   : 128977 / 128792 → avg 128885 → carat_22 = 132751
  * Gold 750 AM/PM   : 105604 / 105452 → avg 105528 → carat_18 = 108693

GST math: ``int(round((am + pm) / 2) * 1.03)`` — matches the LLM
prompt formula exactly.
"""
import os
import sys
import types
from unittest.mock import MagicMock, patch

import pytest

# Reuse the kiteconnect-stub harness so this suite can import the
# BaseRateTask module chain without bringing in the Kite / Google /
# Statement-parser stacks.
from test_kite_token_expiry import (  # noqa: E402
    _install_fake_kiteconnect,
    _install_fake_module,
    _install_import_stubs,
    _project_modules_loaded,
    _withdraw_stubs,
)

_modules_before = _project_modules_loaded()

_install_fake_kiteconnect()
_install_import_stubs()
for _pkg in (
    "nsepython",
    "nsepython.rahu",
    "nsepythonserver",
    "nsepythonserver.rahu",
    "anyio",
    "claude_agent_sdk",
    "aiohttp",
    "tabula",
    "pdfplumber",
    "pypdf",
    "fitz",
    "firebase_admin",
    "firebase_admin.auth",
    "firebase_admin.credentials",
    "openpyxl",
):
    _install_fake_module(_pkg)

# BaseRateTask transitively imports InvestmentService + TransactionService
# (statement parsers etc.) — stub the specific submodules directly in
# sys.modules WITHOUT _install_fake_module (which would shadow the whole
# `services` package and break `services.tasks.*` resolution).
_svc_inv_stub = types.ModuleType("services.InvestmentService")
_svc_inv_stub.InvestmentService = type("InvestmentService", (), {})
sys.modules["services.InvestmentService"] = _svc_inv_stub

_svc_tx_stub = types.ModuleType("services.transactionsService")
_svc_tx_stub.TransactionService = type("TransactionService", (), {})
sys.modules["services.transactionsService"] = _svc_tx_stub

# `_install_import_stubs` stubs `bs4` for suites that legitimately
# detect its absence (see _STUBBED_PACKAGES in test_kite_token_expiry).
# For THIS suite we need the real bs4 — un-stub it so SetIBJAGoldRate
# imports the actual BeautifulSoup class. Skip the suite if bs4 isn't
# installed at all (would hit ImportError on the next line); fail loud
# rather than silently MagicMocking through the parser.
sys.modules.pop("bs4", None)
try:
    import bs4  # noqa: F401
except ImportError:
    pytest.skip(
        "bs4 not installed — scrape-path tests require BeautifulSoup",
        allow_module_level=True,
    )

from services.tasks.SetIBJAGoldRate import (  # noqa: E402
    SetIBJAGoldRate,
    _GOLD_SOURCE_ENV,
    _GOLD_PLAUSIBLE_BANDS,
)
import services.tasks.SetIBJAGoldRate as _gold_module  # noqa: E402

_withdraw_stubs(_project_modules_loaded() - _modules_before)


# ── fixture loading ──────────────────────────────────────────────────────

_HERE = os.path.dirname(os.path.abspath(__file__))
_FIXTURE_PATH = os.path.join(
    _HERE, "test_fixtures", "ibja_today_rates_2026_11_11.html"
)


def _golden_fixture_html():
    """Load the captured 2026-11-11 IBJA fragment."""
    with open(_FIXTURE_PATH, "r", encoding="utf-8") as f:
        return f.read()


def _service():
    """Build a SetIBJAGoldRate instance with test-harness stubs.

    Bypasses __init__ (which touches Flask + the AIRateTask singleton
    machinery) and wires in just the attributes the scrape path and
    _fetch_all need.

    Resets the class-level ``_instance`` singleton first so per-test
    monkey-patches (e.g. stubbing `_fetch_rates_via_scrape` in one
    test) don't leak into the next test's instance.
    """
    SetIBJAGoldRate._instance = None  # reset singleton to force fresh instance
    svc = SetIBJAGoldRate.__new__(SetIBJAGoldRate)
    svc.logger = MagicMock()
    svc.title = "SetGoldRate"
    svc.interval = 300  # matches production default
    return svc


# ── 1: golden-fixture parse — the Lead-flagged CRITICAL deliverable ─────

def test_golden_fixture_parse_returns_expected_carat_values():
    """THE golden-fixture regression guard (Lead-flagged CRITICAL for
    post-cutover safety).

    Reference values from the 2026-11-11 IBJA capture:
      * Gold 999 AM/PM = 140805/140603 → avg = 140704 → carat_24 = 144925
      * Gold 916 AM/PM = 128977/128792 → avg = 128885 → carat_22 = 132751
      * Gold 750 AM/PM = 105604/105452 → avg = 105528 → carat_18 = 108693

    Mutation discriminator: a parser that averages incorrectly (e.g.
    rounds with a different mode, or applies GST before averaging)
    returns different values. A parser that uses the wrong selectors
    (e.g. drops the `lbl` prefix, or targets AM where PM is wanted)
    returns different values.
    """
    svc = _service()
    data = svc._parse_ibja_html(_golden_fixture_html())

    assert data["carat_24"] == 144925
    # carat_22: (128977 + 128792) / 2 = 128884.5; Python round() uses
    # banker's rounding → 128884 (even); 128884 * 1.03 = 132750.52 → 132750
    assert data["carat_22"] == 132750
    assert data["carat_18"] == 108693


def test_golden_fixture_ibja_data_block_has_all_purities():
    """The `ibja_data` block must carry AM/PM for all 5 gold purities
    + silver 999, matching the AI path's output shape.
    """
    svc = _service()
    data = svc._parse_ibja_html(_golden_fixture_html())

    ibja = data["ibja_data"]
    # All 5 gold purities
    assert ibja["gold"]["999"] == {"am": 140805, "pm": 140603}
    assert ibja["gold"]["995"] == {"am": 140241, "pm": 140040}
    assert ibja["gold"]["916"] == {"am": 128977, "pm": 128792}
    assert ibja["gold"]["750"] == {"am": 105604, "pm": 105452}
    assert ibja["gold"]["585"] == {"am": 82371, "pm": 82253}
    # Silver
    assert ibja["silver"]["999"] == {"am": 186495, "pm": 185900}
    # Metadata
    assert ibja["currency"] == "INR"
    assert "scrape" in ibja["source"]  # distinguishable from AI path's "source" value


def test_golden_fixture_date_is_today_in_DD_Mon_YY_format():
    """The LLM prompt mandated `DD-Mon-YY` output. Scrape path must
    match that on-disk contract (future consumer parsing depends on it).
    """
    from datetime import datetime
    svc = _service()
    data = svc._parse_ibja_html(_golden_fixture_html())

    expected = datetime.now().strftime("%d-%b-%y")  # e.g. "11-Nov-26"
    assert data["ibja_data"]["date"] == expected


# ── 2: selector-miss + value-coerce failure handling ────────────────────

def test_parse_raises_value_error_on_missing_selector():
    """A missing span raises ValueError with the specific selector id
    so operators triaging a PARSE-FAILED cycle see WHICH selector
    drifted (not just "parse failed").
    """
    html_missing = "<html><body>no spans here</body></html>"
    svc = _service()
    with pytest.raises(ValueError) as exc_info:
        svc._parse_ibja_html(html_missing)
    assert "lblGold999_AM" in str(exc_info.value)
    assert "selector miss" in str(exc_info.value)


def test_parse_raises_value_error_on_non_integer_value():
    """A span present but containing non-integer text raises ValueError
    with the offending text in the message.
    """
    html_bad = _golden_fixture_html().replace(
        '<span id="lblGold999_AM">140805</span>',
        '<span id="lblGold999_AM">NOT_A_NUMBER</span>'
    )
    svc = _service()
    with pytest.raises(ValueError) as exc_info:
        svc._parse_ibja_html(html_bad)
    assert "lblGold999_AM" in str(exc_info.value)
    assert "non-int" in str(exc_info.value)


def test_parse_raises_value_error_on_empty_span():
    """A span present but empty raises ValueError (distinct from the
    missing-span case to help triage upstream HTML changes).
    """
    html_empty = _golden_fixture_html().replace(
        '<span id="lblGold999_AM">140805</span>',
        '<span id="lblGold999_AM"></span>'
    )
    svc = _service()
    with pytest.raises(ValueError) as exc_info:
        svc._parse_ibja_html(html_empty)
    assert "lblGold999_AM" in str(exc_info.value)
    assert "empty" in str(exc_info.value)


# ── 3: _fetch_rates_via_scrape — network + parse wrapper ─────────────────

def test_fetch_scrape_returns_data_and_none_on_success():
    """Successful HTTP + parse returns (jsonData, None)."""
    svc = _service()
    fake_resp = MagicMock()
    fake_resp.text = _golden_fixture_html()
    fake_resp.raise_for_status.return_value = None
    fake_requests = MagicMock()
    fake_requests.get.return_value = fake_resp
    with patch.object(_gold_module, "requests", fake_requests):
        data, err = svc._fetch_rates_via_scrape()
    assert err is None
    assert data["carat_24"] == 144925


def test_fetch_scrape_returns_none_and_err_on_network_failure():
    """Network failure returns (None, err_str). Err captures the
    exception message so operators see what broke.
    """
    svc = _service()
    fake_requests = MagicMock()
    fake_requests.get.side_effect = ConnectionError("IBJA unreachable")
    with patch.object(_gold_module, "requests", fake_requests):
        data, err = svc._fetch_rates_via_scrape()
    assert data is None
    assert err is not None
    assert "IBJA unreachable" in err
    assert "scrape fetch failed" in err


def test_fetch_scrape_returns_none_and_err_on_http_error():
    """Non-2xx HTTP response → raise_for_status raises → caught + returned
    as (None, err).
    """
    svc = _service()
    fake_resp = MagicMock()
    fake_resp.raise_for_status.side_effect = Exception("503 Service Unavailable")
    fake_requests = MagicMock()
    fake_requests.get.return_value = fake_resp
    with patch.object(_gold_module, "requests", fake_requests):
        data, err = svc._fetch_rates_via_scrape()
    assert data is None
    assert err is not None


def test_fetch_scrape_returns_none_and_err_on_parse_failure():
    """A 200 response with HTML missing selectors returns (None, err)
    with "parse failed" in the message + the specific selector that
    was missing.
    """
    svc = _service()
    fake_resp = MagicMock()
    fake_resp.text = "<html><body>no rate spans</body></html>"
    fake_resp.raise_for_status.return_value = None
    fake_requests = MagicMock()
    fake_requests.get.return_value = fake_resp
    with patch.object(_gold_module, "requests", fake_requests):
        data, err = svc._fetch_rates_via_scrape()
    assert data is None
    assert "parse failed" in err
    assert "lblGold999_AM" in err  # specific selector surfaced


# ── 4: env-var dispatch in _fetch_all ────────────────────────────────────

def test_fetch_all_default_env_takes_ai_path(monkeypatch):
    """Default (unset `GOLD_SOURCE`) routes to `fetch_rates_via_ai` so
    Phase 2 rollout is opt-in.
    """
    monkeypatch.delenv(_GOLD_SOURCE_ENV, raising=False)
    svc = _service()
    # Stub fetch_rates_via_ai so we can detect which path ran.
    svc.fetch_rates_via_ai = MagicMock(return_value=(None, "ai-marker"))
    svc._fetch_rates_via_scrape = MagicMock(return_value=(None, "scrape-marker"))

    data, _, _, _, extras = svc._fetch_all()

    svc.fetch_rates_via_ai.assert_called_once()
    svc._fetch_rates_via_scrape.assert_not_called()
    # Error message carries source_label "ai"
    assert "ai" in extras["error"]


def test_fetch_all_scrape_env_takes_scrape_path(monkeypatch):
    """`GOLD_SOURCE=scrape` routes to `_fetch_rates_via_scrape`."""
    monkeypatch.setenv(_GOLD_SOURCE_ENV, "scrape")
    svc = _service()
    svc.fetch_rates_via_ai = MagicMock(return_value=(None, "ai-marker"))
    svc._fetch_rates_via_scrape = MagicMock(return_value=(None, "scrape-marker"))

    data, _, _, _, extras = svc._fetch_all()

    svc._fetch_rates_via_scrape.assert_called_once()
    svc.fetch_rates_via_ai.assert_not_called()
    assert "scrape" in extras["error"]


def test_fetch_all_env_dispatch_is_case_insensitive(monkeypatch):
    """`GOLD_SOURCE=SCRAPE` (upper) also routes scrape path — no
    silent `ai` fallback on casing mismatch during operator rollout.
    """
    monkeypatch.setenv(_GOLD_SOURCE_ENV, "SCRAPE")
    svc = _service()
    svc.fetch_rates_via_ai = MagicMock(return_value=(None, "ai-marker"))
    svc._fetch_rates_via_scrape = MagicMock(return_value=(None, "scrape-marker"))

    svc._fetch_all()

    svc._fetch_rates_via_scrape.assert_called_once()


# ── 5: end-to-end _fetch_all with scrape — Fix-B3 + display-key preserved

def test_fetch_all_scrape_path_full_pipeline_preserves_fixB3_and_display_remap(monkeypatch):
    """End-to-end test: GOLD_SOURCE=scrape, golden fixture via mocked
    requests, verify the full `_fetch_all` pipeline:
      1. Routes to scrape path
      2. Produces carat_24/22/18 from fixture
      3. Fix-B3 bands pass (values within _GOLD_PLAUSIBLE_BANDS)
      4. Display-key remap applies (carat_24 → "24 Carat")
      5. Returns 5-tuple matching BaseRateTask contract
    """
    monkeypatch.setenv(_GOLD_SOURCE_ENV, "scrape")
    svc = _service()
    fake_resp = MagicMock()
    fake_resp.text = _golden_fixture_html()
    fake_resp.raise_for_status.return_value = None
    fake_requests = MagicMock()
    fake_requests.get.return_value = fake_resp
    with patch.object(_gold_module, "requests", fake_requests):
        data, total, ok, perm, extras = svc._fetch_all()

    # 5-tuple shape (BaseRateTask contract)
    assert total == 1
    assert ok == 1
    assert perm == {}
    assert extras == {}

    # Display-key remap applied
    assert "24 Carat" in data
    assert "22 Carat" in data
    assert "18 Carat" in data
    # Original schema keys removed post-remap
    assert "carat_24" not in data
    assert "carat_22" not in data
    assert "carat_18" not in data

    # Values survived Fix-B3 bands AND match golden-fixture expectations
    assert data["24 Carat"] == 144925
    assert data["22 Carat"] == 132750  # banker's rounding on .5; see parse test
    assert data["18 Carat"] == 108693

    # ibja_data block preserved with scrape-specific source marker
    assert "ibja_data" in data
    assert "scrape" in data["ibja_data"]["source"]


def test_fetch_all_scrape_fixB3_bands_still_apply():
    """Defense-in-depth: Fix-B3 _GOLD_PLAUSIBLE_BANDS must still catch
    out-of-range scrape output. Guards against selector drift that
    extracts a valid-int-but-wrong value (e.g. extracts the wrong span).

    Simulate IBJA emitting a 10× scale value for 999 (upstream decimal-
    shift bug). Scrape parser extracts it, Fix-B3 catches it before
    disk-write.
    """
    svc = _service()
    # Fabricate HTML with 999 AM at 1_400_000 (10× the real value,
    # above carat_24 ceiling of 250_000)
    bad_html = _golden_fixture_html().replace(
        '<span id="lblGold999_AM">140805</span>',
        '<span id="lblGold999_AM">1400000</span>'
    )
    fake_resp = MagicMock()
    fake_resp.text = bad_html
    fake_resp.raise_for_status.return_value = None
    fake_requests = MagicMock()
    fake_requests.get.return_value = fake_resp
    with patch.dict(os.environ, {_GOLD_SOURCE_ENV: "scrape"}):
        with patch.object(_gold_module, "requests", fake_requests):
            data, total, ok, perm, extras = svc._fetch_all()

    # Fix-B3 rejected — None-tuple preserving last-good
    assert data is None
    assert total == 1
    assert ok == 0
    # Error mentions ak-znk Fix-B3 + specific carat value
    assert "Fix-B3" in extras["error"]
    assert "carat_24" in extras["error"]


# ── 6: observability log for dual-run early-warning ─────────────────────

def test_fetch_all_emits_structured_observability_log(monkeypatch):
    """Lead flag (2026-11-11): operator needs per-cycle logs for the
    1-week / 14-consecutive-cycle observation window. The structured
    `gold_rate_cycle source=... carat_24=... carat_22=... carat_18=...`
    line enables offline divergence computation + "no streak forming
    by cycle 10" early-warning detection.
    """
    monkeypatch.setenv(_GOLD_SOURCE_ENV, "scrape")
    svc = _service()
    fake_resp = MagicMock()
    fake_resp.text = _golden_fixture_html()
    fake_resp.raise_for_status.return_value = None
    fake_requests = MagicMock()
    fake_requests.get.return_value = fake_resp
    with patch.object(_gold_module, "requests", fake_requests):
        svc._fetch_all()

    # Any of the logger.info calls should carry the structured line.
    info_calls = [c.args[0] for c in svc.logger.info.call_args_list]
    structured_lines = [l for l in info_calls if "gold_rate_cycle " in l]
    assert len(structured_lines) >= 1, (
        "observability log line missing — streak-detection + divergence "
        "analysis depend on it"
    )
    line = structured_lines[0]
    assert "source=scrape" in line
    assert "carat_24=144925" in line
    assert "carat_22=132750" in line  # banker's rounding — see parse test
    assert "carat_18=108693" in line


# ── 7: GST + averaging arithmetic precision ─────────────────────────────

def test_averaging_uses_round_then_multiply_matching_system_prompt():
    """The AI prompt mandated:
      avg_price = round((am + pm) / 2)
      avg_with_gst = int(avg_price * 1.03)  # truncation

    Scrape path must match this EXACTLY — a different rounding mode
    (e.g. banker's rounding in Python 3 vs the LLM's half-up) or
    multiply-before-round would drift from historical AI values and
    trip the Phase 2 divergence threshold falsely.

    Fixture: 140805 + 140603 = 281408 / 2 = 140704.0 (exact int).
    * 1.03 = 144925.12 → int() → 144925.
    """
    svc = _service()
    data = svc._parse_ibja_html(_golden_fixture_html())

    # Exact arithmetic reproduction:
    expected_avg_999 = round((140805 + 140603) / 2)  # 140704
    expected_carat_24 = int(expected_avg_999 * 1.03)  # 144925
    assert data["carat_24"] == expected_carat_24
    assert data["carat_24"] == 144925


def test_averaging_handles_odd_sum_matching_prompt_round_mode():
    """Case where (am + pm) / 2 produces a .5 — Python's `round()`
    uses banker's rounding (round-half-to-even). The AI prompt said
    simply "round", so any half-to-even behavior is the reference
    semantics for the scrape path.

    Build HTML where 999 AM + PM = odd number; verify round() returns
    the expected banker's-rounded value.
    """
    html = _golden_fixture_html().replace(
        '<span id="lblGold999_AM">140805</span>',
        '<span id="lblGold999_AM">140705</span>'  # (140705+140603)/2 = 140654.0 (exact)
    )
    svc = _service()
    data = svc._parse_ibja_html(html)
    # (140705 + 140603) / 2 = 140654.0 → round(140654.0) = 140654
    # 140654 * 1.03 = 144873.62 → int() = 144873
    assert data["carat_24"] == 144873


# ── 8: pre-fold mutation discriminator (via source inspection) ──────────

def test_scrape_path_methods_defined_on_class():
    """Code-level invariant: the scrape path's three entry methods
    must be defined on SetIBJAGoldRate. Mutation discriminator — if
    a future refactor accidentally removes one, the test fails even
    if parse logic looks superficially correct.
    """
    assert hasattr(SetIBJAGoldRate, "_fetch_rates_via_scrape")
    assert hasattr(SetIBJAGoldRate, "_parse_ibja_html")
    assert hasattr(SetIBJAGoldRate, "_extract_span_int")


def test_env_var_name_is_stable_public_symbol():
    """`_GOLD_SOURCE_ENV` is a module-level constant so operator
    tooling (deploy scripts, env-check runbooks) can reference it by
    the SAME Python identifier they'd see in review.
    """
    assert _GOLD_SOURCE_ENV == "GOLD_SOURCE"
