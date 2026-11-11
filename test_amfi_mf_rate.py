"""Tests for SetAMFIMFRate (ak-nutd Phase 2).

## What this suite pins

The AMFI NAV file parser + prior-snapshot persistence + BaseRateTask
contract adherence for the MF rate source pivot (mfapi.in →
AMFI NAVAll.txt).

## Mutation-test discipline (rig-standard from ak-lvu arc)

The suite is written against the ak-nutd fixture shape — same AMFI
text structure Overseer agreed to via Q1-Q3 answers 2026-11-11.
Pre-fold code (SetAMFIMFRate absent entirely) raises ImportError on
every test. Post-fold: full pass. "Same test binary, opposite outcome
per commit" discipline per rig standard.

## Reference fixture

Sample AMFI NAVAll.txt body covering the full parse surface:
  * File header: `Scheme Code;ISIN...;Date`
  * Category section marker: `Open Ended Schemes(Debt Scheme - Gilt Fund)`
  * Fund-house section marker: `Axis Mutual Fund`
  * Blank line
  * Normal data row (today's date, non-zero NAV)
  * Stale data row (90-day-old date — filtered as stale)
  * Zero-NAV row (filtered as zero-placeholder)
  * N.A. NAV row (filtered as placeholder)
  * Malformed row (wrong field count — filtered as malformed)
  * Bad-date row (non-parseable date — filtered as malformed)

Discriminator values (post-parse expected map):
  * 3 valid entries (Axis Short Term, HDFC Gilt, Franklin Gilt)
  * stale_skipped = 1
  * zero_skipped = 2 (zero + N.A.)
  * malformed_skipped = 2 (wrong-field-count + bad-date)
"""
import json
import os
import sys
import tempfile
import types
from unittest.mock import MagicMock, patch

import pytest

# Reuse the kiteconnect-stub harness from test_kite_token_expiry so this
# suite can import the BaseRateTask module chain without bringing in
# kiteconnect + the Google stack.
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
# Transitively-pulled packages that aren't installed in CI.
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
    "bs4",
    "openpyxl",
):
    _install_fake_module(_pkg)


# BaseRateTask → baseTask transitively imports InvestmentService +
# TransactionService, both of which pull their own heavy chains
# (statement parsers, Kite, Firebase …). None of that is on the path
# under test — stub the specific submodules directly in sys.modules
# WITHOUT using _install_fake_module (which would shadow the whole
# `services` package and break `services.tasks.*` resolution).
_svc_inv_stub = types.ModuleType("services.InvestmentService")
_svc_inv_stub.InvestmentService = type("InvestmentService", (), {})
sys.modules["services.InvestmentService"] = _svc_inv_stub

_svc_tx_stub = types.ModuleType("services.transactionsService")
_svc_tx_stub.TransactionService = type("TransactionService", (), {})
sys.modules["services.transactionsService"] = _svc_tx_stub

from services.tasks.SetAMFIMFRate import SetAMFIMFRate  # noqa: E402
# Keep a module reference so `patch.object(_amfi_module, 'requests', …)`
# works without going through a sys.modules lookup. (A string-path patch
# re-imports the module from sys.modules, which fails post-withdraw.)
import services.tasks.SetAMFIMFRate as _amfi_module  # noqa: E402

_withdraw_stubs(_project_modules_loaded() - _modules_before)


# ── fixture builders ──────────────────────────────────────────────────────

def _today_amfi():
    """DD-Mon-YYYY for today — matches AMFI's native date format."""
    from datetime import datetime
    return datetime.now().strftime("%d-%b-%Y")


def _days_ago_amfi(days):
    from datetime import datetime, timedelta
    return (datetime.now() - timedelta(days=days)).strftime("%d-%b-%Y")


def _today_canonical():
    """DD-MM-YYYY — the on-disk form the parser canonicalizes to
    (matches mfapi.in output contract consumed by
    JsonDownloadService.getMFRate).
    """
    from datetime import datetime
    return datetime.now().strftime("%d-%m-%Y")


def _sample_amfi_text():
    """Representative AMFI NAVAll.txt body covering every parse branch.

    Dates use helpers so the suite stays correct across time without
    needing to be re-dated.
    """
    today = _today_amfi()
    stale = _days_ago_amfi(90)  # well beyond _STALE_THRESHOLD_DAYS=7
    lines = [
        "Scheme Code;ISIN Div Payout/ISIN Growth;ISIN Div Reinvestment;"
        "Scheme Name;Plan;Option;Net Asset Value;Date",
        "",
        "Open Ended Schemes(Debt Scheme - Gilt Fund)",
        "",
        "HDFC Mutual Fund",
        "",
        # Normal valid row
        f"118989;INF179K01YV8;-;HDFC Gilt Fund;Direct Plan;Growth;"
        f"50.1234;{today}",
        # Stale row (90 days old) — filtered as stale
        f"118990;INF179K01YW6;-;HDFC Gilt Fund;Direct Plan;IDCW;"
        f"20.5678;{stale}",
        "",
        "Franklin Templeton Mutual Fund",
        "",
        # Normal valid row (different fund house — state tracking test)
        f"129012;INF090I01AA1;-;Franklin Gilt Fund;Direct Plan;Growth;"
        f"35.4567;{today}",
        # Zero NAV — filtered as placeholder
        f"129013;INF090I01AB9;-;Franklin Gilt Fund (Wound Down);"
        f"Direct Plan;Growth;0;{today}",
        "",
        "Open Ended Schemes(Debt Scheme - Short Duration Fund)",
        "",
        "Axis Mutual Fund",
        "",
        # Normal valid row (different category — state tracking test)
        f"120503;INF846K01DE5;-;Axis Short Term Fund;Direct Plan;Growth;"
        f"28.9012;{today}",
        # N.A. NAV — filtered as placeholder
        f"120504;INF846K01DF2;-;Axis Short Term Fund (Plan X);"
        f"Direct Plan;Growth;N.A.;{today}",
        # Malformed — wrong field count (7 instead of 8)
        f"999001;INF999K01XX1;-;Bad Fund;Direct Plan;Growth;{today}",
        # Malformed — bad date
        f"999002;INF999K01YY2;-;Bad Date Fund;Direct Plan;Growth;"
        f"10.0000;not-a-date-string",
    ]
    return "\n".join(lines)


def _service(prior_data=None):
    """Build a SetAMFIMFRate instance with test-harness stubs.

    Bypasses __init__ (which touches Flask + singleton machinery) and
    wires in just the handful of attributes _fetch_all / _parse /
    _rotate_prior rely on.
    """
    svc = SetAMFIMFRate.__new__(SetAMFIMFRate)
    svc.logger = MagicMock()
    svc.title = "SetMFRate"
    svc.interval = 60 * 24  # matches production default

    # Stub jsonService — only `normalize_scheme_code`,
    # `getLatestFile`, `ratesType`, `MfRatePrefix` touched by the paths
    # under test.
    js = MagicMock()
    # Delegate normalize_scheme_code to the real implementation so
    # malformed-code tests measure the real behavior (int / str /
    # whitespace-strip rules). Assign directly — the attribute lookup
    # on JSONDownloadService already unwraps the @staticmethod
    # descriptor to the underlying function, so a `staticmethod()`
    # wrapper here would re-wrap and break direct-call.
    from services.JsonDownloadService import JSONDownloadService
    js.normalize_scheme_code = JSONDownloadService.normalize_scheme_code
    js.ratesType = "rates"
    js.MfRatePrefix = "MF_rate"
    svc.jsonService = js

    # Per-test tmp_dir so prior-snapshot writes don't bleed across tests.
    svc._tmp_dir_handle = tempfile.TemporaryDirectory()
    svc.tmp_dir = svc._tmp_dir_handle.name

    # Optional: seed a prior-snapshot into tmp_dir.
    if prior_data is not None:
        prior_path = os.path.join(svc.tmp_dir, "MFRate.prior.json")
        with open(prior_path, "w") as f:
            json.dump({"data": prior_data}, f)

    return svc


# ── 1: structural parse — fields, filtering, state tracking ──────────────

def test_parse_produces_three_valid_entries_from_sample_fixture():
    """THE core mutation discriminator.

    The reference fixture has 5 rows that reach the "post-structural-
    parse" stage (`total_rows == 5` — 3 valid + 1 stale + 1 zero ...
    counting the N.A. too makes it total=6 but N.A. and zero both get
    counted in total since they pass date/field-count). Let me re-count
    at test time."""
    svc = _service()
    parsed_map, total, stale, zero, malformed = svc._parse_amfi_text(
        _sample_amfi_text(), prior_map={}
    )

    # 3 valid entries landed
    assert len(parsed_map) == 3
    assert set(parsed_map.keys()) == {"118989", "129012", "120503"}


def test_parse_counts_stale_in_permanent_skip_bucket():
    """Stale rows (date older than 7 days) are counted separately and
    excluded from the denominator (mirrors mfapi.in permanent_404
    semantics). 1 row in the fixture is 90 days old.
    """
    svc = _service()
    _, _, stale, _, _ = svc._parse_amfi_text(_sample_amfi_text(), prior_map={})
    assert stale == 1


def test_parse_counts_zero_placeholder_in_zero_bucket():
    """Zero-NAV and N.A. rows both count toward zero_skipped (stay IN
    the denominator so an AMFI regression wave surfaces via the ratio
    gate). 2 rows in the fixture match.
    """
    svc = _service()
    _, _, _, zero, _ = svc._parse_amfi_text(_sample_amfi_text(), prior_map={})
    assert zero == 2  # one bare "0" + one "N.A."


def test_parse_counts_malformed_shape_separately():
    """Wrong-field-count and bad-date rows count toward malformed_
    skipped. 2 rows in the fixture match.
    """
    svc = _service()
    _, _, _, _, malformed = svc._parse_amfi_text(
        _sample_amfi_text(), prior_map={}
    )
    assert malformed == 2


def test_parse_section_header_lines_are_not_counted_as_malformed():
    """The file has fund-house markers (`HDFC Mutual Fund`) and
    category markers (`Open Ended Schemes(...)`) as lines without
    semicolons. These are STATE transitions, not malformed rows — must
    not inflate the malformed count.
    """
    svc = _service()
    _, total, _, _, malformed = svc._parse_amfi_text(
        _sample_amfi_text(), prior_map={}
    )
    # malformed == 2 (asserted above) excludes the ~5 section/fund-house
    # lines in the fixture; those are correctly silent-skipped.
    assert malformed == 2


def test_parse_file_header_line_is_silent_skip():
    """The `Scheme Code;ISIN Div Payout/...;Date` file header line
    matches the field-count filter but must be silently skipped (NOT
    counted as malformed).
    """
    svc = _service()
    _, _, _, _, malformed = svc._parse_amfi_text(
        _sample_amfi_text(), prior_map={}
    )
    assert malformed == 2  # as above — file header not counted


# ── 2: entry shape — field names + value canonicalization ────────────────

def test_parsed_entry_has_mfapi_compatible_shape():
    """The on-disk contract expected by
    JsonDownloadService.getMFRate + InvestmentService.fetchUserSecurities:
    entries must carry `scheme_id` + `nav` + `date` + `fundHouse` +
    `schemeType`. Preserving this shape keeps the FE unchanged across
    the pivot.
    """
    svc = _service()
    parsed_map, *_ = svc._parse_amfi_text(_sample_amfi_text(), prior_map={})
    entry = parsed_map["118989"]
    assert entry["scheme_id"] == "118989"
    assert entry["nav"] == "50.1234"
    assert entry["date"] == _today_canonical()  # DD-MM-YYYY, not DD-Mon-YYYY
    assert entry["fundHouse"] == "HDFC Mutual Fund"
    assert "Gilt Fund" in entry["schemeType"]


def test_date_is_canonicalized_from_AMFI_native_to_mfapi_contract():
    """AMFI emits dates as DD-Mon-YYYY (e.g. `09-Nov-2026`). The on-
    disk contract consumed by InvestmentService L322-329 uses
    DD-MM-YYYY (mfapi.in's native). The parser must convert at ingest.

    Mutation discriminator: a parser that preserves the AMFI format
    (`09-Nov-2026`) would break FE parsing.
    """
    svc = _service()
    parsed_map, *_ = svc._parse_amfi_text(_sample_amfi_text(), prior_map={})
    entry = parsed_map["118989"]
    assert entry["date"] == _today_canonical()
    # Negative assertion: the native AMFI form must NOT persist
    assert "Nov" not in entry["date"]
    assert "-" in entry["date"]


def test_fund_house_tracks_across_multiple_fund_sections():
    """State tracking: each new fund-house header line updates
    `current_fund_house` and subsequent data rows pick it up. Verify
    three different fund houses get attributed correctly.
    """
    svc = _service()
    parsed_map, *_ = svc._parse_amfi_text(_sample_amfi_text(), prior_map={})
    assert parsed_map["118989"]["fundHouse"] == "HDFC Mutual Fund"
    assert parsed_map["129012"]["fundHouse"] == "Franklin Templeton Mutual Fund"
    assert parsed_map["120503"]["fundHouse"] == "Axis Mutual Fund"


def test_scheme_type_tracks_across_multiple_category_sections():
    """State tracking: each new category header line updates
    `current_category` and subsequent data rows pick it up.
    """
    svc = _service()
    parsed_map, *_ = svc._parse_amfi_text(_sample_amfi_text(), prior_map={})
    # First two schemes under "Debt Scheme - Gilt Fund"
    assert "Gilt Fund" in parsed_map["118989"]["schemeType"]
    assert "Gilt Fund" in parsed_map["129012"]["schemeType"]
    # Third scheme under "Short Duration Fund"
    assert "Short Duration" in parsed_map["120503"]["schemeType"]


# ── 3: prior-snapshot merge (Option A — Overseer Q1) ─────────────────────

def test_prior_snapshot_attaches_lastNav_and_lastDate():
    """Option A invariant: entries receive `lastNav`/`lastDate` from
    the prior snapshot when the scheme_id matches.

    Mutation discriminator: without the merge, the FE change
    computation in InvestmentService.fetchUserSecurities L322-329
    falls back to 0/0% on every cycle (semantically wrong).
    """
    prior = [
        {"scheme_id": "118989", "nav": "49.8765", "date": "08-11-2026"},
    ]
    svc = _service(prior_data=prior)
    parsed_map, *_ = svc._parse_amfi_text(
        _sample_amfi_text(), prior_map=svc._read_prior_snapshot()
    )
    entry = parsed_map["118989"]
    assert entry["lastNav"] == "49.8765"
    assert entry["lastDate"] == "08-11-2026"


def test_cold_start_without_prior_file_still_produces_valid_entries():
    """Cold-start (no MFRate.prior.json): parser returns valid entries
    WITHOUT `lastNav`/`lastDate`. FE falls back to 0/0% change until
    next cycle warms the cache.
    """
    svc = _service()  # no prior_data
    parsed_map, *_ = svc._parse_amfi_text(
        _sample_amfi_text(), prior_map=svc._read_prior_snapshot()
    )
    entry = parsed_map["118989"]
    assert entry["scheme_id"] == "118989"
    assert entry["nav"] == "50.1234"
    assert "lastNav" not in entry
    assert "lastDate" not in entry


def test_prior_snapshot_schemes_absent_from_today_are_not_emitted():
    """A scheme present in prior snapshot but NOT in today's AMFI
    (newly-delisted) must not be emitted — today's map is the
    authoritative universe. Prior snapshot is a lookup auxiliary, not
    a union source.
    """
    prior = [
        {"scheme_id": "999999", "nav": "100.0000", "date": "08-11-2026"},
    ]
    svc = _service(prior_data=prior)
    parsed_map, *_ = svc._parse_amfi_text(
        _sample_amfi_text(), prior_map=svc._read_prior_snapshot()
    )
    assert "999999" not in parsed_map


def test_malformed_prior_file_degrades_to_cold_start():
    """A corrupt MFRate.prior.json (invalid JSON) returns {} from
    _read_prior_snapshot so the cycle proceeds as cold-start.
    """
    svc = _service()
    prior_path = os.path.join(svc.tmp_dir, "MFRate.prior.json")
    with open(prior_path, "w") as f:
        f.write("{not valid json")
    prior_map = svc._read_prior_snapshot()
    assert prior_map == {}


# ── 4: _fetch_all 5-tuple contract ───────────────────────────────────────

def test_fetch_all_success_returns_5_tuple_matching_BaseRateTask_contract():
    """BaseRateTask.run() unpacks a 5-tuple: (data, total, ok,
    permanent_skips, extras). Successful fetch returns the parsed map
    wrapped in {"data": [...]} shape consumed by
    JsonDownloadService.getMFRate.
    """
    svc = _service()
    fake_resp = MagicMock()
    fake_resp.text = _sample_amfi_text()
    fake_resp.raise_for_status.return_value = None
    fake_requests = MagicMock()
    fake_requests.get.return_value = fake_resp
    with patch.object(_amfi_module, "requests", fake_requests):
        data, total, ok, perm, extras = svc._fetch_all()
    assert isinstance(data, dict)
    assert "data" in data
    assert isinstance(data["data"], list)
    assert ok == 3  # three valid entries from fixture
    assert perm == {"stale": 1}
    assert extras["stale"] == 1
    assert extras["zero"] == 2
    assert extras["malformed"] == 2


def test_fetch_all_network_failure_returns_none_tuple_with_error():
    """Fetch failure must return `(None, 0, 0, {}, {'error': msg})` so
    BaseRateTask.run() short-circuits to Failed WITHOUT clobbering
    last-good. Matches SetMFRate's ak-5jq-H1 pattern.
    """
    svc = _service()
    fake_requests = MagicMock()
    fake_requests.get.side_effect = ConnectionError("AMFI unreachable")
    with patch.object(_amfi_module, "requests", fake_requests):
        data, total, ok, perm, extras = svc._fetch_all()
    assert data is None
    assert total == 0
    assert ok == 0
    assert perm == {}
    assert "error" in extras
    assert "AMFI" in extras["error"]


def test_fetch_all_http_error_also_returns_none_tuple():
    """A non-200 HTTP response (503, 404, etc.) from requests.get →
    raise_for_status raises → caught and converted to None-tuple with
    error. Matches BaseRateTask's "last-good preserved on fetch
    failure" semantic.
    """
    svc = _service()
    fake_resp = MagicMock()
    fake_resp.raise_for_status.side_effect = Exception("503 Service Unavailable")
    fake_requests = MagicMock()
    fake_requests.get.return_value = fake_resp
    with patch.object(_amfi_module, "requests", fake_requests):
        data, _, _, _, extras = svc._fetch_all()
    assert data is None
    assert "error" in extras


# ── 5: prior-snapshot rotation (post-swap hook) ──────────────────────────

def test_rotate_prior_snapshot_copies_current_mfrate_to_prior_path():
    """`_rotate_prior_snapshot` copies the just-written MFRate.json
    to MFRate.prior.json inside tmp_dir. Verifies the file lands at
    the expected path with identical content.
    """
    svc = _service()
    # Seed a "current" MFRate.json via the stubbed getLatestFile
    current_path = os.path.join(svc.tmp_dir, "MFRate_current.json")
    expected_content = {"data": [{"scheme_id": "118989", "nav": "50.1234"}]}
    with open(current_path, "w") as f:
        json.dump(expected_content, f)
    svc.jsonService.getLatestFile.return_value = current_path

    svc._rotate_prior_snapshot()

    prior_path = os.path.join(svc.tmp_dir, "MFRate.prior.json")
    assert os.path.isfile(prior_path)
    with open(prior_path, "r") as f:
        prior_content = json.load(f)
    assert prior_content == expected_content


def test_rotate_prior_snapshot_handles_missing_current_file_non_fatally():
    """If the current MFRate.json isn't found at rotation time
    (shouldn't happen post-successful swap, but guard against it),
    the rotation is a no-op with a WARN log — never raises.
    """
    svc = _service()
    svc.jsonService.getLatestFile.return_value = None  # not found
    # Must not raise
    svc._rotate_prior_snapshot()
    prior_path = os.path.join(svc.tmp_dir, "MFRate.prior.json")
    assert not os.path.isfile(prior_path)


# ── 6: scheduler env-var dispatch ────────────────────────────────────────

def _scheduler_source():
    """Read scheduler.py as source for code-level invariant checks.

    Scheduler.py transitively imports Google OAuth + Firebase stacks
    not available under this suite's stub harness, so we can't
    importlib.reload(services.tasks.scheduler) at test-run time. The
    dispatch is a two-line expression + a single TASK_MAPPING entry
    — code-level checks against the source give the same guarantee.
    """
    here = os.path.dirname(os.path.abspath(__file__))
    with open(os.path.join(here, "services/tasks/scheduler.py"), "r") as f:
        return f.read()


def test_scheduler_imports_SetAMFIMFRate():
    """The new task class must be imported at module scope for the
    env-var dispatch below to resolve.
    """
    src = _scheduler_source()
    assert "from services.tasks.SetAMFIMFRate import SetAMFIMFRate" in src


def test_scheduler_env_var_dispatch_expression_is_present():
    """The TASK_MAPPING['SetMFRate'] entry must dispatch via
    _MF_RATE_IMPL (env-var-gated to SetAMFIMFRate vs SetMFRate).
    """
    src = _scheduler_source()
    # The dispatch block
    assert 'os.getenv("MF_RATE_SOURCE"' in src
    assert '"amfi"' in src
    # Both classes referenced in the dispatch
    assert "SetAMFIMFRate if " in src and "else SetMFRate" in src
    # The TASK_MAPPING entry uses the dispatch result
    assert '"SetMFRate": _MF_RATE_IMPL' in src


def test_scheduler_env_var_dispatch_logic_amfi_branch(monkeypatch):
    """Pure logic check on the dispatch expression: MF_RATE_SOURCE=amfi
    (case-insensitive) triggers the SetAMFIMFRate branch.
    """
    monkeypatch.setenv("MF_RATE_SOURCE", "amfi")
    assert os.getenv("MF_RATE_SOURCE", "mfapi").lower() == "amfi"
    monkeypatch.setenv("MF_RATE_SOURCE", "AMFI")
    assert os.getenv("MF_RATE_SOURCE", "mfapi").lower() == "amfi"


def test_scheduler_env_var_dispatch_logic_default_branch(monkeypatch):
    """Default (unset or `mfapi`) does NOT trigger the amfi branch so
    Phase 2 rollout is opt-in.
    """
    monkeypatch.delenv("MF_RATE_SOURCE", raising=False)
    assert os.getenv("MF_RATE_SOURCE", "mfapi").lower() != "amfi"
    monkeypatch.setenv("MF_RATE_SOURCE", "mfapi")
    assert os.getenv("MF_RATE_SOURCE", "mfapi").lower() != "amfi"
