#!/usr/bin/env python3
"""
Unit tests for the Family B v2 rate-fetcher architecture.

Covers:
- `utils/web_extract.py::fetch_and_extract` — the shared helper
- `services/tasks/SetIBJAGoldRate.py::SetIBJAGoldRate`
- `services/tasks/SetEPFRate.py::SetEPFRate`
- `services/tasks/SetPpfRate.py::SetPPFRate`
- `utils/AIHelper.py::fetch_via_ai` (stocks.ipo)

Strategy:
- Monkeypatch `requests.get` to return a canned `_FakeResponse`
- Monkeypatch the SDK extraction call (`utils.sdk_runner.run_query_collect`) to
  return a canned `SdkRunResult` — so tests run without network or API key
- Assert each fetcher's `run()` method or the helper returns the expected
  shape, calls the right URL, and degrades cleanly on the expected errors

Convention matches existing project tests: standalone script, no pytest.
Run with: `python3 test_rate_fetchers_unit.py`. Exit code = pass/fail.

Refs: hq-wisp-bommx (Family B v2).
"""

import asyncio
import os
import sys
import tempfile
import types
from unittest.mock import patch, MagicMock

# Ensure repo root is on sys.path so `services` / `utils` imports work
sys.path.insert(0, os.path.dirname(os.path.abspath(__file__)))

# ── Stub optional deps that may not be installed in the worktree ─────
# Production has `anyio` and `claude_agent_sdk` from requirements.txt; the
# crew worktree is a code-edit sandbox without a venv (per project
# conventions). All real anyio.run / SDK calls are patched in the tests
# below, so stubs just need to satisfy import statements without affecting
# any production code path.
if "anyio" not in sys.modules:
    # Real anyio.run takes an async-function and runs it. The stub uses
    # asyncio.run so the patched run_query_collect coroutine actually
    # executes and returns the fake SdkRunResult — without this, callers
    # would get None and AttributeError on `.error`.
    _anyio = types.ModuleType("anyio")

    def _anyio_run_stub(async_func, *args, **kw):
        return asyncio.run(async_func(*args, **kw))

    _anyio.run = _anyio_run_stub
    sys.modules["anyio"] = _anyio

if "claude_agent_sdk" not in sys.modules:
    _sdk = types.ModuleType("claude_agent_sdk")
    _sdk.ClaudeAgentOptions = type("ClaudeAgentOptions", (), {
        "__init__": lambda self, **kw: setattr(self, "__dict__", dict(kw)) or None,
    })
    _sdk.query = lambda **kw: None
    _sdk.AssistantMessage = type("AssistantMessage", (), {})
    _sdk.ResultMessage = type("ResultMessage", (), {})
    _sdk.TextBlock = type("TextBlock", (), {})
    _sdk.ToolUseBlock = type("ToolUseBlock", (), {})
    sys.modules["claude_agent_sdk"] = _sdk

if "flask" not in sys.modules:
    # utils/logger imports `from flask import jsonify`. Stub flask with just
    # a jsonify placeholder — none of these tests exercise jsonify.
    _flask = types.ModuleType("flask")
    _flask.jsonify = lambda *a, **kw: None
    sys.modules["flask"] = _flask

# Stub the entire services.tasks.baseTask chain so we can import fetcher
# subclasses without dragging in sqlalchemy / models / db. The fetcher run()
# logic is verified end-to-end in the integration test against real upstream;
# unit tests focus on URL/SCHEMA constants + the pure post-processing
# function (PPF period expansion).
if "services.tasks.baseTask" not in sys.modules:
    _bt = types.ModuleType("services.tasks.baseTask")

    class _StubBaseTask:
        def __init__(self, title=None, priority=None):
            self.title = title
            self.priority = priority

    _bt.BaseTask = _StubBaseTask
    sys.modules["services.tasks.baseTask"] = _bt

# Test counter + failure list — surfaced at the end for a clean summary
_PASS = 0
_FAIL = 0
_FAILURES = []


def _record(name, ok, reason=""):
    global _PASS, _FAIL
    if ok:
        _PASS += 1
        print(f"  PASS  {name}")
    else:
        _FAIL += 1
        _FAILURES.append((name, reason))
        print(f"  FAIL  {name} — {reason}")


# ── Fakes ────────────────────────────────────────────────────────────


class _FakeResponse:
    """Mimics the subset of requests.Response that fetch_and_extract uses."""

    def __init__(self, text="<html>fake</html>", status_code=200, raise_exc=None):
        self.text = text
        self.status_code = status_code
        self._raise_exc = raise_exc

    def raise_for_status(self):
        if self._raise_exc is not None:
            raise self._raise_exc


def _make_fake_sdk_result(structured_output=None, error=None, tool_calls=0):
    """Build an object shaped like utils.sdk_runner.SdkRunResult."""
    obj = MagicMock()
    obj.structured_output = structured_output
    obj.error = error
    obj.text = ""
    obj.tool_calls = tool_calls
    obj.turns = 1 if structured_output is not None else 0
    obj.latency_ms = 100
    obj.error_class = None
    return obj


def _patch_sdk(structured_output=None, error=None, tool_calls=0):
    """
    Return a context manager that patches `run_query_collect` so the SDK is
    never actually invoked. The patched coroutine returns the canned
    SdkRunResult fake.
    """
    fake = _make_fake_sdk_result(
        structured_output=structured_output,
        error=error,
        tool_calls=tool_calls,
    )

    async def _fake_run_query_collect(*, agent, options, prompt):
        # Drain the async generator so it doesn't leak (matches real SDK call shape)
        try:
            agen = prompt()
            async for _ in agen:
                pass
        except TypeError:
            # `prompt` was a string, not a callable
            pass
        return fake

    return patch("utils.web_extract.run_query_collect", new=_fake_run_query_collect)


# ── Tests: web_extract.fetch_and_extract ─────────────────────────────


def test_web_extract():
    print("\n[web_extract.fetch_and_extract]")

    from utils import web_extract

    # 1. Happy path — fetch succeeds, SDK returns structured output
    fake_resp = _FakeResponse(text="hello world")
    with patch("utils.web_extract.requests.get", return_value=fake_resp), \
         _patch_sdk(structured_output={"data": [{"Year": "2024-04", "Interest Rate": 8.25}]}):
        result, err = web_extract.fetch_and_extract(
            url="https://example.com/rates",
            system_prompt="extract rates",
            schema={"type": "object", "properties": {"data": {"type": "array"}}},
            agent="test.happy",
        )
    _record("happy path returns structured output",
            result == {"data": [{"Year": "2024-04", "Interest Rate": 8.25}]} and err == "",
            f"got result={result!r} err={err!r}")

    # 2. HTTP fetch failure surfaces as an error detail (not crash)
    import requests as _requests
    with patch("utils.web_extract.requests.get",
               side_effect=_requests.ConnectionError("network down")):
        result, err = web_extract.fetch_and_extract(
            url="https://example.com/dead",
            system_prompt="extract",
            schema={"type": "object"},
            agent="test.fetch_fail",
        )
    _record("HTTP fetch error returns (None, detail)",
            result is None and "HTTP fetch failed" in (err or ""),
            f"got result={result!r} err={err!r}")

    # 3. HTTPError (4xx/5xx) also surfaces as fetch failure
    with patch("utils.web_extract.requests.get",
               return_value=_FakeResponse(raise_exc=_requests.HTTPError("404"))):
        result, err = web_extract.fetch_and_extract(
            url="https://example.com/404",
            system_prompt="extract",
            schema={"type": "object"},
            agent="test.http_4xx",
        )
    _record("HTTP 4xx returns (None, detail)",
            result is None and "HTTP fetch failed" in (err or ""),
            f"got result={result!r} err={err!r}")

    # 4. Sonnet returns no structured_output → caller gets a specific error
    fake_resp = _FakeResponse(text="hello")
    with patch("utils.web_extract.requests.get", return_value=fake_resp), \
         _patch_sdk(structured_output=None):
        result, err = web_extract.fetch_and_extract(
            url="https://example.com",
            system_prompt="x",
            schema={"type": "object"},
            agent="test.no_struct",
        )
    _record("missing structured_output returns specific detail",
            result is None and "structured_output" in (err or ""),
            f"got result={result!r} err={err!r}")

    # 5. Sonnet returns an error from the SDK
    fake_resp = _FakeResponse(text="hello")
    with patch("utils.web_extract.requests.get", return_value=fake_resp), \
         _patch_sdk(structured_output=None, error="Provider unavailable"):
        result, err = web_extract.fetch_and_extract(
            url="https://example.com",
            system_prompt="x",
            schema={"type": "object"},
            agent="test.sdk_error",
        )
    _record("SDK error surfaces in detail",
            result is None and "Sonnet extraction error" in (err or ""),
            f"got result={result!r} err={err!r}")

    # 6a. v5 (hq-wisp-a0byf): tools-wander branch — model called a tool
    # but emitted no structured_output. Error string must differentiate
    # this from the fast-fail "no structured_output" case so the future
    # archaeologist can tell the two failure modes apart in journal logs.
    fake_resp = _FakeResponse(text="hello")
    with patch("utils.web_extract.requests.get", return_value=fake_resp), \
         _patch_sdk(structured_output=None, tool_calls=2):
        result, err = web_extract.fetch_and_extract(
            url="https://example.com",
            system_prompt="x",
            schema={"type": "object"},
            agent="test.tools_wander",
        )
    _record("tools-wander returns differentiated error",
            result is None and "wandered into tools" in (err or "")
            and "called=2" in (err or ""),
            f"got result={result!r} err={err!r}")

    # 6b. v5: allowed_tools=[] is wired into the SDK options. Capture the
    # constructed ClaudeAgentOptions and assert the lock is present —
    # without this, the model is free to wander into Grep/Read/etc. on
    # large HTML payloads (rate.epf post-v4 symptom).
    fake_resp = _FakeResponse(text="hello")
    captured_options = {}

    async def _capture_options(*, agent, options, prompt):
        captured_options["options"] = options
        # Drain the async generator
        try:
            agen = prompt()
            async for _ in agen:
                pass
        except TypeError:
            pass
        return _make_fake_sdk_result(structured_output={"ok": True})

    with patch("utils.web_extract.requests.get", return_value=fake_resp), \
         patch("utils.web_extract.run_query_collect", new=_capture_options):
        web_extract.fetch_and_extract(
            url="https://example.com",
            system_prompt="x",
            schema={"type": "object"},
            agent="test.allowed_tools",
        )
    _record("allowed_tools=[] locks model to structured_output only",
            getattr(captured_options.get("options"), "allowed_tools", None) == [],
            f"got allowed_tools={getattr(captured_options.get('options'), 'allowed_tools', '<missing>')!r}")

    # 7. Truncation: response > max_chars is clipped (no crash)
    big_body = "x" * 250_000
    fake_resp = _FakeResponse(text=big_body)
    with patch("utils.web_extract.requests.get", return_value=fake_resp), \
         _patch_sdk(structured_output={"data": []}):
        result, err = web_extract.fetch_and_extract(
            url="https://example.com",
            system_prompt="x",
            schema={"type": "object"},
            agent="test.truncate",
            max_chars=10_000,
        )
    _record("oversized response is truncated cleanly",
            result == {"data": []} and err == "",
            f"got result={result!r} err={err!r}")


# ── Tests: SetIBJAGoldRate ───────────────────────────────────────────


def test_gold_fetcher():
    print("\n[SetIBJAGoldRate]")

    from services.tasks import SetIBJAGoldRate as gold_mod

    # Constants encode the contract with web_extract — verify URL, schema
    # shape, and agent label without running the full BaseTask pipeline
    # (which would require sqlalchemy + models + db).
    _record("GOLD_URL is an https IBJA URL",
            isinstance(gold_mod.GOLD_URL, str)
            and gold_mod.GOLD_URL.startswith("https://")
            and "ibja" in gold_mod.GOLD_URL.lower(),
            f"got GOLD_URL={gold_mod.GOLD_URL!r}")
    # v4: schema keys are API-compliant snake_case (`carat_NN`); the on-disk
    # display keys ("24 Carat" etc.) are produced by SetIBJAGoldRate.run via
    # _GOLD_DISPLAY_KEYS re-map. Schema-side assertion verifies the keys are
    # regex-valid (no spaces); the re-map is a separate assertion.
    import re as _re
    _api_key_re = _re.compile(r"^[a-zA-Z0-9_.-]{1,64}$")
    _record("GOLD_SCHEMA requires the carat_24/22/18 top-level keys",
            isinstance(gold_mod.GOLD_SCHEMA, dict)
            and set(gold_mod.GOLD_SCHEMA.get("required", []))
                >= {"carat_24", "carat_22", "carat_18", "ibja_data"},
            f"got required={gold_mod.GOLD_SCHEMA.get('required')!r}")
    _record("GOLD_SCHEMA declares integer types for Carat values",
            gold_mod.GOLD_SCHEMA["properties"]["carat_24"]["type"] == "integer"
            and gold_mod.GOLD_SCHEMA["properties"]["carat_22"]["type"] == "integer",
            "Carat fields should be integer per IBJA convention")
    _record("GOLD_SCHEMA top-level keys satisfy Anthropic property-key regex",
            all(_api_key_re.match(k) for k in gold_mod.GOLD_SCHEMA["properties"]),
            f"v4 regression guard — bad keys: "
            f"{[k for k in gold_mod.GOLD_SCHEMA['properties'] if not _api_key_re.match(k)]}")
    _record("_GOLD_DISPLAY_KEYS maps schema keys → display form for on-disk JSON",
            gold_mod._GOLD_DISPLAY_KEYS == [
                ("carat_24", "24 Carat"),
                ("carat_22", "22 Carat"),
                ("carat_18", "18 Carat"),
            ],
            "frontend reads GoldRate.json with the historical display keys; "
            "the re-map must preserve that on-disk contract")
    _record("GOLD_SYSTEM_PROMPT mentions IBJA + carat purities",
            "IBJA" in gold_mod.GOLD_SYSTEM_PROMPT
            and "999" in gold_mod.GOLD_SYSTEM_PROMPT
            and "916" in gold_mod.GOLD_SYSTEM_PROMPT,
            "system prompt should reference upstream + standard purities")


# ── Tests: SetEPFRate ────────────────────────────────────────────────


def test_epf_fetcher():
    print("\n[SetEPFRate]")

    from services.tasks import SetEPFRate as epf_mod

    _record("EPF_URL is an https Wikipedia URL",
            isinstance(epf_mod.EPF_URL, str)
            and epf_mod.EPF_URL.startswith("https://")
            and "wikipedia" in epf_mod.EPF_URL.lower(),
            f"got EPF_URL={epf_mod.EPF_URL!r}")
    _record("EPF_SCHEMA requires top-level `data` array",
            isinstance(epf_mod.EPF_SCHEMA, dict)
            and "data" in epf_mod.EPF_SCHEMA.get("required", []),
            f"got required={epf_mod.EPF_SCHEMA.get('required')!r}")
    # v4: schema items use compliant `interest_rate` (snake_case); the on-disk
    # "Interest Rate" key is restored by SetEPFRate.run's per-entry re-map.
    _record("EPF_SCHEMA items have Year + interest_rate shape (API-compliant)",
            epf_mod.EPF_SCHEMA["properties"]["data"]["items"]["required"]
                == ["Year", "interest_rate"],
            "data items must satisfy the Anthropic property-key regex; "
            "SetEPFRate.run re-maps to the on-disk EPFRate.json key before saving")
    _record("EPF_SYSTEM_PROMPT specifies YYYY-MM format + April-March FY expansion",
            "YYYY-MM" in epf_mod.EPF_SYSTEM_PROMPT
            and "April" in epf_mod.EPF_SYSTEM_PROMPT
            and "March" in epf_mod.EPF_SYSTEM_PROMPT,
            "system prompt must encode the FY-to-monthly expansion rule")


# ── Tests: SetPPFRate ────────────────────────────────────────────────


def test_ppf_fetcher():
    print("\n[SetPPFRate]")

    from services.tasks import SetPpfRate as ppf_mod

    _record("PPF_URL is an https Wikipedia URL",
            isinstance(ppf_mod.PPF_URL, str)
            and ppf_mod.PPF_URL.startswith("https://")
            and "wikipedia" in ppf_mod.PPF_URL.lower(),
            f"got PPF_URL={ppf_mod.PPF_URL!r}")
    _record("PPF_SCHEMA requires top-level `periods` array",
            isinstance(ppf_mod.PPF_SCHEMA, dict)
            and "periods" in ppf_mod.PPF_SCHEMA.get("required", []),
            f"got required={ppf_mod.PPF_SCHEMA.get('required')!r}")
    _record("PPF_SCHEMA items have from + rate shape",
            ppf_mod.PPF_SCHEMA["properties"]["periods"]["items"]["required"]
                == ["from", "rate"],
            "periods items must match the v1 contract consumed by _expand_periods_to_monthly")

    # _expand_periods_to_monthly is a pure, deterministic function — exercise it
    # directly with deterministic input to verify the post-processing math
    # didn't drift in the rewrite. This is the only fetcher with nontrivial
    # post-processing; gold and EPF just save the extracted JSON as-is.
    periods = [
        {"from": "2023-04", "rate": 7.1},
        {"from": "2023-07", "rate": 7.1},
    ]
    expanded = ppf_mod.SetPPFRate._expand_periods_to_monthly(periods)
    _record("expand_periods: first 3 months under first period are Apr/May/Jun",
            len(expanded) >= 3
            and expanded[0] == {"Year": "2023-04", "Interest Rate": 7.1}
            and expanded[1] == {"Year": "2023-05", "Interest Rate": 7.1}
            and expanded[2] == {"Year": "2023-06", "Interest Rate": 7.1},
            f"got expanded[:3]={expanded[:3]!r}")
    _record("expand_periods: year-rollover handled (Dec → Jan +1y)",
            # Try a period that crosses year boundary
            ppf_mod.SetPPFRate._expand_periods_to_monthly([
                {"from": "2022-11", "rate": 7.0},
                {"from": "2023-02", "rate": 7.1},
            ])[:3]
            == [
                {"Year": "2022-11", "Interest Rate": 7.0},
                {"Year": "2022-12", "Interest Rate": 7.0},
                {"Year": "2023-01", "Interest Rate": 7.0},
            ],
            "month++ should roll year++ when month > 12")


# ── Tests: AIHelper (stocks.ipo) ─────────────────────────────────────


def test_stocks_ipo():
    print("\n[utils.AIHelper.fetch_via_ai — stocks.ipo]")

    from utils import AIHelper

    # Constants
    _record("_IPO_URL is an https Chittorgarh URL",
            isinstance(AIHelper._IPO_URL, str)
            and AIHelper._IPO_URL.startswith("https://")
            and "chittorgarh" in AIHelper._IPO_URL.lower(),
            f"got _IPO_URL={AIHelper._IPO_URL!r}")
    _record("_IPO_SCHEMA requires top-level `results` array",
            "results" in AIHelper._IPO_SCHEMA.get("required", []),
            f"got required={AIHelper._IPO_SCHEMA.get('required')!r}")
    _record("_IPO_SCHEMA items require isin + price + date (caller-needed fields)",
            set(AIHelper._IPO_SCHEMA["properties"]["results"]["items"]["required"])
                >= {"isin", "allotment_price", "allotment_date"},
            "missing keys would break StocksService._lookup_ipo_prices flow")

    # 1. Happy path: sonnet returns the expected results array
    ipo_data = {
        "results": [
            {"symbol": "TATAMOT", "isin": "INE155A01022",
             "allotment_price": 245.5, "allotment_date": "2010-09-15",
             "source": "IPO 2010-09"},
        ]
    }
    fake_resp = _FakeResponse(text="<html>ipo list page</html>")
    with patch("utils.web_extract.requests.get", return_value=fake_resp), \
         _patch_sdk(structured_output=ipo_data):
        result, err = AIHelper.fetch_via_ai(
            "Look up these stocks: TATAMOT (INE155A01022), first sell 2024-01-15"
        )
    _record("happy path returns results array",
            result == ipo_data and err == "",
            f"got result={result!r} err={err!r}")

    # 2. Empty results (no matching stocks found) is a valid response — caller
    #    handles it. fetch_via_ai should NOT report an error in this case.
    fake_resp = _FakeResponse(text="<html>x</html>")
    with patch("utils.web_extract.requests.get", return_value=fake_resp), \
         _patch_sdk(structured_output={"results": []}):
        result, err = AIHelper.fetch_via_ai("Look up: UNKNOWNTICKER (INE000X00000)")
    _record("empty results is success (no error reported)",
            result == {"results": []} and err == "",
            f"got result={result!r} err={err!r}")

    # 3. SDK error surfaces as detail (caller logs and returns [] in StocksService)
    fake_resp = _FakeResponse(text="<html>x</html>")
    with patch("utils.web_extract.requests.get", return_value=fake_resp), \
         _patch_sdk(structured_output=None, error="quota exceeded"):
        result, err = AIHelper.fetch_via_ai("Look up: X (INE000X)")
    _record("SDK error returns (None, detail)",
            result is None and err and "quota exceeded" in err,
            f"got result={result!r} err={err!r}")


# ── Driver ───────────────────────────────────────────────────────────


def main():
    print("=" * 70)
    print("Family B v2 unit tests — rate fetchers + stocks.ipo (web_extract arch)")
    print("=" * 70)

    test_web_extract()
    test_gold_fetcher()
    test_epf_fetcher()
    test_ppf_fetcher()
    test_stocks_ipo()

    print("\n" + "=" * 70)
    print(f"Result: {_PASS} passed, {_FAIL} failed")
    if _FAILURES:
        print("\nFailures:")
        for name, reason in _FAILURES:
            print(f"  - {name}: {reason}")
    print("=" * 70)
    return 0 if _FAIL == 0 else 1


if __name__ == "__main__":
    sys.exit(main())
