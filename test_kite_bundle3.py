"""Tests for ak-dx98 Kite Bundle 3 (monitoring surfaces).

Covers the surfaces that can be verified without a real DB, live
Flask, or live Kite:

  * Tool registration + dispatch parity for ``get_kite_gtts`` +
    ``get_kite_alerts``.
  * ``KiteService`` wrapper routing — each of ``get_gtt_triggers``,
    ``get_gtt_trigger``, ``get_alerts``, ``get_alert_history``
    passes through ``_call`` so a KiteTokenExpired at the lower
    layer surfaces consistently.
  * ``InvestmentService.fetchKiteGtts`` caching + LTP-distance
    enrichment (active triggers get a ``monitor`` block with
    current_ltp + distance_pct; non-active get ``monitor=None``;
    OHLC fetch failure → fail-soft with ``monitor_error``).
  * ``InvestmentService.fetchKiteAlerts`` caching — same pattern as
    margins + GTTs: hit returns cached, miss fetches, failure
    evicts stale.
  * ``InvestmentService.fetchKiteAlertHistory`` pass-through — no
    cache (drill-down wants fresh).

Phase-B (full BE smoke via live Kite) exercises a real account —
operator-run manual verification post-merge per Lead's standard
review dispatch.
"""
import re
import os
import sys
import types
from unittest.mock import MagicMock

import pytest


# claude_agent_sdk + anyio stubs — same pattern as
# test_investment_agent_foundation. Withdraw after imports so later
# suites that detect SDK-absence still skip correctly.
_INSTALLED_STUBS = []
for _pkg in ("claude_agent_sdk", "anyio"):
    if _pkg not in sys.modules:
        _m = types.ModuleType(_pkg)
        _m.__path__ = []
        _m.__getattr__ = lambda name, pkg=_pkg: MagicMock(name=f"{pkg}.{name}")
        sys.modules[_pkg] = _m
        _INSTALLED_STUBS.append(_pkg)

from services.agent_tools import INVESTMENT_TOOLS  # noqa: E402

for _pkg in _INSTALLED_STUBS:
    sys.modules.pop(_pkg, None)


def _read_source(rel_path):
    here = os.path.dirname(os.path.abspath(__file__))
    with open(os.path.join(here, rel_path), "r") as f:
        return f.read()


# ── 1: Tool registration + dispatch parity ───────────────────────────────


@pytest.mark.parametrize("tool_name", ["get_kite_gtts", "get_kite_alerts"])
def test_bundle3_tool_registered_in_INVESTMENT_TOOLS(tool_name):
    by_name = {t["name"]: t for t in INVESTMENT_TOOLS}
    assert tool_name in by_name, (
        f"{tool_name} missing from INVESTMENT_TOOLS — agent cannot invoke "
        f"an unregistered tool"
    )
    tool_def = by_name[tool_name]
    assert "description" in tool_def and tool_def["description"].strip()
    assert tool_def["input_schema"]["type"] == "object"
    # Both tools are account-level + take no params (matches Bundle 2
    # pattern). A future per-UUID / per-trigger-id variant would carry
    # required args; guard against accidental param drift here.
    assert tool_def["input_schema"].get("required", []) == []


@pytest.mark.parametrize("tool_name", ["get_kite_gtts", "get_kite_alerts"])
def test_bundle3_tool_dispatched_in_executor(tool_name):
    """Source-level grep for the dispatch branch — importing the
    executor pulls the full Flask+Firebase chain not available in CI.
    """
    src = _read_source("services/agent_tool_executor.py")
    pattern = re.compile(
        rf'elif\s+tool_name\s*==\s*["\']{re.escape(tool_name)}["\']\s*:'
    )
    assert pattern.search(src), (
        f"{tool_name} has no dispatch branch in _execute_investment_tool"
    )


# ── 2: KiteService wrapper routing ───────────────────────────────────────


def _kite_service_stub():
    """Build a KiteService-shaped stub whose ``_call`` echoes the
    lambda result — mirrors the real flow's ``_call`` seam without
    pulling the full token-auth chain. Any wrapper method under
    test must route through this ``_call`` or it bypasses the
    production error-normalization path (KiteTokenExpired etc.).
    """
    svc = types.SimpleNamespace()
    svc.logger = MagicMock()
    svc._kite_client = MagicMock()
    svc._call_log = []

    def _call(user_id, label, fn):
        svc._call_log.append((user_id, label))
        return fn(svc._kite_client)

    svc._call = _call
    return svc


def _bind_wrapper(svc, method_name):
    """Pull a wrapper method straight off KiteService's class via
    source-text + exec — same trick as test_investment_web_intelligence
    uses for compute_allowed_tools, needed here because importing
    KiteService transitively pulls kiteconnect + Firebase.
    """
    import ast
    src = _read_source("services/KiteService.py")
    tree = ast.parse(src)
    for node in tree.body:
        if isinstance(node, ast.ClassDef) and node.name == "KiteService":
            for item in node.body:
                if (
                    isinstance(item, ast.FunctionDef)
                    and item.name == method_name
                ):
                    # Strip decorators so exec gets a plain def.
                    item.decorator_list = []
                    ns = {}
                    exec(
                        compile(ast.Module([item], []), "<extract>", "exec"),
                        ns,
                    )
                    return ns[method_name].__get__(svc, type(svc))
    raise RuntimeError(f"{method_name} not found on KiteService")


def test_get_gtt_triggers_routes_through_call():
    svc = _kite_service_stub()
    svc._kite_client.get_gtts.return_value = [{"id": 1, "status": "active"}]
    get_gtt_triggers = _bind_wrapper(svc, "get_gtt_triggers")

    result = get_gtt_triggers("u1")

    assert result == [{"id": 1, "status": "active"}]
    svc._kite_client.get_gtts.assert_called_once()
    assert svc._call_log[-1] == ("u1", "GTT triggers")


def test_get_gtt_trigger_routes_through_call_with_id():
    svc = _kite_service_stub()
    svc._kite_client.get_gtt.return_value = {"id": 42, "status": "cancelled"}
    get_gtt_trigger = _bind_wrapper(svc, "get_gtt_trigger")

    result = get_gtt_trigger("u1", 42)

    assert result == {"id": 42, "status": "cancelled"}
    svc._kite_client.get_gtt.assert_called_once_with(42)
    # Call label carries the id so journalctl grep can bisect.
    assert svc._call_log[-1] == ("u1", "GTT trigger 42")


def test_get_alerts_routes_through_call_raw_get():
    """Alerts use the SDK's raw ``_get('/alerts')`` escape hatch
    because the named wrapper isn't uniformly available across
    kiteconnect Python SDK versions."""
    svc = _kite_service_stub()
    svc._kite_client._get.return_value = [
        {"uuid": "a1", "type": "simple"},
        {"uuid": "a2", "type": "ato"},
    ]
    get_alerts = _bind_wrapper(svc, "get_alerts")

    result = get_alerts("u1")

    assert len(result) == 2
    assert result[0]["uuid"] == "a1"
    svc._kite_client._get.assert_called_once_with("/alerts")
    assert svc._call_log[-1] == ("u1", "alerts")


def test_get_alerts_returns_empty_list_on_none():
    """Defensive — Kite returning None should surface as [] so the
    cache + FE don't trip on a NoneType.len() further up the stack."""
    svc = _kite_service_stub()
    svc._kite_client._get.return_value = None
    get_alerts = _bind_wrapper(svc, "get_alerts")

    result = get_alerts("u1")

    assert result == []


def test_get_alert_history_routes_through_call_raw_get():
    svc = _kite_service_stub()
    svc._kite_client._get.return_value = [
        {"triggered_at": "2026-11-01T10:00:00", "meta": {}},
    ]
    get_alert_history = _bind_wrapper(svc, "get_alert_history")

    result = get_alert_history("u1", "uuid-xyz")

    assert len(result) == 1
    svc._kite_client._get.assert_called_once_with(
        "/alerts/uuid-xyz/history"
    )
    assert svc._call_log[-1] == ("u1", "alert history uuid-xyz")


def test_get_alert_history_returns_empty_list_on_none():
    svc = _kite_service_stub()
    svc._kite_client._get.return_value = None
    get_alert_history = _bind_wrapper(svc, "get_alert_history")

    result = get_alert_history("u1", "uuid-xyz")

    assert result == []


# ── 3: fetchKiteGtts caching + enrichment ────────────────────────────────


def _investment_service_stub():
    """Build an InvestmentService-shaped stub with just the seams
    fetchKiteGtts / fetchKiteAlerts / _enrich_gtts_with_ltp touch.

    Inlines the method bodies via AST-extract from the real source
    so the tests guard the shipped logic (not a hand-written clone
    that could drift silently).
    """
    import ast
    src = _read_source("services/InvestmentService.py")
    tree = ast.parse(src)

    inst = types.SimpleNamespace()
    inst.StockService = types.SimpleNamespace()
    inst.StockService.kite_service = MagicMock()
    inst.logger = MagicMock()

    # Pull class-body assignments (the _*_ttl_seconds constants) + the
    # 4 method defs we need.
    wanted_methods = {
        "fetchKiteGtts",
        "fetchKiteAlerts",
        "fetchKiteAlertHistory",
        "_enrich_gtts_with_ltp",
    }
    wanted_assigns = {
        "_kite_gtts_cache_ttl_seconds",
        "_kite_alerts_cache_ttl_seconds",
    }
    for node in tree.body:
        if isinstance(node, ast.ClassDef) and node.name == "InvestmentService":
            for item in node.body:
                if isinstance(item, ast.Assign):
                    for tgt in item.targets:
                        if isinstance(tgt, ast.Name) and tgt.id in wanted_assigns:
                            ns = {}
                            exec(
                                compile(
                                    ast.Module([item], []),
                                    "<extract>", "exec",
                                ),
                                ns,
                            )
                            setattr(inst, tgt.id, ns[tgt.id])
                elif (
                    isinstance(item, ast.FunctionDef)
                    and item.name in wanted_methods
                ):
                    item.decorator_list = []
                    ns = {}
                    exec(
                        compile(ast.Module([item], []), "<extract>", "exec"),
                        ns,
                    )
                    setattr(inst, item.name, ns[item.name].__get__(
                        inst, type(inst)
                    ))
    return inst


def test_fetchKiteGtts_fetches_on_first_call_and_caches():
    inst = _investment_service_stub()
    inst.StockService.kite_service.get_gtt_triggers.return_value = [
        {"id": 1, "status": "cancelled"},
    ]

    first = inst.fetchKiteGtts("u1")
    second = inst.fetchKiteGtts("u1")

    assert first == second
    inst.StockService.kite_service.get_gtt_triggers.assert_called_once()


def test_fetchKiteGtts_per_user_cache_isolation():
    inst = _investment_service_stub()
    inst.StockService.kite_service.get_gtt_triggers.side_effect = [
        [{"id": 1, "status": "cancelled"}],
        [{"id": 2, "status": "cancelled"}],
    ]

    a = inst.fetchKiteGtts("user-A")
    b = inst.fetchKiteGtts("user-B")

    assert a[0]["id"] == 1
    assert b[0]["id"] == 2
    assert (
        inst.StockService.kite_service.get_gtt_triggers.call_count == 2
    )


def test_fetchKiteGtts_fetch_failure_evicts_stale():
    inst = _investment_service_stub()
    inst.StockService.kite_service.get_gtt_triggers.return_value = [
        {"id": 1, "status": "cancelled"}
    ]
    inst.fetchKiteGtts("u1")

    inst._kite_gtts_cache.pop("u1", None)
    inst.StockService.kite_service.get_gtt_triggers.side_effect = (
        RuntimeError("Kite down")
    )

    with pytest.raises(RuntimeError):
        inst.fetchKiteGtts("u1")

    assert "u1" not in inst._kite_gtts_cache


def test_fetchKiteGtts_enriches_active_triggers_with_distance():
    """Active GTT with scalar trigger value → monitor block with
    current_ltp + scalar distance_pct."""
    inst = _investment_service_stub()
    inst.StockService.kite_service.get_gtt_triggers.return_value = [
        {
            "id": 1,
            "status": "active",
            "type": "single",
            "condition": {
                "exchange": "NSE",
                "tradingsymbol": "RELIANCE",
                "trigger_values": [1400.0],
            },
        },
    ]
    inst.StockService.kite_service.get_ohlc.return_value = {
        "NSE:RELIANCE": {"last_price": 1456.0},
    }

    result = inst.fetchKiteGtts("u1")

    assert result[0]["monitor"] is not None
    assert result[0]["monitor"]["current_ltp"] == 1456.0
    # (1456 - 1400) / 1400 * 100 = 4.0
    assert result[0]["monitor"]["distance_pct"] == pytest.approx(4.0)


def test_fetchKiteGtts_enriches_oco_two_leg_with_distance_list():
    """OCO (two-leg) GTT → distance_pct is a list of 2."""
    inst = _investment_service_stub()
    inst.StockService.kite_service.get_gtt_triggers.return_value = [
        {
            "id": 1,
            "status": "active",
            "type": "two-leg",
            "condition": {
                "exchange": "NSE",
                "tradingsymbol": "INFY",
                "trigger_values": [1500.0, 1700.0],
            },
        },
    ]
    inst.StockService.kite_service.get_ohlc.return_value = {
        "NSE:INFY": {"last_price": 1600.0},
    }

    result = inst.fetchKiteGtts("u1")

    distances = result[0]["monitor"]["distance_pct"]
    assert isinstance(distances, list)
    assert len(distances) == 2
    # (1600 - 1500)/1500 * 100 = 6.6667; (1600 - 1700)/1700 * 100 = -5.8824
    assert distances[0] == pytest.approx(6.6667, abs=1e-4)
    assert distances[1] == pytest.approx(-5.8824, abs=1e-4)


def test_fetchKiteGtts_non_active_triggers_get_monitor_None():
    inst = _investment_service_stub()
    inst.StockService.kite_service.get_gtt_triggers.return_value = [
        {
            "id": 1,
            "status": "cancelled",
            "condition": {
                "exchange": "NSE",
                "tradingsymbol": "RELIANCE",
                "trigger_values": [1400.0],
            },
        },
        {
            "id": 2,
            "status": "triggered",
            "condition": {
                "exchange": "NSE",
                "tradingsymbol": "INFY",
                "trigger_values": [1500.0],
            },
        },
    ]

    result = inst.fetchKiteGtts("u1")

    assert result[0]["monitor"] is None
    assert result[1]["monitor"] is None
    # Non-active triggers → no OHLC fetch at all.
    inst.StockService.kite_service.get_ohlc.assert_not_called()


def test_fetchKiteGtts_ohlc_failure_fails_soft_with_error_class():
    """OHLC batch fetch failure → active GTTs get a monitor dict
    with error_class='ohlc_fetch_failed' rather than crashing
    the whole dashboard call."""
    inst = _investment_service_stub()
    inst.StockService.kite_service.get_gtt_triggers.return_value = [
        {
            "id": 1,
            "status": "active",
            "condition": {
                "exchange": "NSE",
                "tradingsymbol": "RELIANCE",
                "trigger_values": [1400.0],
            },
        },
    ]
    inst.StockService.kite_service.get_ohlc.side_effect = RuntimeError(
        "quote endpoint down"
    )

    result = inst.fetchKiteGtts("u1")

    assert isinstance(result[0]["monitor"], dict)
    assert result[0]["monitor"]["error_class"] == "ohlc_fetch_failed"
    assert "quote endpoint down" in result[0]["monitor"]["message"]


def test_fetchKiteGtts_quote_missing_for_instrument_falls_back():
    """OHLC batch returns nothing for the GTT's symbol (Kite dropped
    it / stale instrument) → monitor marks it quote_unavailable."""
    inst = _investment_service_stub()
    inst.StockService.kite_service.get_gtt_triggers.return_value = [
        {
            "id": 1,
            "status": "active",
            "condition": {
                "exchange": "NSE",
                "tradingsymbol": "RELIANCE",
                "trigger_values": [1400.0],
            },
        },
    ]
    inst.StockService.kite_service.get_ohlc.return_value = {}  # empty

    result = inst.fetchKiteGtts("u1")

    assert result[0]["monitor"]["error_class"] == "quote_unavailable"


def test_fetchKiteGtts_zero_trigger_value_doesnt_divide_by_zero():
    """Defensive — trigger_value=0 is illegal but we shouldn't
    crash the whole response if Kite returns one."""
    inst = _investment_service_stub()
    inst.StockService.kite_service.get_gtt_triggers.return_value = [
        {
            "id": 1,
            "status": "active",
            "condition": {
                "exchange": "NSE",
                "tradingsymbol": "RELIANCE",
                "trigger_values": [0.0],
            },
        },
    ]
    inst.StockService.kite_service.get_ohlc.return_value = {
        "NSE:RELIANCE": {"last_price": 1400.0},
    }

    result = inst.fetchKiteGtts("u1")

    # distance_pct=None for the degenerate zero-trigger case.
    assert result[0]["monitor"]["distance_pct"] is None
    # current_ltp still populated.
    assert result[0]["monitor"]["current_ltp"] == 1400.0


# ── 4: fetchKiteAlerts caching ───────────────────────────────────────────


def test_fetchKiteAlerts_fetches_on_first_call_and_caches():
    inst = _investment_service_stub()
    inst.StockService.kite_service.get_alerts.return_value = [
        {"uuid": "a1", "type": "simple"},
    ]

    first = inst.fetchKiteAlerts("u1")
    second = inst.fetchKiteAlerts("u1")

    assert first == second
    inst.StockService.kite_service.get_alerts.assert_called_once()


def test_fetchKiteAlerts_per_user_cache_isolation():
    inst = _investment_service_stub()
    inst.StockService.kite_service.get_alerts.side_effect = [
        [{"uuid": "a1"}],
        [{"uuid": "b1"}],
    ]

    a = inst.fetchKiteAlerts("user-A")
    b = inst.fetchKiteAlerts("user-B")

    assert a[0]["uuid"] == "a1"
    assert b[0]["uuid"] == "b1"
    assert inst.StockService.kite_service.get_alerts.call_count == 2


def test_fetchKiteAlerts_fetch_failure_evicts_stale():
    inst = _investment_service_stub()
    inst.StockService.kite_service.get_alerts.return_value = [
        {"uuid": "a1"}
    ]
    inst.fetchKiteAlerts("u1")

    inst._kite_alerts_cache.pop("u1", None)
    inst.StockService.kite_service.get_alerts.side_effect = RuntimeError(
        "Kite down"
    )

    with pytest.raises(RuntimeError):
        inst.fetchKiteAlerts("u1")

    assert "u1" not in inst._kite_alerts_cache


# ── 5: fetchKiteAlertHistory pass-through ────────────────────────────────


def test_fetchKiteAlertHistory_is_a_pass_through_no_cache():
    inst = _investment_service_stub()
    inst.StockService.kite_service.get_alert_history.return_value = [
        {"triggered_at": "2026-11-01T10:00:00"},
    ]

    first = inst.fetchKiteAlertHistory("u1", "uuid-xyz")
    second = inst.fetchKiteAlertHistory("u1", "uuid-xyz")

    assert first == second
    # NO cache on history (drill-down wants fresh) — 2 calls = 2 fetches.
    assert (
        inst.StockService.kite_service.get_alert_history.call_count == 2
    )
    # And both calls pass through the uuid verbatim.
    inst.StockService.kite_service.get_alert_history.assert_any_call(
        "u1", "uuid-xyz"
    )


# ── 6: Route registration drift guard ───────────────────────────────────


def test_bundle3_routes_registered_in_app():
    """The 3 new routes must appear in app.py's investmentRoutes.
    Drift guard: if a route is quietly renamed / dropped, the FE
    'Active monitors' section 404s silently."""
    src = _read_source("app.py")
    assert "'/kite/gtts'" in src
    assert "'/kite/alerts'" in src
    assert "'/kite/alerts/<alert_uuid>/history'" in src
    # And they bind to the correct controller methods.
    assert "fetchKiteGtts" in src
    assert "fetchKiteAlerts" in src
    assert "fetchKiteAlertHistory" in src


def test_bundle3_endpoints_defined_on_investmentsEP():
    """The 3 controller methods must still exist. Source grep (vs
    import) because controllers/investmentsEP.py pulls Firebase +
    Google — same reason test_kite_bundle2 uses the same pattern."""
    src = _read_source("controllers/investmentsEP.py")
    assert "def fetchKiteGtts(self):" in src
    assert "def fetchKiteAlerts(self):" in src
    assert "def fetchKiteAlertHistory(self, alert_uuid):" in src
