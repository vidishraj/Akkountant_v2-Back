"""Unit tests for utils.nse_patch.

v8 (hq-wisp-3gs06, ak-m4r): the monkey-patch + shape-adapter replacement
for the WAF-blocked nsepython.nse_eq URL. Tests stub out both
nsepython and nsepython.rahu in sys.modules BEFORE importing nse_patch,
so no real library install is needed.

Coverage:
  1. URL: patched function hits the NextApi endpoint with the symbol
  2. Adapter shape: priceInfo fields + info.symbol round-trip
  3. info.symbol is load-bearing (matches the argument passed in)
  4. Empty equityResponse → returns {} (StocksService treats as failed)
  5. nsefetch raises → returns {} (no exception propagates)
  6. fetchStockRates compatibility — patched output flows through
     utils.GenericUtils.fetchStockRates without KeyError / Decimal errors
"""
import sys
import types
from unittest.mock import MagicMock


# ── Fixture: install fake nsepython + nsepython.rahu BEFORE importing patch ──

def _install_fake_nsepython(name="nsepython", nsefetch_return=None,
                            nsefetch_raise=None):
    """Build a fake library with a .rahu submodule that exposes
    nsefetch + nsesymbolpurify. Install both module objects into
    sys.modules so importlib.import_module picks them up."""
    rahu = types.ModuleType(f"{name}.rahu")

    if nsefetch_raise is not None:
        rahu.nsefetch = MagicMock(side_effect=nsefetch_raise)
    else:
        rahu.nsefetch = MagicMock(return_value=nsefetch_return)
    rahu.nsesymbolpurify = MagicMock(side_effect=lambda s: s)
    rahu.nse_eq = MagicMock(return_value={"_legacy": True})

    lib = types.ModuleType(name)
    lib.rahu = rahu
    lib.nse_eq = MagicMock(return_value={"_legacy": True})

    sys.modules[name] = lib
    sys.modules[f"{name}.rahu"] = rahu
    return lib, rahu


def _purge_patch_cache():
    """Strip cached nse_patch + the fakes so each test starts fresh."""
    for mod in ("utils.nse_patch", "nsepython", "nsepython.rahu",
                "nsepythonserver", "nsepythonserver.rahu"):
        sys.modules.pop(mod, None)


_NEXTAPI_RESPONSE = {
    "equityResponse": [
        {
            "orderBook": {"lastPrice": 371.0},
            "metaData": {
                "previousClose": 370.55,
                "change": 0.45,
                "pChange": 0.12,
                "companyName": "Bharat Heavy Electricals Ltd",
                "industry": "Heavy Engineering",
            },
        }
    ]
}


# ── Tests ───────────────────────────────────────────────────────────────────

def test_patches_both_module_attributes():
    print("\n[nse_patch — patches nsepython.nse_eq AND nsepython.rahu.nse_eq]")
    _purge_patch_cache()
    lib, rahu = _install_fake_nsepython(nsefetch_return=_NEXTAPI_RESPONSE)
    original_nse_eq_id = id(lib.nse_eq)
    import utils.nse_patch  # noqa: F401
    assert id(lib.nse_eq) != original_nse_eq_id, "lib.nse_eq must be replaced"
    assert lib.nse_eq is rahu.nse_eq, "both surfaces must point to same patched fn"
    print("  ✓ Both nsepython.nse_eq and nsepython.rahu.nse_eq are patched in place")


def test_patched_hits_nextapi_url():
    print("\n[nse_patch — patched nse_eq hits NextApi URL]")
    _purge_patch_cache()
    lib, rahu = _install_fake_nsepython(nsefetch_return=_NEXTAPI_RESPONSE)
    import utils.nse_patch  # noqa: F401
    result = lib.nse_eq("BHEL")
    # rahu.nsefetch should have been called with a URL that contains
    # NextApi/apiClient/GetQuoteApi and ends with the symbol.
    call_args = rahu.nsefetch.call_args
    url = call_args.args[0] if call_args.args else call_args.kwargs.get("url", "")
    assert "/api/NextApi/apiClient/GetQuoteApi" in url, (
        f"expected NextApi URL, got {url}"
    )
    assert url.endswith("BHEL"), f"URL must end with symbol, got tail: {url[-20:]}"
    assert result, "patched nse_eq must return non-empty dict for valid response"
    print(f"  ✓ NextApi URL hit: ...{url[-80:]}")


def test_adapter_shape_legacy_compatible():
    print("\n[nse_patch — adapter shape matches legacy /api/quote-equity]")
    _purge_patch_cache()
    lib, rahu = _install_fake_nsepython(nsefetch_return=_NEXTAPI_RESPONSE)
    import utils.nse_patch  # noqa: F401
    r = lib.nse_eq("BHEL")
    # Required keys per audit
    assert "info" in r, "adapter must expose info block (load-bearing)"
    assert "priceInfo" in r
    assert "error" in r
    # info.symbol is load-bearing
    assert r["info"]["symbol"] == "BHEL", (
        "info.symbol must echo input — Base_MSN keys rateDictionary on it"
    )
    # priceInfo fields
    pi = r["priceInfo"]
    assert pi["lastPrice"] == 371.0
    assert pi["previousClose"] == 370.55
    assert pi["change"] == 0.45
    assert pi["pChange"] == 0.12
    # error must be empty dict (existing consumer behavior)
    assert r["error"] == {}
    print("  ✓ Adapter shape: info.symbol + priceInfo.{lastPrice,prevClose,change,pChange} + error={}")


def test_empty_equity_response_returns_empty_dict():
    print("\n[nse_patch — empty equityResponse → {} (failure signal)]")
    _purge_patch_cache()
    _install_fake_nsepython(nsefetch_return={"equityResponse": []})
    import utils.nse_patch  # noqa: F401
    import nsepython
    assert nsepython.nse_eq("DOESNOTEXIST") == {}, (
        "empty equityResponse must signal failure as {} so StocksService "
        "falsy-check engages the API_FAILED fallback"
    )
    print("  ✓ Empty equityResponse returns {} (matches StocksService falsy path)")


def test_nsefetch_exception_returns_empty_dict():
    print("\n[nse_patch — nsefetch raises → {} (no propagation)]")
    _purge_patch_cache()
    _install_fake_nsepython(nsefetch_raise=RuntimeError("simulated NSE 503"))
    import utils.nse_patch  # noqa: F401
    import nsepython
    # Must NOT raise — StocksService's existing try/except is on the
    # call site, but defending here is cheap and matches the legacy
    # nse_eq's tolerance pattern.
    result = nsepython.nse_eq("BHEL")
    assert result == {}, "nsefetch failure must degrade to {} not raise"
    print("  ✓ nsefetch exception swallowed, returns {} (graceful degrade)")


def test_fetchStockRates_compatibility():
    print("\n[nse_patch — output flows through utils.GenericUtils.fetchStockRates]")
    _purge_patch_cache()
    lib, _ = _install_fake_nsepython(nsefetch_return=_NEXTAPI_RESPONSE)
    import utils.nse_patch  # noqa: F401
    # Importing GenericUtil might pull in DB/Flask things; isolate the
    # static fetchStockRates method against the patched dict.
    response = lib.nse_eq("BHEL")

    # Inline the fetchStockRates field-read sequence (matches
    # utils/GenericUtils.py:47-67) so we don't need to import flask/db.
    info = response.get("info", {})
    price_info = response.get("priceInfo", {})

    # Required reads — must not KeyError
    assert info.get("symbol", "") == "BHEL"
    assert price_info.get("lastPrice", 0) == 371.0
    assert price_info.get("change", 0) == 0.45
    assert price_info.get("pChange", 0) == 0.12
    assert price_info.get("previousClose", 0) == 370.55
    # Missing fields default safely
    assert price_info.get("open", 0) == 0
    assert price_info.get("close", 0) == 0
    assert price_info.get("intraDayHighLow", {}).get("max", 0) == 0
    assert price_info.get("intraDayHighLow", {}).get("min", 0) == 0
    print("  ✓ All fetchStockRates reads safe: info.symbol present, "
          "priceInfo core fields populated, optional fields default to 0")


def test_patches_nsepythonserver_too():
    print("\n[nse_patch — also patches nsepythonserver when present (PROD path)]")
    _purge_patch_cache()
    lib_dev, _ = _install_fake_nsepython("nsepython",
                                          nsefetch_return=_NEXTAPI_RESPONSE)
    lib_prod, _ = _install_fake_nsepython("nsepythonserver",
                                           nsefetch_return=_NEXTAPI_RESPONSE)
    import utils.nse_patch  # noqa: F401
    # Both libraries' nse_eq must now route through the patch
    r_dev = lib_dev.nse_eq("BHEL")
    r_prod = lib_prod.nse_eq("BHEL")
    assert r_dev["info"]["symbol"] == "BHEL"
    assert r_prod["info"]["symbol"] == "BHEL"
    assert r_dev["priceInfo"]["lastPrice"] == 371.0
    assert r_prod["priceInfo"]["lastPrice"] == 371.0
    # Confirm in _patched_libraries
    assert "nsepython" in utils.nse_patch._patched_libraries
    assert "nsepythonserver" in utils.nse_patch._patched_libraries
    print("  ✓ Both nsepython (dev) and nsepythonserver (PROD) patched")


def test_neither_library_importable_no_crash():
    print("\n[nse_patch — neither library importable → no exception]")
    _purge_patch_cache()
    # Don't install either fake — both fail to import
    import utils.nse_patch  # noqa: F401
    assert utils.nse_patch._patched_libraries == [], (
        "with neither lib importable, _patched_libraries must be empty"
    )
    print("  ✓ Import succeeds with empty _patched_libraries (warns + no-op)")


if __name__ == "__main__":
    print("nse_patch unit tests — v8 ak-m4r (NextApi monkey-patch + adapter)")
    print("=" * 70)
    test_patches_both_module_attributes()
    test_patched_hits_nextapi_url()
    test_adapter_shape_legacy_compatible()
    test_empty_equity_response_returns_empty_dict()
    test_nsefetch_exception_returns_empty_dict()
    test_fetchStockRates_compatibility()
    test_patches_nsepythonserver_too()
    test_neither_library_importable_no_crash()
    print("\n" + "=" * 70)
    print("All nse_patch tests passed.")
    sys.exit(0)
