"""Monkey-patch nsepython.nse_eq to use the NextApi endpoint.

v8 (hq-wisp-3gs06, ak-m4r): bypasses Akamai's 403 on the legacy
/api/quote-equity endpoint by hitting /api/NextApi/apiClient/GetQuoteApi
instead. Per infra probe hq-wisp-k1s85: the same internal nsefetch HTTP
stack reaches NextApi cleanly for 6/6 target symbols including ETFs —
no client swap needed, only the URL inside nse_eq is hardcoded broken.

This module re-shapes the NextApi response back to the legacy
/api/quote-equity payload that StocksService consumers expect.

PROD imports `nsepythonserver`, dev imports `nsepython` — the patch
applies to whichever (or both) is importable at runtime, so the wiring
in services/StocksService.py needs no ENV branching.

Apply by importing this module ONCE at startup, before the first
nse_eq() call. Revert by deleting this module + its import line in
services/StocksService.py once upstream fixes the URL.

Audit notes (services/, utils/):
  - Only nse_eq consumer: services/StocksService.py:401 (findSecurity)
  - Only response-shape consumer: utils/GenericUtils.py:49
    (fetchStockRates) which reads:
      info.{symbol, companyName, industry}        ← we set symbol
      priceInfo.{lastPrice, change, pChange, previousClose}  ← covered
      priceInfo.{open, close}                     ← default to 0
      priceInfo.intraDayHighLow.{max, min}        ← default to 0
  - `info.symbol` is load-bearing: Base_MSN.calculateStockRates keys
    `rateDictionary[rate_card['symbol']]`, so omitting it would
    collapse all rates onto a single empty-string key. The adapter
    sets info.symbol from the requested symbol arg.
  - companyName / industry / open / close / dayHigh / dayLow flow
    only to UI display through msn_summary_schema; defaulting to ''
    or 0 is honest about NextApi not exposing those fields, and
    portfolio math (Base_MSN.calculateProfitAndCurrentValue) only
    reads lastPrice.
"""
import importlib
import logging

log = logging.getLogger(__name__)

_BASE_URL = (
    "https://www.nseindia.com/api/NextApi/apiClient/GetQuoteApi"
    "?functionName=getSymbolData&marketType=N&series=EQ&symbol="
)


def _make_patched(rahu_module):
    """Build a patched nse_eq bound to a specific library's rahu submodule.

    Each library (nsepython / nsepythonserver) ships its own rahu with its
    own nsefetch + cookie state. We close over the right one per library
    so patches don't cross-contaminate.
    """
    nsefetch = getattr(rahu_module, "nsefetch", None)
    nsesymbolpurify = getattr(rahu_module, "nsesymbolpurify", None)

    def _patched_nse_eq(symbol):
        """Drop-in replacement for nse_eq using NextApi + shape adapter.

        Returns a dict shaped like legacy /api/quote-equity so the
        downstream utils.GenericUtils.fetchStockRates consumer keeps
        working. On any failure (empty equityResponse, network error,
        etc.) returns {} so StocksService.findSecurity treats it as
        "fetch failed" via the existing falsy check.
        """
        try:
            s = nsesymbolpurify(symbol) if nsesymbolpurify else symbol
            raw = nsefetch(_BASE_URL + s) if nsefetch else None
            er = (raw or {}).get("equityResponse") or []
            if not er:
                log.warning(
                    "nse_patch: empty equityResponse for %s", symbol
                )
                return {}
            eq = er[0] if isinstance(er, list) else er
            ob = eq.get("orderBook") or {}
            md = eq.get("metaData") or {}
            return {
                # info.symbol is load-bearing (Base_MSN.calculateStockRates
                # keys on it). We set it from the requested symbol so the
                # dictionary keying stays correct even if NextApi omits it.
                "info": {
                    "symbol": symbol,
                    "companyName": md.get("companyName", "") or "",
                    "industry": md.get("industry", "") or "",
                },
                "priceInfo": {
                    "lastPrice": ob.get("lastPrice"),
                    "previousClose": md.get("previousClose"),
                    "change": md.get("change"),
                    "pChange": md.get("pChange"),
                },
                "error": {},
            }
        except Exception as ex:
            log.error(
                "nse_patch: nse_eq failed for %s: %s", symbol, ex
            )
            return {}

    return _patched_nse_eq


def _try_patch(library_name):
    """Patch one library if importable. Returns True on success."""
    try:
        lib = importlib.import_module(library_name)
    except ImportError:
        return False
    try:
        rahu = importlib.import_module(f"{library_name}.rahu")
    except ImportError:
        # Some library variants may not expose rahu as a submodule; fall
        # back to using attribute access on the top-level module so we
        # at least try to bind nsefetch / nsesymbolpurify off it.
        rahu = lib
    patched = _make_patched(rahu)
    # Patch both surface points the consumer might hit:
    #   - nsepython.nse_eq (direct module attribute)
    #   - nsepython.rahu.nse_eq (deeper attribute path some consumers use)
    try:
        rahu.nse_eq = patched
    except AttributeError:
        pass
    try:
        lib.nse_eq = patched
    except AttributeError:
        pass
    log.info("nse_patch: patched %s.nse_eq to NextApi endpoint", library_name)
    return True


# Apply at import time. Both libraries are attempted because dev uses
# `nsepython` and PROD uses `nsepythonserver`; whichever (or both) is
# installed gets patched. If neither imports successfully the consumer
# will fail at its own `import nsepython` site — no point exploding here.
_patched_libraries = []
for _name in ("nsepython", "nsepythonserver"):
    if _try_patch(_name):
        _patched_libraries.append(_name)

if not _patched_libraries:
    log.warning(
        "nse_patch: neither nsepython nor nsepythonserver importable; "
        "patch did not apply. StocksService will hit unpatched library "
        "at its own import site."
    )
