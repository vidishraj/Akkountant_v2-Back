"""
Standalone AI helper for fetching IPO allotment prices for Indian stocks.

v2 architecture (post-hq-wisp-bommx): fetch a known IPO listing page via
`requests`, then ask sonnet to extract per-stock data using `output_format`
JSON schema. Replaces v1's SDK-driven `WebSearch` + `WebFetch` shape, which
shared the same empty-TextBlocks failure mode as the rate fetchers
(`turns=0 tools_called=0`).

The caller (`services.StocksService._lookup_ipo_prices`) passes a `prompt`
listing the symbols/ISINs it wants prices for. We pass that list through to
sonnet as the `extra_context`, alongside the fetched IPO listing page body,
and ask sonnet to look up each entry.

Refs: hq-wisp-bommx (Family B v2 architecture directive).
"""

from __future__ import annotations  # PEP 604 (`dict | None`) on 3.9 worktree env

from utils.logger import Logger
from utils.web_extract import fetch_and_extract

_logger = Logger(__name__).get_logger()


# Upstream URL for Indian IPO list with allotment prices. chittorgarh.com
# maintains a comprehensive table of mainboard IPOs with issue prices,
# listing dates, and listing-day gains, going back ~15 years. It's the
# most-cited source in Indian equity reporting and rarely changes URL
# structure.
_IPO_URL = "https://www.chittorgarh.com/report/mainboard-ipo-list-in-india/82/"


_IPO_SYSTEM_PROMPT = """You are a data extraction assistant. You will receive
the HTML of an Indian IPO listing page that contains a table of mainboard
IPOs with allotment / issue prices and listing dates.

The user message will include a list of stocks (symbol + ISIN + first sell
date) the caller needs IPO allotment data for. For each requested stock,
find the matching row in the IPO list and return its allotment price and
date.

Rules:
- `allotment_price` = the price at which retail investors received shares
  in the IPO (issue price). If the stock was received via a corporate
  action (demerger, bonus, etc.) rather than an IPO and you cannot find an
  issue price on the page, use the listing-day price if available, else
  skip that stock.
- `allotment_date` = IPO allotment date or listing date in YYYY-MM-DD
  format.
- `source` = a short note like "IPO 2021-03-15" or "demerger from XYZ".
- Match stocks by ISIN preferentially; symbol as a secondary signal.
- If you cannot find a stock in the page text, omit it from `results`
  (do not invent values).
- Return only stocks you found. Empty `results` is a valid output if
  none of the requested stocks are on the page."""


_IPO_SCHEMA = {
    "type": "object",
    "properties": {
        "results": {
            "type": "array",
            "items": {
                "type": "object",
                "properties": {
                    "symbol": {"type": "string"},
                    "isin": {"type": "string"},
                    "allotment_price": {"type": "number"},
                    "allotment_date": {"type": "string"},
                    "source": {"type": "string"},
                },
                "required": ["isin", "allotment_price", "allotment_date"],
            },
        }
    },
    "required": ["results"],
}


def fetch_via_ai(prompt: str, system: str = None) -> tuple[dict | None, str]:
    """
    Fetch IPO allotment data for the stocks listed in `prompt`.

    Args:
        prompt: Caller-provided context listing the requested stocks
            (symbols, ISINs, first sell dates). Passed verbatim to sonnet as
            extra context alongside the fetched IPO page body.
        system: Optional override of the extraction system prompt. Defaults
            to the IPO-specific prompt above. Mostly here for backwards
            compatibility with the v1 signature; new call sites should not
            pass this.

    Returns:
        (parsed_dict, "") on success — schema-validated, shape
        `{"results": [{"symbol", "isin", "allotment_price", ...}, ...]}`.
        (None, detail) on any failure — fetch error, sonnet error, or
        structured-output missing.
    """
    system_prompt = system or _IPO_SYSTEM_PROMPT

    result, err = fetch_and_extract(
        url=_IPO_URL,
        system_prompt=system_prompt,
        schema=_IPO_SCHEMA,
        agent="stocks.ipo",
        extra_context=prompt,
    )

    if err:
        _logger.error(f"stocks.ipo fetch_and_extract failed: {err}")
        return None, err
    return result, ""
