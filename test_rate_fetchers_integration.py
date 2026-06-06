#!/usr/bin/env python3
"""
Integration test for Family B v2 rate-fetcher URLs.

This is a LIGHTWEIGHT integration test — it verifies each upstream URL is
reachable and the page body contains rate-relevant signal text. It does
NOT run the SDK extraction step (that needs a Claude API key and costs
real money to verify end-to-end).

Run with: `python3 test_rate_fetchers_integration.py`
Exit 0 = all URLs reachable and contain expected signal.
Exit 1 = at least one URL is unreachable or missing signal.

Use this BEFORE mayor pushes to red-flag a bad URL choice. Cheap, fast,
and doesn't burn API quota.

The full end-to-end test (fetch + sonnet extract → assert structured
output) happens on the next scheduled rate.gold cycle in prod (~5h after
deploy). Infra greps `agent_run agent=rate.gold turns≥1 tools_called=0
status=ok` and checks the persisted GOLDRATE.json.

Refs: hq-wisp-bommx (Family B v2).
"""

import os
import sys
import time
import requests

sys.path.insert(0, os.path.dirname(os.path.abspath(__file__)))


# URLs are duplicated here from the fetcher source rather than imported, so
# this script can run standalone in the worktree without dragging in the
# full project import chain (sqlalchemy, flask, models, etc.). If a fetcher
# URL changes, update both places. The unit tests cover the URL-shape
# contract on the source side.
#
# Authoritative sources:
#   services/tasks/SetIBJAGoldRate.py::GOLD_URL
#   services/tasks/SetEPFRate.py::EPF_URL
#   services/tasks/SetPpfRate.py::PPF_URL
#   utils/AIHelper.py::_IPO_URL
_GOLD_URL = "https://ibjarates.com/"
_EPF_URL = "https://en.wikipedia.org/wiki/Employees%27_Provident_Fund_Organisation"
_PPF_URL = "https://en.wikipedia.org/wiki/Public_Provident_Fund_(India)"
_IPO_URL = "https://www.chittorgarh.com/report/mainboard-ipo-list-in-india/82/"


# Per-URL probes: (label, url, list_of_signal_substrings_we_expect_to_see)
# Substrings are case-insensitive, lowercased before search. Any one match
# counts. Generous to survive minor page redesigns.
_PROBES = [
    ("rate.gold (IBJA)", _GOLD_URL, ["gold", "silver", "ibja"]),
    ("rate.epf (Wikipedia EPFO)", _EPF_URL, [
        "epfo", "employees' provident fund", "interest rate",
    ]),
    ("rate.ppf (Wikipedia PPF)", _PPF_URL, [
        "public provident fund", "interest rate", "ppf",
    ]),
    ("stocks.ipo (Chittorgarh IPO list)", _IPO_URL, [
        "ipo", "issue price", "listing",
    ]),
]


_DEFAULT_HEADERS = {
    "User-Agent": (
        "Mozilla/5.0 (X11; Linux x86_64) AppleWebKit/537.36 "
        "(KHTML, like Gecko) Chrome/124.0 Safari/537.36"
    ),
    "Accept": "text/html,application/xhtml+xml,application/xml;q=0.9,*/*;q=0.8",
}


def probe(label, url, signals, timeout=20):
    print(f"\n[{label}]")
    print(f"  URL: {url}")
    t0 = time.monotonic()
    try:
        resp = requests.get(url, headers=_DEFAULT_HEADERS, timeout=timeout)
    except requests.RequestException as e:
        print(f"  FAIL — HTTP error: {e}")
        return False
    dt = int((time.monotonic() - t0) * 1000)
    print(f"  status={resp.status_code}  latency={dt}ms  bytes={len(resp.text)}")

    if resp.status_code != 200:
        print(f"  FAIL — non-200 status")
        return False

    body_lower = resp.text.lower()
    matched = [s for s in signals if s.lower() in body_lower]
    missing = [s for s in signals if s.lower() not in body_lower]
    print(f"  matched signals: {matched}")
    if missing:
        print(f"  missing signals: {missing}")
    if not matched:
        print("  FAIL — no signal substrings found in body")
        return False
    print("  PASS — at least one signal substring found")
    return True


def main():
    print("=" * 70)
    print("Family B v2 integration test — upstream URL reachability + signal")
    print("=" * 70)

    results = []
    for label, url, signals in _PROBES:
        ok = probe(label, url, signals)
        results.append((label, ok))

    print("\n" + "=" * 70)
    passed = sum(1 for _, ok in results if ok)
    failed = len(results) - passed
    print(f"Result: {passed}/{len(results)} URLs reachable with signal")
    for label, ok in results:
        print(f"  {'PASS' if ok else 'FAIL'}  {label}")
    print("=" * 70)
    return 0 if failed == 0 else 1


if __name__ == "__main__":
    sys.exit(main())
