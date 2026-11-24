#!/usr/bin/env python3
"""ak-rcg1 (ak-ofir Phase 2B): one-shot backfill for past-dated
InvestmentSnapshot rows so the Investments trend chart fills back
further than the daemon start date.

## Shape

Backfill script, NOT a scheduler task. The daemon
(``investmentSnapshotTask``) is forward-only; this runs ONCE to
seed history, after which the daemon adds today's row daily.

Target window = max(cache coverage, holdings history). For each
day D in the window and each asset_type ∈ {Stocks, MF, NPS, EPF,
PPF, Gold}, reconstructs holdings AS-OF D, looks up historical
value via the appropriate cache, and UPSERTs one
``investmentSnapshots`` row keyed on (D, user, investment_type).

## Dry-run discipline (per bead)

Default mode is DRY-RUN — prints what WOULD be upserted (N rows
per asset_type × target_date), NO DB writes. Only after Overseer
reviews dry-run output + approves → re-run with ``--commit``.

## Usage

    # Dry-run (default): preview what would land, zero DB writes.
    python3 -I scripts/synthesize_historical_snapshots.py \\
        --user-id <firebase_user_id> \\
        [--days-back 365]

    # Commit: actually UPSERT. Requires --commit AND --assume-yes.
    python3 -I scripts/synthesize_historical_snapshots.py \\
        --user-id <firebase_user_id> \\
        --days-back 365 \\
        --commit --assume-yes

The ``--assume-yes`` safety belt is distinct from ``--commit`` so
that a single typo (missing ``--commit``) can't accidentally
commit; an operator must pass BOTH.

## Idempotency

Pure MySQL ``INSERT ... ON DUPLICATE KEY UPDATE`` on
``(date, user, investment_type)`` unique constraint. Re-running
over the same window with the same inputs produces the same
rows — safe to retry on partial failure.

## What's NOT synthesized

* Dates where a slice's coverage floor isn't met (< 50% of held
  buyIds have a price lookup for the day) → OMITTED for that
  slice on that day. Prevents under-counted trend-chart dips.
* Dates where a user had no holdings of a type → OMITTED (zero
  row would be semantically wrong).
* Dates where gold rate is absent from goldHistoricalRates →
  Gold slice OMITTED (per Overseer opt-in gold scope).
* The target_date is CAPPED at (daemon_start_date - 1) by default
  — we don't want to overwrite the live daemon's forward-only
  rows with historical approximations. Operator can pass
  ``--allow-daemon-overlap`` to disable this cap for a specific
  re-sync use case.

## Blast radius

Single table (``investmentSnapshots``). Script is user-id-scoped —
multi-user call would need a loop (out of scope today; single-user
app). All inserts use the same UK the daemon already enforces, so
even a partial commit is consistent.
"""

from __future__ import annotations

import os  # noqa: E402
# ak-n3lf: importing app builds the app at import time (create_all DDL,
# seed job rows, scheduler threads that write to prod). A one-off script
# needs none of that: both gates are set before ANY project import (only
# when run as a script, so importing it in a test leaks nothing).
# Usage: on a NEW database, start app.service once first (it creates the
# tables); this script never runs schema setup.
if __name__ == "__main__":
    os.environ["AK_SKIP_SCHEDULERS"] = "1"
    os.environ["AK_SKIP_DB_INIT"] = "1"

import argparse
import os
import sys
from datetime import date as date_cls, datetime, timedelta, timezone
from decimal import Decimal


# Allow running from anywhere.
_BACKEND_ROOT = os.path.abspath(
    os.path.join(os.path.dirname(__file__), os.pardir)
)
if _BACKEND_ROOT not in sys.path:
    sys.path.insert(0, _BACKEND_ROOT)


IST = timezone(timedelta(hours=5, minutes=30))

# Default window for the backfill — matches Phase-2A cold-start
# window (365d) and ak-4o41's stock candle cold-start.
_DEFAULT_DAYS_BACK = 365


def _parse_args():
    p = argparse.ArgumentParser(
        description=(
            "Backfill past-dated investmentSnapshots rows "
            "(ak-rcg1 Phase 2B)."
        ),
    )
    p.add_argument("--user-id", required=True,
                   help="Firebase user_id to synthesize for.")
    p.add_argument(
        "--days-back", type=int, default=_DEFAULT_DAYS_BACK,
        help=(
            f"Days of history to synthesize ending at today's date "
            f"minus one (daemon-write guard). Default: "
            f"{_DEFAULT_DAYS_BACK}."
        ),
    )
    p.add_argument(
        "--commit", action="store_true",
        help=(
            "Perform the UPSERT. Omit for dry-run. Must be paired "
            "with --assume-yes to actually write."
        ),
    )
    p.add_argument(
        "--assume-yes", action="store_true",
        help=(
            "Safety belt — distinct from --commit. Both flags "
            "required to actually write. A single-typo guard."
        ),
    )
    p.add_argument(
        "--allow-daemon-overlap", action="store_true",
        help=(
            "Allow the target range to include today (overlaps the "
            "daemon's forward-only writes). Default is to cap at "
            "today - 1."
        ),
    )
    return p.parse_args()


def _iter_target_dates(today, days_back, allow_daemon_overlap):
    """Yield target dates from (today - days_back) through
    (today - 1) inclusive by default, or through today if
    ``--allow-daemon-overlap`` is set."""
    end = today if allow_daemon_overlap else today - timedelta(days=1)
    start = today - timedelta(days=days_back)
    cur = start
    while cur <= end:
        yield cur
        cur = cur + timedelta(days=1)


def _load_msn_events(session, user_id, security_type):
    """Pull SecurityTransactions for one securityType as
    TxEvent tuples ready for the synthesizer."""
    from models.securityTransactions import SecurityTransactions
    from models.purchasedSecurities import PurchasedSecurities
    from services.historical_snapshot_synthesizer import TxEvent

    # Join to PurchasedSecurities so we have each tx's buy_code
    # (SecurityTransactions stores buy_id but the buy_code —
    # tradingsymbol / scheme_code / NPS scheme_id — lives on
    # PurchasedSecurities.securityCode).
    rows = (
        session.query(
            SecurityTransactions.buyId,
            SecurityTransactions.date,
            SecurityTransactions.quant,
            SecurityTransactions.price,
            SecurityTransactions.transactionType,
            PurchasedSecurities.securityCode,
        )
        .join(
            PurchasedSecurities,
            SecurityTransactions.buyId == PurchasedSecurities.buyID,
        )
        .filter(
            SecurityTransactions.userID == user_id,
            SecurityTransactions.securityType == security_type,
        )
        .all()
    )
    return [
        TxEvent(
            buy_id=r[0],
            tx_date=r[1],
            qty=Decimal(str(r[2])),
            price=Decimal(str(r[3])),
            tx_type=str(r[4]),
            buy_code=r[5],
        )
        for r in rows
    ]


def _build_stock_price_lookup(session, user_id):
    """Resolve buyCode → instrument_token via SetKiteStockDetails
    dump, then preload candles for held tokens.

    Returns a function ``(buy_code, target_date) -> Optional[Decimal]``.
    """
    from models.stockHistoricalCandle import StockHistoricalCandle
    from services.JsonDownloadService import JsonDownloadService

    json_svc = JsonDownloadService()
    try:
        stock_list = json_svc.getStockList()
    except Exception:
        stock_list = {"data": []}
    symbol_to_token = {}
    for row in stock_list.get("data", []):
        code = row.get("stockCode")
        tok = row.get("instrument_token")
        if code and tok is not None:
            symbol_to_token[code] = int(tok)

    # Preload candles — one query up-front beats N per-day lookups.
    tokens = list(set(symbol_to_token.values()))
    candles = {}
    if tokens:
        rows = (
            session.query(
                StockHistoricalCandle.instrument_token,
                StockHistoricalCandle.trade_date,
                StockHistoricalCandle.close,
            )
            .filter(StockHistoricalCandle.instrument_token.in_(tokens))
            .all()
        )
        for tok, d, close in rows:
            candles[(tok, d)] = Decimal(str(close))

    def lookup(buy_code, target_date):
        tok = symbol_to_token.get(buy_code)
        if tok is None:
            return None
        return candles.get((tok, target_date))

    return lookup


def _build_mf_price_lookup(session):
    """Preload mfHistoricalNavs into a (scheme_code, date)
    dict."""
    from models.mfHistoricalNav import MFHistoricalNav
    navs = {}
    for scheme_code, nav_date, nav in session.query(
        MFHistoricalNav.scheme_code,
        MFHistoricalNav.nav_date,
        MFHistoricalNav.nav,
    ).all():
        navs[(scheme_code, nav_date)] = Decimal(str(nav))

    def lookup(buy_code, target_date):
        return navs.get((buy_code, target_date))
    return lookup


def _build_nps_price_lookup(session):
    """Preload npsHistoricalNavs."""
    from models.npsHistoricalNav import NPSHistoricalNav
    navs = {}
    for scheme_id, nav_date, nav in session.query(
        NPSHistoricalNav.scheme_id,
        NPSHistoricalNav.nav_date,
        NPSHistoricalNav.nav,
    ).all():
        navs[(scheme_id, nav_date)] = Decimal(str(nav))

    def lookup(buy_code, target_date):
        return navs.get((buy_code, target_date))
    return lookup


def _load_epg_deposits(session, user_id, security_type):
    """Pull DepositSecurities for EPF or PPF as DepositEvent list."""
    from models.depositSecurities import DepositSecurities
    from services.historical_snapshot_synthesizer import DepositEvent

    rows = (
        session.query(DepositSecurities)
        .filter(
            DepositSecurities.userID == user_id,
            DepositSecurities.securityType == security_type,
        )
        .order_by(DepositSecurities.date.asc())
        .all()
    )
    return [
        DepositEvent(
            buy_id=r.buyID,
            deposit_date=r.date,
            employee_amount=Decimal(str(r.depositAmount)),
            employer_amount=(
                Decimal(str(r.employerAmount))
                if r.employerAmount is not None else None
            ),
        )
        for r in rows
    ]


def _epf_deposit_events_to_rows(events):
    """Shim DepositEvent → a shape calculateTransactionTable
    accepts. The service method expects objects with .date /
    .depositDescription / .depositAmount / .employerAmount
    attributes."""
    class _Row:
        def __init__(self, evt):
            self.buyID = evt.buy_id
            self.date = evt.deposit_date
            self.depositDescription = ""
            self.depositAmount = evt.employee_amount
            self.employerAmount = evt.employer_amount

    return [_Row(e) for e in events]


def _load_gold_holdings(session, user_id):
    """Pull DepositSecurities(Gold) joined with GoldDetails."""
    from models.depositSecurities import DepositSecurities
    from models.GoldDetails import GoldDetails
    from services.historical_snapshot_synthesizer import GoldHolding

    rows = (
        session.query(
            DepositSecurities.buyID,
            DepositSecurities.date,
            DepositSecurities.depositAmount,
            GoldDetails.quantity,
            GoldDetails.goldType,
        )
        .join(GoldDetails, DepositSecurities.buyID == GoldDetails.buyID)
        .filter(
            DepositSecurities.userID == user_id,
            DepositSecurities.securityType == "Gold",
        )
        .all()
    )
    holdings = []
    deposit_amount_map = {}
    for buy_id, d, deposit_amount, qty, gold_type in rows:
        holdings.append(
            GoldHolding(
                buy_id=buy_id,
                deposit_date=d,
                qty_grams=Decimal(str(qty)),
                gold_type=str(gold_type),
            )
        )
        deposit_amount_map[buy_id] = Decimal(str(deposit_amount))
    return holdings, deposit_amount_map


def _build_gold_rate_lookup(session):
    """Preload goldHistoricalRates into a {date: rate_per_gram}."""
    from models.goldHistoricalRate import GoldHistoricalRate
    rates = {}
    for d, rate, _src in session.query(
        GoldHistoricalRate.date,
        GoldHistoricalRate.rate_per_gram,
        GoldHistoricalRate.source,
    ).all():
        rates[d] = Decimal(str(rate))
    return rates


def _synthesize_all(session, user_id, target_dates):
    """Run synthesis for all (date, asset_type) combinations.

    Returns a list of row dicts ready for UPSERT. Logs skip reasons
    to stdout in dry-run mode so the operator sees why a slice was
    omitted."""
    from services.historical_snapshot_synthesizer import (
        synthesize_msn_slice, synthesize_epg_deposit_slice,
        synthesize_gold_slice, TYPE_LABEL_STOCKS, TYPE_LABEL_MF,
        TYPE_LABEL_NPS, TYPE_LABEL_EPF, TYPE_LABEL_PPF,
    )
    from services.EPFService import EPFService
    from services.PPFService import PPFService

    # Load events once, reuse across all target_dates.
    stock_events = _load_msn_events(session, user_id, "Stocks")
    mf_events = _load_msn_events(session, user_id, "Mutual_Funds")
    nps_events = _load_msn_events(session, user_id, "NPS")
    epf_deposits = _load_epg_deposits(session, user_id, "EPF")
    ppf_deposits = _load_epg_deposits(session, user_id, "PF")
    gold_holdings, gold_deposit_amount_map = _load_gold_holdings(
        session, user_id,
    )

    stock_price = _build_stock_price_lookup(session, user_id)
    mf_price = _build_mf_price_lookup(session)
    nps_price = _build_nps_price_lookup(session)
    gold_rates = _build_gold_rate_lookup(session)

    epf_svc = EPFService()
    ppf_svc = PPFService()

    rows = []
    stats = {
        "Stocks": {"ok": 0, "skip": 0},
        "MF": {"ok": 0, "skip": 0},
        "NPS": {"ok": 0, "skip": 0},
        "EPF": {"ok": 0, "skip": 0},
        "PPF": {"ok": 0, "skip": 0},
        "Gold": {"ok": 0, "skip": 0},
    }

    for d in target_dates:
        # Stocks
        r = synthesize_msn_slice(
            target_date=d, user_id=user_id,
            investment_type=TYPE_LABEL_STOCKS,
            events=stock_events, price_lookup=stock_price,
        )
        if r:
            rows.append(r)
            stats["Stocks"]["ok"] += 1
        else:
            stats["Stocks"]["skip"] += 1

        # MF
        r = synthesize_msn_slice(
            target_date=d, user_id=user_id,
            investment_type=TYPE_LABEL_MF,
            events=mf_events, price_lookup=mf_price,
        )
        if r:
            rows.append(r)
            stats["MF"]["ok"] += 1
        else:
            stats["MF"]["skip"] += 1

        # NPS
        r = synthesize_msn_slice(
            target_date=d, user_id=user_id,
            investment_type=TYPE_LABEL_NPS,
            events=nps_events, price_lookup=nps_price,
        )
        if r:
            rows.append(r)
            stats["NPS"]["ok"] += 1
        else:
            stats["NPS"]["skip"] += 1

        # EPF
        r = synthesize_epg_deposit_slice(
            target_date=d, user_id=user_id,
            investment_type=TYPE_LABEL_EPF,
            deposits=epf_deposits,
            calc_transaction_table=lambda dl: (
                epf_svc.calculateTransactionTable(
                    _epf_deposit_events_to_rows(dl),
                )
            ),
        )
        if r:
            rows.append(r)
            stats["EPF"]["ok"] += 1
        else:
            stats["EPF"]["skip"] += 1

        # PPF
        r = synthesize_epg_deposit_slice(
            target_date=d, user_id=user_id,
            investment_type=TYPE_LABEL_PPF,
            deposits=ppf_deposits,
            calc_transaction_table=lambda dl: (
                ppf_svc.calculateTransactionTable(
                    _epf_deposit_events_to_rows(dl),
                )
            ),
        )
        if r:
            rows.append(r)
            stats["PPF"]["ok"] += 1
        else:
            stats["PPF"]["skip"] += 1

        # Gold
        r = synthesize_gold_slice(
            target_date=d, user_id=user_id,
            holdings=gold_holdings,
            gold_rate_per_gram_24k=gold_rates.get(d),
            deposit_amount_lookup=gold_deposit_amount_map.get,
        )
        if r:
            rows.append(r)
            stats["Gold"]["ok"] += 1
        else:
            stats["Gold"]["skip"] += 1

    return rows, stats


def _upsert_rows(session, rows):
    """MySQL UPSERT on (date, user, investment_type) UK."""
    from sqlalchemy.dialects.mysql import insert as mysql_insert
    from models.investmentSnapshot import InvestmentSnapshot

    if not rows:
        return 0
    stmt = mysql_insert(InvestmentSnapshot).values(rows)
    upsert = stmt.on_duplicate_key_update(
        total_invested=stmt.inserted.total_invested,
        current_value=stmt.inserted.current_value,
        profit=stmt.inserted.profit,
        profit_percent=stmt.inserted.profit_percent,
    )
    session.execute(upsert)
    return len(rows)


def main():
    args = _parse_args()

    if args.commit and not args.assume_yes:
        print(
            "[synthesize] --commit requires --assume-yes (safety "
            "belt).",
            file=sys.stderr,
        )
        return 2
    if args.assume_yes and not args.commit:
        print(
            "[synthesize] --assume-yes without --commit is a no-op; "
            "add --commit to write.",
            file=sys.stderr,
        )
        return 2

    today = datetime.now(IST).date()
    target_dates = list(
        _iter_target_dates(
            today, args.days_back, args.allow_daemon_overlap,
        )
    )
    print(
        f"[synthesize] user_id={args.user_id} "
        f"window={target_dates[0]}..{target_dates[-1]} "
        f"({len(target_dates)} days) "
        f"mode={'COMMIT' if args.commit else 'DRY-RUN'}"
    )

    try:
        from app import flask_app
    except Exception as exc:
        print(f"[synthesize] app import failed: {exc}", file=sys.stderr)
        return 3

    with flask_app.app_context():
        from flask import g
        g.db = flask_app.db
        session = flask_app.db.session

        rows, stats = _synthesize_all(
            session, args.user_id, target_dates,
        )

    # Report stats
    print("\n[synthesize] per-slice ok/skip counts:")
    for slice_name, counts in stats.items():
        print(
            f"  {slice_name:>8}: ok={counts['ok']:>4}  "
            f"skip={counts['skip']:>4}"
        )
    print(
        f"\n[synthesize] total rows synthesized: {len(rows)}"
    )

    # Preview sample
    preview_n = min(10, len(rows))
    if preview_n:
        print(f"\n[synthesize] preview (first {preview_n} rows):")
        for r in rows[:preview_n]:
            print(
                f"  {r['date']} {r['investment_type']:>8}: "
                f"invested=₹{r['total_invested']}  "
                f"value=₹{r['current_value']}  "
                f"profit=₹{r['profit']}  "
                f"({r['profit_percent']}%)"
            )

    if not args.commit:
        print(
            "\n[synthesize] DRY-RUN complete — no DB writes. Review "
            "the preview + stats, then re-run with --commit "
            "--assume-yes to persist."
        )
        return 0

    # Commit path
    with flask_app.app_context():
        from flask import g
        g.db = flask_app.db
        session = flask_app.db.session
        try:
            n = _upsert_rows(session, rows)
            session.commit()
        except Exception as exc:
            session.rollback()
            print(
                f"[synthesize] UPSERT failed; rolled back: {exc}",
                file=sys.stderr,
            )
            return 5

    print(
        f"\n[synthesize] COMMITTED: {n} rows UPSERTed into "
        f"investmentSnapshots."
    )
    return 0


if __name__ == "__main__":
    sys.exit(main())
