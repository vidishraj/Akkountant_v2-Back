#!/usr/bin/env python3
"""ak-v8z1 (ak-ofir Phase 2A): one-shot loader for operator-supplied
historical gold rates into ``goldHistoricalRates``.

Per Overseer 2026-11-22 scope decision (ak-ofir Phase 1 §5): IBJA
has no public historical endpoint, and the vendor-dependency
decision for metalpriceapi / goldapi was deferred. This script
exists so a human operator can hand over a CSV or JSON of historical
rates (gathered from IBJA archives, media quotes, or a third-party
API) and get it into the backfill-synthesis stream without a
schema change later.

## Input formats

**CSV** (default):
    date,rate_per_gram,source
    2026-11-21,7238.50,goodreturns
    2026-11-20,7215.25,goodreturns
    ...

**JSON** (array of {date, rate_per_gram, source}):
    [
      {"date": "2026-11-21", "rate_per_gram": 7238.50, "source": "goodreturns"},
      ...
    ]

Date column accepts ``YYYY-MM-DD``. Rate is INR per gram (NOT per
10g — convert at source). Source is an operator-provided provenance
label (<=50 chars).

## Dry-run

Default is DRY-RUN — prints what would be upserted + validation
errors, no DB writes. Pass ``--commit`` to actually persist.

## Idempotency

``goldHistoricalRates`` uses ``date`` as PK. Re-running the loader
over an overlapping date range uses MySQL ``INSERT ... ON
DUPLICATE KEY UPDATE`` so repeat runs are safe.

## Usage

    python3 -I scripts/load_gold_historical_rates.py \\
        --file /path/to/gold_history.csv \\
        [--format csv|json] \\
        [--commit]

The ``-I`` flag isolates Python's sys.path so a planted module in
the input-file's directory can't be imported (per CLAUDE.md
untrusted-input guidance).
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
import csv
import json
import os
import sys
from datetime import datetime
from decimal import Decimal, InvalidOperation


# Allow running from anywhere in the backend repo.
_BACKEND_ROOT = os.path.abspath(
    os.path.join(os.path.dirname(__file__), os.pardir)
)
if _BACKEND_ROOT not in sys.path:
    sys.path.insert(0, _BACKEND_ROOT)


def _parse_rows_csv(path):
    """Yield dict rows from a CSV file with the expected columns."""
    with open(path, "r", newline="") as fh:
        reader = csv.DictReader(fh)
        for lineno, row in enumerate(reader, start=2):  # header @1
            yield lineno, row


def _parse_rows_json(path):
    """Yield dict rows from a JSON array file."""
    with open(path, "r") as fh:
        data = json.load(fh)
    if not isinstance(data, list):
        raise ValueError(
            f"JSON input must be a top-level array; got "
            f"{type(data).__name__}"
        )
    # Line numbers are not meaningful for JSON; use the index.
    for i, row in enumerate(data, start=1):
        if not isinstance(row, dict):
            raise ValueError(
                f"JSON entry at index {i} must be an object, got "
                f"{type(row).__name__}"
            )
        yield i, row


def _validate_row(lineno, row):
    """Validate + coerce a raw row into (date, Decimal, source).

    Returns (ok: bool, value_or_error).
    """
    required = ("date", "rate_per_gram", "source")
    missing = [k for k in required if not row.get(k)]
    if missing:
        return False, (
            f"line {lineno}: missing required column(s): {missing}"
        )
    date_str = str(row["date"]).strip()
    try:
        date_val = datetime.strptime(date_str, "%Y-%m-%d").date()
    except ValueError:
        return False, (
            f"line {lineno}: 'date' must be YYYY-MM-DD, got "
            f"{date_str!r}"
        )
    rate_str = str(row["rate_per_gram"]).strip()
    try:
        rate_val = Decimal(rate_str)
    except (InvalidOperation, TypeError):
        return False, (
            f"line {lineno}: 'rate_per_gram' must be numeric, got "
            f"{rate_str!r}"
        )
    if rate_val <= 0:
        return False, (
            f"line {lineno}: 'rate_per_gram' must be > 0, got "
            f"{rate_val}"
        )
    source_val = str(row["source"]).strip()[:50]
    if not source_val:
        return False, f"line {lineno}: 'source' must be non-empty"
    return True, (date_val, rate_val, source_val)


def main():
    parser = argparse.ArgumentParser(
        description=(
            "Load historical gold rates into goldHistoricalRates "
            "(ak-v8z1 Phase 2A)."
        )
    )
    parser.add_argument(
        "--file", required=True,
        help="Input CSV or JSON file with historical gold rows.",
    )
    parser.add_argument(
        "--format", choices=("csv", "json"), default="csv",
        help="Input file format (default: csv).",
    )
    parser.add_argument(
        "--commit", action="store_true",
        help=(
            "Perform the UPSERT. Omit for dry-run (validate + "
            "preview only; no DB write)."
        ),
    )
    args = parser.parse_args()

    if not os.path.isfile(args.file):
        print(
            f"[load_gold_historical_rates] input file not found: "
            f"{args.file}",
            file=sys.stderr,
        )
        return 2

    # Parse + validate every row before writing anything.
    valid_rows = []
    validation_errors = []
    iterator = (
        _parse_rows_csv(args.file) if args.format == "csv"
        else _parse_rows_json(args.file)
    )
    for lineno, row in iterator:
        ok, result = _validate_row(lineno, row)
        if ok:
            valid_rows.append(result)
        else:
            validation_errors.append(result)

    if validation_errors:
        print(
            f"[load_gold_historical_rates] validation failures "
            f"({len(validation_errors)}):",
            file=sys.stderr,
        )
        for err in validation_errors[:50]:
            print(f"  {err}", file=sys.stderr)
        if len(validation_errors) > 50:
            print(
                f"  ... and {len(validation_errors) - 50} more",
                file=sys.stderr,
            )
        if not valid_rows:
            return 3

    print(
        f"[load_gold_historical_rates] parsed {len(valid_rows)} "
        f"valid rows "
        f"({len(validation_errors)} errors)."
    )

    # Dry-run preview
    preview_n = min(5, len(valid_rows))
    if preview_n:
        print(f"[load_gold_historical_rates] preview first {preview_n}:")
        for date_val, rate_val, source_val in valid_rows[:preview_n]:
            print(f"  {date_val} | ₹{rate_val}/g | {source_val}")

    if not args.commit:
        print(
            "[load_gold_historical_rates] DRY-RUN — no DB write. "
            "Pass --commit to persist."
        )
        return 0

    # Commit path: a bare engine from DATABASE_URL + UPSERT (ak-n3lf: no
    # app boot, so no create_all DDL, seed jobs or scheduler threads).
    try:
        from models.goldHistoricalRate import GoldHistoricalRate
        from sqlalchemy import create_engine
        from sqlalchemy.dialects.mysql import insert as mysql_insert
        from sqlalchemy.orm import Session
    except Exception as exc:
        print(
            f"[load_gold_historical_rates] import chain failed: "
            f"{type(exc).__name__}",
            file=sys.stderr,
        )
        return 4
    try:
        from dotenv import load_dotenv     # same .env the app reads
        load_dotenv()
    except ImportError:
        pass
    url = os.getenv("DATABASE_URL")
    if not url:
        print("[load_gold_historical_rates] DATABASE_URL is not set", file=sys.stderr)
        return 4

    payload = [
        {
            "date": date_val,
            "rate_per_gram": rate_val,
            "source": source_val,
        }
        for date_val, rate_val, source_val in valid_rows
    ]
    stmt = mysql_insert(GoldHistoricalRate).values(payload)
    # On conflict: refresh rate_per_gram + source (operator-
    # supplied label may have changed).
    upsert = stmt.on_duplicate_key_update(
        rate_per_gram=stmt.inserted.rate_per_gram,
        source=stmt.inserted.source,
    )
    engine = create_engine(url, pool_pre_ping=True)
    try:
        with Session(engine) as session:
            try:
                session.execute(upsert)
                session.commit()
            except Exception as exc:
                session.rollback()
                print(
                    f"[load_gold_historical_rates] UPSERT failed; "
                    f"rolled back: {type(exc).__name__}",
                    file=sys.stderr,
                )
                return 5
    finally:
        engine.dispose()

    print(
        f"[load_gold_historical_rates] COMMITTED: "
        f"{len(valid_rows)} rows upserted."
    )
    return 0


if __name__ == "__main__":
    sys.exit(main())
