#!/usr/bin/env python3
"""ak-7y6o (ak-8kp4 Phase 3a, 2026-11-23) — one-shot operator
script to bulk-ingest per-trade rows from the weekly-securities-
statement PDFs into `InvestmentTrade`.

## Shape

Operator ingest script — NOT a scheduler task. Overseer invokes
once after a dry-run review:

Run from the repo root (the Flask app reads config relative to
cwd). The script puts the repo root on sys.path itself, so plain
`python3` and `python3 -I` both work (ak-4t7f):

    # Dry-run (DEFAULT): parses + logs aggregates, zero DB writes.
    venv/bin/python scripts/ak_8kp4_phase3a_ingest_weekly_securities.py \\
        --user-id <firebase_user_id>

    # Live: actually INSERT. Requires --commit AND --assume-yes
    # (mirrors ak-rcg1 convention — a single-typo guard).
    venv/bin/python scripts/ak_8kp4_phase3a_ingest_weekly_securities.py \\
        --user-id <firebase_user_id> \\
        --statements-dir /home/opc/Akkountant_v2-Back/claude_statements/<uid>/brokerage_statement \\
        --commit --assume-yes

## Idempotency

The `InvestmentTrade` table carries a composite UniqueConstraint
on `(user_id, isin, trade_date, settlement_no, transaction_type)`.
A second run on the same set of PDFs INSERTS zero new rows —
the DB rejects duplicates via the constraint. Each row is
inserted under its own SAVEPOINT; a violation of
`uq_trade_idempotent` is counted as a dedup collision, while any
OTHER IntegrityError or exception counts as a failure (ak-4t7f).

## Exit codes

0 ok | 2 no PDFs found | 3 --commit without --assume-yes |
4 PyMuPDF not importable | 5 --user-id not in users (live) |
6 some rows failed to insert (live; the rest were committed)

## Price = NULL

All rows land with `price=None` + `price_source=None`. Phase 3b
(ak-TBD) UPDATE from Kite Console tradebook CSV. Phase 3c
(ak-TBD) aggregates into per-period `investmentSnapshots`.

## Report (aggregates only — never per-row / per-scrip data)

After the pass, emits:
  * total rows inserted / deduped / rejected (parser-side)
  * BUY / SELL counts, distinct-ISIN count, trade-date range
  * per-FY (Apr→Mar Indian financial year) row count
  * ISIN reconciliation (ak-vm8a, two-sided): checksum-valid ISINs
    in the PDF text == table rows with a valid ISIN (candidates +
    valid-ISIN rejections + pending-skipped); mismatching files are
    counted and WARNed in either direction
  * elapsed time

§23 preserved: #2 (verbatim PDF tuple dedup, no normalization),
#5 (dry-run default + double-flag commit), #7 (price=NULL
semantic documented in model docstring).
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
import logging
import os
import sys
from collections import Counter, defaultdict
from datetime import date, datetime
from decimal import Decimal

# ak-4t7f: `python3 -I` drops the script's repo root from sys.path
# (ModuleNotFoundError: utils). Put it back explicitly.
_REPO_ROOT = os.path.dirname(os.path.dirname(os.path.abspath(__file__)))
if _REPO_ROOT not in sys.path:
    sys.path.insert(0, _REPO_ROOT)


# ── argparse up front so --help / --dry-run previews work
# before the Flask bootstrap (which pulls firebase + google) ──


def _build_parser() -> argparse.ArgumentParser:
    p = argparse.ArgumentParser(
        prog="ak_8kp4_phase3a_ingest_weekly_securities",
        description=(
            "ak-7y6o Phase 3a — ingest per-trade rows from "
            "weekly-securities-statement PDFs into "
            "investmentTrades. Dry-run by default; pass "
            "--commit --assume-yes to actually INSERT."
        ),
    )
    p.add_argument(
        "--user-id", required=True,
        help="Firebase user ID (foreign key into users.userID).",
    )
    p.add_argument(
        "--statements-dir", default=None,
        help=(
            "Directory containing the "
            "*_weekly-securities-statement_HA6023.pdf files. "
            "Defaults to "
            "`<cwd>/claude_statements/<user-id>/brokerage_statement`."
        ),
    )
    p.add_argument(
        "--commit", action="store_true",
        help="Actually INSERT rows. Without this, dry-run only.",
    )
    p.add_argument(
        "--assume-yes", action="store_true",
        help=(
            "Safety belt — must be passed ALONGSIDE --commit for "
            "writes to fire. A single-typo guard (a missing "
            "--commit can't accidentally commit; a missing "
            "--assume-yes blocks + prints a helpful hint)."
        ),
    )
    p.add_argument(
        "--log-level", default="INFO",
        choices=("DEBUG", "INFO", "WARNING", "ERROR"),
    )
    return p


def _fy_label(d: date) -> str:
    """Indian financial year: April 1 → March 31. A trade on
    2024-02-15 belongs to FY23-24; on 2024-04-15 to FY24-25."""
    if d.month >= 4:
        return f"FY{d.year % 100:02d}-{(d.year + 1) % 100:02d}"
    return f"FY{(d.year - 1) % 100:02d}-{d.year % 100:02d}"


def _validate_firebase_id(user_id: str) -> str:
    """Call the ak-et1b shape validator before using user_id
    as a filesystem path component (the default statements-dir
    builds `claude_statements/<user-id>/brokerage_statement`)."""
    # Lazy import — only needed on the --statements-dir default
    # path; keeps the --help output dependency-free.
    from utils.firebase_id_validator import (
        assert_valid_firebase_id_for_path,
    )
    return assert_valid_firebase_id_for_path(user_id)


def _find_pdfs(statements_dir: str) -> list:
    """Discover weekly-securities-statement PDFs under the given
    dir. The broker naming is
    `<YYYY-MM-DD>_weekly-securities-statement_HA6023.pdf` — we
    match on the suffix so date-prefix variants are picked up."""
    if not os.path.isdir(statements_dir):
        return []
    pdfs = []
    for fn in sorted(os.listdir(statements_dir)):
        if (fn.endswith("_weekly-securities-statement_HA6023.pdf")
                or fn.endswith(".pdf")  # permissive fallback
                and "weekly-securities" in fn.lower()):
            pdfs.append(os.path.join(statements_dir, fn))
    return pdfs


def _parse_all_pdfs(pdf_paths: list, logger) -> tuple:
    """Call the parser on each PDF; flatten rows and sum the
    rejection + stats counters. Per-PDF failures are logged (file
    basename + exception type only) and counted; they don't abort
    the batch.

    Returns (rows, rejections, stats). `stats` sums the parser's
    per-file stats plus:
      * parse_failures: files whose parse raised
      * files_mismatch: files whose text-ISIN count differs from
        their valid-ISIN table rows in EITHER direction (ak-vm8a)
    """
    from services.statement_parsers.zerodha_weekly_securities import (
        parse_weekly_securities_pdf,
    )
    all_rows = []
    all_rejections = Counter()
    stats = Counter()
    for p in pdf_paths:
        try:
            rows, rejections, st = parse_weekly_securities_pdf(p)
        except Exception as exc:
            logger.warning(
                "[ak-4t7f] parse failure on %s: %s — continuing",
                os.path.basename(p), type(exc).__name__,
            )
            stats["parse_failures"] += 1
            continue
        all_rows.extend(rows)
        all_rejections.update(rejections)
        stats.update(st)
        if st.get("unaccounted_isins") or st.get("overcounted_isins"):
            stats["files_mismatch"] += 1
    for k in ("qty_ambiguous", "label_mismatch", "missing_settlement",
              "malformed"):
        all_rejections.setdefault(k, 0)
    return all_rows, dict(all_rejections), dict(stats)


def _build_report(
    inserted: int, deduped: int, rejections: dict, stats: dict,
    rows: list, elapsed_s: float, logger,
) -> None:
    """Emit the post-pass report. AGGREGATES ONLY: the rig secrecy
    invariant forbids logging per-row or per-scrip financial data
    (ak-4t7f removed the old per-row dump of the first candidate
    rows and the per-scrip row-count table).

    `rows` = the rows the counts describe (dry-run: candidates;
    live: actually inserted).
    """
    per_fy = defaultdict(int)
    for row in rows:
        per_fy[_fy_label(row["trade_date"])] += 1
    buys = sum(1 for r in rows if r["transaction_type"] == "BUY")
    sells = sum(1 for r in rows if r["transaction_type"] == "SELL")
    dates = [r["trade_date"] for r in rows]

    total_rejected = sum(rejections.values())
    logger.info("── ak-8kp4 Phase 3a INGEST REPORT ──")
    logger.info(
        "elapsed: %.1fs | inserted: %d | deduped: %d | "
        "parser-rejected: %d | parse failures: %d",
        elapsed_s, inserted, deduped, total_rejected,
        stats.get("parse_failures", 0),
    )
    logger.info(
        "  Rejections by reason: missing_settlement=%d "
        "(dedup + Phase 3b require non-NULL settlement), "
        "qty_ambiguous=%d, label_mismatch=%d, malformed=%d",
        rejections.get("missing_settlement", 0),
        rejections.get("qty_ambiguous", 0),
        rejections.get("label_mismatch", 0),
        rejections.get("malformed", 0),
    )
    logger.info(
        "  rows: %d (BUY=%d SELL=%d) | distinct ISINs: %d | "
        "trade_date range: %s .. %s",
        len(rows), buys, sells, len({r["isin"] for r in rows}),
        min(dates) if dates else "n/a", max(dates) if dates else "n/a",
    )
    # ak-vm8a: compare like with like, two-sided. text_isins counts
    # checksum-valid ISINs in the page text; isin_rows counts table
    # rows whose ISIN cell is checksum-valid (= candidates +
    # rejections that still had a valid ISIN + pending skips).
    # Rejections WITHOUT a valid ISIN are reported separately; they
    # must never be added to the accounted side.
    candidates = stats.get("candidate_rows", len(rows))
    text_isins = stats.get("text_isins", 0)
    isin_rows = stats.get("isin_rows", 0)
    pending = stats.get("pending_skipped", 0)
    valid_rejected = max(0, isin_rows - candidates - pending)
    logger.info(
        "  ISIN reconciliation: ISINs in PDF text=%d vs table rows "
        "with a valid ISIN=%d (candidate rows %d + rejected %d + "
        "pending-skipped %d) -> %s",
        text_isins, isin_rows, candidates, valid_rejected, pending,
        (
            "INCOMPLETE (parse failures)" if stats.get("parse_failures")
            else "OK" if text_isins == isin_rows
            else "MISMATCH"
        ),
    )
    logger.info(
        "  ISIN cells recovered from truncation: %d (reverted as "
        "unreconciled: %d) | rejected rows without a valid ISIN: %d",
        stats.get("isin_repaired", 0),
        stats.get("isin_repair_reverted", 0),
        max(0, total_rejected - valid_rejected),
    )
    if stats.get("files_mismatch", 0):
        logger.warning(
            "  %d file(s) have an ISIN count mismatch (text vs table: "
            "%d missing from tables, %d extra in tables). See the "
            "per-file WARNs above; the layout may have changed.",
            stats["files_mismatch"], stats.get("unaccounted_isins", 0),
            stats.get("overcounted_isins", 0),
        )
    logger.info(
        "  tables: %d | zero-activity rows: %d | pending rows "
        "skipped (settle in a later statement): %d",
        stats.get("tables", 0), stats.get("zero_activity", 0),
        stats.get("pending_skipped", 0),
    )
    logger.info("Per-FY breakdown:")
    for fy in sorted(per_fy.keys()):
        logger.info("  %s: %d", fy, per_fy[fy])
    logger.info("── END REPORT ──")


def main() -> int:
    args = _build_parser().parse_args()
    logging.basicConfig(
        level=getattr(logging, args.log_level),
        format="%(asctime)s %(levelname)s %(name)s | %(message)s",
    )
    logger = logging.getLogger("ak_8kp4_phase3a_ingest")

    user_id = _validate_firebase_id(args.user_id)

    if args.statements_dir:
        statements_dir = args.statements_dir
    else:
        statements_dir = os.path.join(
            os.getcwd(), "claude_statements",
            user_id, "brokerage_statement",
        )

    pdf_paths = _find_pdfs(statements_dir)
    if not pdf_paths:
        logger.error(
            "ak-7y6o: no weekly-securities-statement PDFs found "
            "under %s — check --statements-dir",
            statements_dir,
        )
        return 2

    logger.info(
        "ak-7y6o: discovered %d PDFs under %s",
        len(pdf_paths), statements_dir,
    )

    try:
        import fitz  # noqa: F401  (PyMuPDF)
    except ImportError:
        logger.error(
            "ak-4t7f: PyMuPDF (fitz) is not importable in this "
            "interpreter. Run with the app venv's python.",
        )
        return 4

    t0 = datetime.utcnow()
    rows, rejections, stats = _parse_all_pdfs(pdf_paths, logger)
    stats["candidate_rows"] = len(rows)
    logger.info(
        "[ak-4t7f] parsed %d candidate rows total (rejected: %d)",
        len(rows), sum(rejections.values()),
    )

    if not args.commit:
        logger.info(
            "[ak-7y6o] DRY-RUN — would INSERT up to %d rows "
            "(minus DB dedup collisions).",
            len(rows),
        )
        _build_report(
            inserted=0,
            deduped=0,
            rejections=rejections,
            stats=stats,
            rows=rows,
            elapsed_s=(datetime.utcnow() - t0).total_seconds(),
            logger=logger,
        )
        logger.info(
            "[ak-7y6o] DRY-RUN complete. Re-run with --commit "
            "--assume-yes to actually INSERT.",
        )
        return 0

    if not args.assume_yes:
        logger.error(
            "ak-7y6o: --commit requires --assume-yes (single-"
            "typo guard). Add --assume-yes and retry.",
        )
        return 3

    # ── Live ingest path ────────────────────────────────────
    # Lazy-import the Flask app + models so --help / --dry-run
    # paths don't pull the full boot chain.
    from app import app
    from models import InvestmentTrade
    from models.users import User

    with app.app_context():
        session = app.db.session
        # ak-4t7f: verify the FK target up front so a wrong
        # --user-id fails once, loudly, instead of N row failures.
        if session.get(User, user_id) is None:
            logger.error(
                "ak-4t7f: --user-id not found in users table; "
                "nothing inserted",
            )
            return 5
        inserted, deduped, failed, inserted_rows = insert_rows(
            session, InvestmentTrade, user_id, rows, logger,
        )
        session.commit()

    stats["insert_failures"] = failed
    _build_report(
        inserted=inserted,
        deduped=deduped,
        rejections=rejections,
        stats=stats,
        rows=inserted_rows,
        elapsed_s=(datetime.utcnow() - t0).total_seconds(),
        logger=logger,
    )
    if failed:
        logger.error(
            "ak-4t7f: %d row(s) failed to insert (NOT dedup); the "
            "other rows were committed. Investigate before relying "
            "on totals.", failed,
        )
        return 6
    logger.info(
        "[ak-7y6o] LIVE ingest complete. Phase 3b (ak-TBD) "
        "can now UPDATE price fields from Kite Console CSV.",
    )
    return 0


_UQ_NAME = "uq_trade_idempotent"
_UQ_SQLITE = (
    "UNIQUE constraint failed: investmentTrades.user_id, "
    "investmentTrades.isin, investmentTrades.trade_date, "
    "investmentTrades.settlement_no, investmentTrades.transaction_type"
)


def is_idempotency_duplicate(exc) -> bool:
    """True only for a violation of the uq_trade_idempotent key:
    MySQL errno 1062 naming that key, or SQLite's UNIQUE failure
    on exactly its columns. Any other IntegrityError (FK, NOT NULL,
    another key) is a real failure, not a dedup."""
    orig = getattr(exc, "orig", exc)
    msg = str(orig)
    errno = getattr(orig, "errno", None)
    if errno is None and getattr(orig, "args", None):
        errno = orig.args[0] if isinstance(orig.args[0], int) else None
    if errno == 1062:
        return _UQ_NAME in msg
    return _UQ_SQLITE in msg


def insert_rows(session, model, user_id, rows, logger) -> tuple:
    """Insert candidate rows, one SAVEPOINT per row, so a duplicate
    never rolls back earlier inserts in the batch. Caller commits.

    Returns (inserted, deduped, failed, inserted_rows). Only a
    uq_trade_idempotent violation counts as deduped; every other
    exception counts as failed (logged with row index + exception
    type only, never row values)."""
    from sqlalchemy.exc import IntegrityError

    inserted = deduped = failed = 0
    inserted_rows: list = []
    for idx, row in enumerate(rows, start=1):
        try:
            with session.begin_nested():
                session.add(model(
                    user_id=user_id,
                    trade_date=row["trade_date"],
                    settlement_date=row["settlement_date"],
                    isin=row["isin"],
                    symbol=row["symbol"],
                    quantity=row["quantity"],
                    transaction_type=row["transaction_type"],
                    segment=row["segment"],
                    settlement_no=row["settlement_no"],
                    price=row["price"],
                    price_source=row["price_source"],
                    source=row["source"],
                    source_file=row["source_file"],
                ))
            inserted += 1
            inserted_rows.append(row)
        except IntegrityError as exc:
            if is_idempotency_duplicate(exc):
                deduped += 1
            else:
                failed += 1
                logger.warning(
                    "ak-4t7f: integrity failure (not a dedup) for "
                    "candidate row #%d", idx,
                )
        except Exception as exc:
            failed += 1
            logger.warning(
                "ak-4t7f: unexpected insert failure for candidate "
                "row #%d — %s", idx, type(exc).__name__,
            )
    return inserted, deduped, failed, inserted_rows


if __name__ == "__main__":
    sys.exit(main())
