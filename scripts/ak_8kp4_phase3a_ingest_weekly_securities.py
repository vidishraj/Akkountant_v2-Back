#!/usr/bin/env python3
"""ak-7y6o (ak-8kp4 Phase 3a, 2026-11-23) — one-shot operator
script to bulk-ingest per-trade rows from the weekly-securities-
statement PDFs into `InvestmentTrade`.

## Shape

Operator ingest script — NOT a scheduler task. Overseer invokes
once after a dry-run review:

    # Dry-run (DEFAULT): parses + logs insert plan, zero DB writes.
    python3 -I scripts/ak_8kp4_phase3a_ingest_weekly_securities.py \\
        --user-id <firebase_user_id>

    # Live: actually INSERT. Requires --commit AND --assume-yes
    # (mirrors ak-rcg1 convention — a single-typo guard).
    python3 -I scripts/ak_8kp4_phase3a_ingest_weekly_securities.py \\
        --user-id <firebase_user_id> \\
        --statements-dir /home/opc/Akkountant_v2-Back/claude_statements/<uid>/brokerage_statement \\
        --commit --assume-yes

## Idempotency

The `InvestmentTrade` table carries a composite UniqueConstraint
on `(user_id, isin, trade_date, settlement_no, transaction_type)`.
A second run on the same set of PDFs INSERTS zero new rows —
the DB rejects duplicates via the constraint; this script
catches the `IntegrityError` per row, counts it as a dedup
collision (not a failure), and continues.

## Price = NULL

All rows land with `price=None` + `price_source=None`. Phase 3b
(ak-TBD) UPDATE from Kite Console tradebook CSV. Phase 3c
(ak-TBD) aggregates into per-period `investmentSnapshots`.

## Report

After the pass, emits:
  * total rows inserted / deduped / rejected (parser-side)
  * per-FY (Apr→Mar Indian financial year) insert count
  * top 10 scrips by insert count
  * elapsed time

§23 preserved: #2 (verbatim PDF tuple dedup, no normalization),
#5 (dry-run default + double-flag commit), #7 (price=NULL
semantic documented in model docstring).
"""
from __future__ import annotations

import argparse
import logging
import os
import sys
from collections import Counter, defaultdict
from datetime import date, datetime
from decimal import Decimal


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
    """Call the parser on each PDF, flatten into one row list +
    accumulate rejection counters. Per-PDF failures are logged
    but don't abort the batch.

    ak-7y6o reviewer CRITICAL (2026-11-23): parser now returns
    (rows, rejections); sum rejections across all files so the
    report surfaces "N trades rejected for missing
    settlement_no" to the operator.
    """
    from services.statement_parsers.zerodha_weekly_securities import (
        parse_weekly_securities_pdf,
    )
    all_rows = []
    all_rejections = {
        "qty_ambiguous": 0,
        "label_mismatch": 0,
        "missing_settlement": 0,
    }
    for p in pdf_paths:
        try:
            rows, rejections = parse_weekly_securities_pdf(p)
        except Exception as exc:
            logger.warning(
                "[ak-7y6o] parse failure on %s: %s — continuing",
                os.path.basename(p), type(exc).__name__,
            )
            continue
        logger.debug(
            "[ak-7y6o] parsed %d rows from %s (rejections: %s)",
            len(rows), os.path.basename(p), rejections,
        )
        all_rows.extend(rows)
        for k, v in rejections.items():
            all_rejections[k] = all_rejections.get(k, 0) + v
    return all_rows, all_rejections


def _build_report(
    inserted: int, deduped: int, rejections: dict,
    inserted_rows: list, elapsed_s: float, logger,
) -> None:
    """Emit the post-pass report: totals + per-FY + top-10
    scrips + per-reason rejection counters.

    ak-7y6o reviewer CRITICAL: rejections now surfaces per-
    reason counts (missing_settlement is the critical one —
    Overseer sees these in dry-run output + decides whether
    the underlying PDFs are anomalous).
    """
    per_fy = defaultdict(int)
    per_scrip = Counter()
    for row in inserted_rows:
        per_fy[_fy_label(row["trade_date"])] += 1
        per_scrip[row["symbol"]] += 1

    total_rejected = sum(rejections.values())
    logger.info("── ak-7y6o Phase 3a INGEST REPORT ──")
    logger.info(
        "elapsed: %.1fs | inserted: %d | deduped: %d | "
        "parser-rejected: %d",
        elapsed_s, inserted, deduped, total_rejected,
    )
    if total_rejected > 0:
        logger.info(
            "  Rejections by reason: missing_settlement=%d "
            "(ak-7y6o CRITICAL — dedup + Phase 3b require "
            "non-NULL settlement), qty_ambiguous=%d, "
            "label_mismatch=%d — see WARN logs above for "
            "per-row detail",
            rejections.get("missing_settlement", 0),
            rejections.get("qty_ambiguous", 0),
            rejections.get("label_mismatch", 0),
        )
    logger.info("Per-FY breakdown:")
    for fy in sorted(per_fy.keys()):
        logger.info("  %s: %d", fy, per_fy[fy])
    logger.info("Top 10 scrips by row count:")
    for scrip, count in per_scrip.most_common(10):
        logger.info("  %-20s %d", scrip, count)
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

    t0 = datetime.utcnow()
    rows, rejections = _parse_all_pdfs(pdf_paths, logger)
    logger.info(
        "[ak-7y6o] parsed %d candidate rows total (rejected: "
        "missing_settlement=%d, qty_ambiguous=%d, "
        "label_mismatch=%d)",
        len(rows),
        rejections.get("missing_settlement", 0),
        rejections.get("qty_ambiguous", 0),
        rejections.get("label_mismatch", 0),
    )

    if not args.commit:
        logger.info(
            "[ak-7y6o] DRY-RUN — would INSERT %d rows. First 5:",
            len(rows),
        )
        for row in rows[:5]:
            logger.info("  %s", row)
        _build_report(
            inserted=0,
            deduped=0,
            rejections=rejections,
            inserted_rows=rows,  # for FY/scrip breakdown
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
    from sqlalchemy.exc import IntegrityError

    inserted = 0
    deduped = 0
    inserted_rows: list = []

    with app.app_context():
        session = app.db.session
        for row in rows:
            try:
                trade = InvestmentTrade(
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
                )
                session.add(trade)
                session.flush()
                inserted += 1
                inserted_rows.append(row)
            except IntegrityError:
                session.rollback()
                deduped += 1
            except Exception as exc:
                session.rollback()
                logger.warning(
                    "ak-7y6o: unexpected insert failure for row "
                    "ISIN=%s date=%s type=%s — %s",
                    row.get("isin"), row.get("trade_date"),
                    row.get("transaction_type"),
                    type(exc).__name__,
                )
        session.commit()

    _build_report(
        inserted=inserted,
        deduped=deduped,
        rejections=rejections,
        inserted_rows=inserted_rows,
        elapsed_s=(datetime.utcnow() - t0).total_seconds(),
        logger=logger,
    )
    logger.info(
        "[ak-7y6o] LIVE ingest complete. Phase 3b (ak-TBD) "
        "can now UPDATE price fields from Kite Console CSV.",
    )
    return 0


if __name__ == "__main__":
    sys.exit(main())
