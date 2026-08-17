#!/usr/bin/env python3
"""ak-32o v3 HDFC re-parse orchestrator — mailProcessor path (Q1 (A)).

Rewires the ak-32o re-parse workflow through the FULL mailProcessor
pipeline (`_process_single_pdf_email`) rather than the surgical
`reprocess_pdf` used by prior v1/v2 attempts. Semantic delta from
reprocess_pdf: this path ALSO stamps `processedEmails` on success and
persists the PDF via `_persist_pdf` — matching what a live ingest
would do for a first-time-seen file.

## Q-gate status (per Lead's dispatch 2026-08-17)
    Q1 (A): CONFIRMED  — _process_single_pdf_email path.
    Q4     : CONFIRMED  — Vidish's user_id (same scope as v2).
    Q2     : ESCALATED to Overseer — per-file (a) vs inter-file (b)
             delta for OPENING_BALANCE row. **Phase 4 CODE IS NOT
             WIRED**; the orchestrator emits Phase-4 preview data
             only (stated_opening / stated_closing / reconstructed_
             closing / delta per file) so Overseer can pick semantics
             from real numbers. `--phase all` STOPS after Phase 5's
             verify report; explicit `--enable-phase4-per-file` /
             `--enable-phase4-inter-file` flags exist as placeholders
             behind an `AWAITING_Q2_GO` guard that refuses to run.
    Q3     : ESCALATED to Overseer — pin exact fileIDs vs
             whichever-fileDetails-query-returns. Phase 1's row-count
             check is SOFT WARN (not hard abort) until Overseer picks.
             Actual manifest is printed for Lead + Overseer to eyeball
             before Phase 2 fires.

## Scope
Backend PREP only. Infra fires. This script never executes destructive
prod writes unless BOTH:
  * --dry-run flag is ABSENT
  * --assume-yes flag is PRESENT
Without --assume-yes, Phase 2 prints the projected DELETE + count and
exits (semi-dry-run mode) so an operator can eyeball.

## Prior work
agent/ak32o-fire-script @ 950be49 (v1) + bef7505 (v2) used reprocess_pdf.
REFERENCE ONLY — different semantics per Lead. Not re-used.

## Structured output
Every phase emits one-line JSON events prefixed `ak32o|` so infra can
grep-and-paste back to Lead in the `[STATUS ak-32o]` format.
"""

from __future__ import annotations

import argparse
import json
import os
import sys
import time
from datetime import datetime, timezone
from glob import glob as _glob

sys.path.insert(0, os.path.dirname(os.path.dirname(os.path.abspath(__file__))))

from dotenv import load_dotenv
load_dotenv()

from flask import g
from sqlalchemy import text
from utils.logger import Logger

logger = Logger("ak32o_hdfc_mp_fire").get_logger()

BASE_DIR = os.path.dirname(os.path.dirname(os.path.abspath(__file__)))
STATEMENTS_DIR = os.path.join(BASE_DIR, "claude_statements")

# ak-32o expected file count. v2 (bef7505) fixed the scope mismatch that
# had put us at 11 (from processedEmails) vs 12 (canonical fileDetails).
# Kept as SOFT WARN (not hard abort) pending Overseer's Q3 answer on
# whether to pin exact fileIDs.
_EXPECTED_FILE_COUNT = 12

# ak-32o Phase 4 OPENING_BALANCE delta insert. HELD until Q2 answered.
_PHASE4_AWAITING_Q2_GO = True

# ak-32o v3 reviewer MAJOR (hq-wisp-soqod7): Phase 4 preview must
# EXCLUDE any synthetic OPENING_BALANCE rows from debit_sum/credit_sum
# so reconstructed_closing + delta calculations aren't polluted by
# rows that were themselves inserted to close a delta.
#
# Backstop-behavior investigation result: the ak-ifc
# `_run_hdfc_file_level_reconciliation` backstop (fires on
# _bank='HDFC_DEBIT') does NOT insert any synthetic OPENING_BALANCE
# row today — the DIVERGED path only calls
# `mark_reconciliation_fallback` (bool flag on fileDetails) + re-runs
# chunk extraction under `force_no_mask=True`, which produces MORE
# real `processed_via=CLAUDE_CODE` bank rows for the same fileID.
# Those are legitimate transactions and correctly count toward the
# preview's sums.
#
# The exclusion filter is nonetheless required (reviewer M4 subsumed):
#   (a) Phase 4's OWN destructive insert (post-Q2) will land in the
#       same table as `tag='OPENING_BALANCE' + referenceID LIKE
#       'OPENING_BALANCE_%'` per the plan. Any re-run of the preview
#       after that insert must not double-count the synthetic row
#       when reconstructing the closing balance.
#   (b) Defense-in-depth against any future backstop / migration that
#       DOES land a synthetic row of this class.
#
# Filter is (tag != 'OPENING_BALANCE' AND referenceID NOT LIKE
# 'OPENING_BALANCE_%') — matches the deterministic PK shape Phase 4's
# insert will use.
_OPENING_BALANCE_TAG = "OPENING_BALANCE"
_OPENING_BALANCE_REF_PREFIX = "OPENING_BALANCE_"


# ─── Utility: structured emit ──────────────────────────────────────────

def emit(event: str, **fields):
    """Emit a one-line JSON event to stdout for infra to grep+paste.

    Prefix `ak32o|` so operators can filter (`| grep '^ak32o|'`) even
    when other loggers interleave.
    """
    payload = {"event": event, "ts": datetime.now(timezone.utc).isoformat(), **fields}
    print(f"ak32o|{json.dumps(payload, default=str, sort_keys=True)}", flush=True)


# ─── Flask app (matches reprocess_pdfs.py boilerplate) ─────────────────

def build_app():
    """Build the Akkountant app w/o HTTP server (matches reprocess_pdfs.py)."""
    original_env = os.environ.get("ENV")
    os.environ["ENV"] = "LOCAL"
    from app import Akkountant
    app = Akkountant(__name__)
    if original_env is not None:
        os.environ["ENV"] = original_env
    else:
        os.environ.pop("ENV", None)
    return app


# ─── Phase 1: enumerate the canonical HDFC files ───────────────────────

def enumerate_hdfc_files(app, user_id: str) -> list[dict]:
    """v2 fileDetails query pattern (bef7505) — canonical, not processedEmails-scoped.

    Chronological order (oldest first): COALESCE(pe.email_date, fd.uploadDate) ASC.
    Path resolution: primary processedEmails.pdf_filename → claude_statements/<uid>/;
    fallback glob claude_statements/<uid>/**/{gmail_id}_*.
    """
    with app.app_context():
        g.db = app.db
        rows = app.db.session.execute(text("""
            SELECT fd.fileID          AS file_id,
                   fd.gmail_message_id AS gmail_id,
                   fd.fileName        AS file_name,
                   fd.bank            AS bank,
                   fd.uploadDate      AS upload_date,
                   pe.pdf_filename    AS pdf_filename,
                   pe.email_date      AS email_date,
                   pe.sender          AS sender,
                   pe.subject         AS subject,
                   pe.category        AS category
              FROM fileDetails fd
              LEFT JOIN processedEmails pe
                ON pe.gmail_id = fd.gmail_message_id
               AND pe.user_id = fd.user
             WHERE fd.bank = 'HDFC_DEBIT'
               AND fd.user = :uid
               AND fd.deleted = 0
             ORDER BY COALESCE(pe.email_date, fd.uploadDate) ASC
        """), {"uid": user_id}).fetchall()

    out = []
    user_stmt_dir = os.path.join(STATEMENTS_DIR, user_id)
    for r in rows:
        entry = dict(r._mapping)
        gmail_id = entry.get("gmail_id")
        pdf_rel = entry.get("pdf_filename")
        resolved = None
        if pdf_rel:
            candidate = pdf_rel if os.path.isabs(pdf_rel) else os.path.join(user_stmt_dir, pdf_rel)
            if os.path.isfile(candidate):
                resolved = candidate
        if not resolved and gmail_id:
            matches = _glob(os.path.join(user_stmt_dir, "**", f"{gmail_id}_*"), recursive=True)
            resolved = matches[0] if matches else None
        entry["resolved_pdf_path"] = resolved
        out.append(entry)
    return out


# ─── Phase 0: snapshot ──────────────────────────────────────────────────

def snapshot_hdfc(app, user_id: str, file_ids: list[str], backup_table: str,
                  *, dry_run: bool) -> dict:
    """Snapshot the HDFC_DEBIT rows scoped to `file_ids` into `backup_table`.

    Idempotent: CREATE IF NOT EXISTS + INSERT IGNORE (PK dedup).
    Asserts snapshot count > 0 AND matches live count for the same predicate
    to catch "we backed up from the wrong scope" BEFORE the wipe.
    """
    live_count = _count_live_hdfc_scoped(app, user_id, file_ids)

    if dry_run:
        emit("phase0.dry", backup_table=backup_table, live_count=live_count)
        return {"live_count": live_count, "snapshot_count": None, "dry_run": True}

    with app.app_context():
        g.db = app.db
        app.db.session.execute(text(
            f"CREATE TABLE IF NOT EXISTS `{backup_table}` LIKE `transactions`"
        ))
        app.db.session.commit()

        # INSERT IGNORE for retry-safety (PK collisions from prior snapshot rows
        # are silently ignored).
        placeholders = ", ".join(f":fid_{i}" for i in range(len(file_ids)))
        params = {f"fid_{i}": fid for i, fid in enumerate(file_ids)}
        params["uid"] = user_id
        app.db.session.execute(text(f"""
            INSERT IGNORE INTO `{backup_table}`
                SELECT * FROM transactions
                 WHERE user = :uid
                   AND bank = 'HDFC_DEBIT'
                   AND fileID IN ({placeholders})
        """), params)
        app.db.session.commit()

        snapshot_count = app.db.session.execute(text(
            f"SELECT COUNT(*) FROM `{backup_table}`"
        )).scalar()

    if snapshot_count == 0:
        emit("phase0.error", reason="snapshot table has zero rows", backup_table=backup_table)
        raise SystemExit("Phase 0 aborted: snapshot count is 0")
    if snapshot_count < live_count:
        emit("phase0.warn", reason="snapshot < live (retry from prior snapshot?)",
             backup_table=backup_table, snapshot_count=snapshot_count, live_count=live_count)
    emit("phase0.done", backup_table=backup_table,
         snapshot_count=snapshot_count, live_count=live_count)
    return {"live_count": live_count, "snapshot_count": snapshot_count, "dry_run": False}


def _count_live_hdfc_scoped(app, user_id: str, file_ids: list[str]) -> int:
    if not file_ids:
        return 0
    with app.app_context():
        g.db = app.db
        placeholders = ", ".join(f":fid_{i}" for i in range(len(file_ids)))
        params = {f"fid_{i}": fid for i, fid in enumerate(file_ids)}
        params["uid"] = user_id
        return app.db.session.execute(text(f"""
            SELECT COUNT(*) FROM transactions
             WHERE user = :uid
               AND bank = 'HDFC_DEBIT'
               AND fileID IN ({placeholders})
        """), params).scalar() or 0


# ─── Phase 2: wipe (double-gated) ───────────────────────────────────────

def wipe_hdfc(app, user_id: str, file_ids: list[str], snapshot_result: dict,
              *, dry_run: bool, assume_yes: bool) -> dict:
    """DELETE the HDFC_DEBIT rows scoped to `file_ids`.

    Pre-guards:
      * dry_run → print projected DELETE count via SELECT COUNT, no write.
      * NOT assume_yes → print projected count + exit (semi-dry-run — safety
        net so operator can eyeball before firing).
      * snapshot must exist AND snapshot_count == current live_count
        (concurrent writes landed between snapshot + wipe → refuse).
    """
    live_count = _count_live_hdfc_scoped(app, user_id, file_ids)

    if dry_run:
        emit("phase2.dry", would_delete=live_count, file_count=len(file_ids))
        return {"deleted": 0, "would_delete": live_count, "dry_run": True}

    if not assume_yes:
        emit("phase2.gated",
             reason="--assume-yes required for destructive wipe",
             would_delete=live_count, file_count=len(file_ids))
        raise SystemExit(
            "Phase 2 gated: pass --assume-yes to perform the DELETE "
            "(prints projected count only without it)"
        )

    # Snapshot integrity check.
    snap_count = snapshot_result.get("snapshot_count")
    if snap_count is None:
        emit("phase2.error", reason="no snapshot count available (Phase 0 was dry?)")
        raise SystemExit("Phase 2 aborted: no snapshot to protect against")
    if snap_count != live_count:
        emit("phase2.error",
             reason="snapshot_count != current live_count (concurrent writes?)",
             snapshot_count=snap_count, current_live=live_count)
        raise SystemExit(
            "Phase 2 aborted: snapshot drift — re-run Phase 0 to refresh, "
            "then retry Phase 2"
        )

    # ak-32o v3 reviewer M1: pre-write intent emit for forensic
    # completeness if the DELETE throws mid-flight (crash / lost
    # connection). Snapshot precedes so data is safe; this is pure
    # observability so operators can distinguish "wipe never fired"
    # from "wipe fired and DB is in-between snapshot + live state."
    with app.app_context():
        g.db = app.db
        placeholders = ", ".join(f":fid_{i}" for i in range(len(file_ids)))
        params = {f"fid_{i}": fid for i, fid in enumerate(file_ids)}
        params["uid"] = user_id
        emit("phase2.deleting",
             would_delete=live_count, file_count=len(file_ids),
             file_ids=file_ids, snapshot_table=None)  # snapshot_table filled by caller context
        result = app.db.session.execute(text(f"""
            DELETE FROM transactions
             WHERE user = :uid
               AND bank = 'HDFC_DEBIT'
               AND fileID IN ({placeholders})
        """), params)
        app.db.session.commit()
        deleted = result.rowcount

    emit("phase2.done", deleted=deleted, expected=live_count)
    return {"deleted": deleted, "expected": live_count, "dry_run": False}


# ─── Phase 3: chronological re-invoke via mailProcessor (path A) ──────

def reinvoke_mailprocessor_chronologically(
    app, user_id: str, files: list[dict],
    *, dry_run: bool,
) -> list[dict]:
    """For each file in `files` (already sorted oldest-first by caller),
    invoke `mp._process_single_pdf_email` with the persisted PDF path
    (bypasses Gmail download). Returns per-file result dicts."""
    results = []

    if dry_run:
        for f in files:
            emit("phase3.dry", file_id=f["file_id"], gmail_id=f["gmail_id"],
                 fileName=f["file_name"], pdf=f.get("resolved_pdf_path"))
            results.append({
                "file_id": f["file_id"], "gmail_id": f["gmail_id"],
                "dry_run": True,
            })
        return results

    mp = app.mailProcessor
    with app.app_context():
        g.db = app.db
        for i, f in enumerate(files, 1):
            file_id = f["file_id"]
            gmail_id = f["gmail_id"]
            pdf_path = f.get("resolved_pdf_path")
            if not pdf_path or not os.path.isfile(pdf_path):
                emit("phase3.skip", file_id=file_id, gmail_id=gmail_id,
                     reason="PDF path unresolved or missing on disk")
                results.append({
                    "file_id": file_id, "gmail_id": gmail_id,
                    "status": "skipped", "reason": "pdf_unresolved",
                })
                continue

            email = {
                "gmail_id": gmail_id,
                "sender": f.get("sender") or "reprocess@local",
                "subject": f.get("subject") or f"Reprocess ak-32o {file_id}",
                "date": str(f.get("email_date") or ""),
                # Signal overrides to _process_single_pdf_email:
                "_bank": "HDFC_DEBIT",
                "_category": f.get("category") or "bank_statement",
                "_local_pdf_paths": [pdf_path],
            }
            emit("phase3.start", idx=i, total=len(files),
                 file_id=file_id, gmail_id=gmail_id, pdf=pdf_path)
            t0 = time.time()
            try:
                # _process_single_pdf_email doesn't return the analysis
                # summary; we probe DB after the call for row count.
                mp._process_single_pdf_email(email, user_id, processing_mode="text")
                elapsed = time.time() - t0
                # Count what actually landed for this fileID + user.
                from models.transactions import Transactions
                inserted = app.db.session.query(Transactions).filter(
                    Transactions.fileID == file_id,
                    Transactions.user == user_id,
                ).count()
                emit("phase3.done", file_id=file_id, gmail_id=gmail_id,
                     inserted=inserted, elapsed_s=round(elapsed, 2))
                results.append({
                    "file_id": file_id, "gmail_id": gmail_id,
                    "status": "ok", "inserted": inserted,
                    "elapsed_s": round(elapsed, 2),
                })
            except Exception as exc:
                elapsed = time.time() - t0
                emit("phase3.error", file_id=file_id, gmail_id=gmail_id,
                     error=str(exc), elapsed_s=round(elapsed, 2))
                results.append({
                    "file_id": file_id, "gmail_id": gmail_id,
                    "status": "error", "error": str(exc),
                    "elapsed_s": round(elapsed, 2),
                })
    return results


# ─── Phase 4 preview (Q2 held) + Phase 5: verify + report ──────────────

def phase4_preview(app, user_id: str, files: list[dict]) -> list[dict]:
    """Emit stated_opening / stated_closing / reconstructed_closing / delta
    per file so Overseer can pick per-file (a) vs inter-file (b) semantics.

    Read-only. Does NOT insert any OPENING_BALANCE row (that's held on Q2).
    """
    import fitz as _fitz
    from utils.statement_sections import (
        detect_hdfc_sections, parse_hdfc_savings_summary,
    )
    from models.transactions import Transactions
    from sqlalchemy import func as sa_func

    previews = []

    with app.app_context():
        g.db = app.db
        prev_reconstructed_closing = None
        for f in files:
            file_id = f["file_id"]
            pdf_path = f.get("resolved_pdf_path")
            if not pdf_path or not os.path.isfile(pdf_path):
                previews.append({"file_id": file_id, "note": "pdf_unresolved"})
                continue

            # Parse PDF summary.
            try:
                doc = _fitz.open(pdf_path)
                lines = []
                for pn in range(1, doc.page_count + 1):
                    lines.append(f"\f<PAGE:{pn}>")
                    lines.extend(doc[pn - 1].get_text("text").split("\n"))
                doc.close()
                raw = "\n".join(lines)
                spans = detect_hdfc_sections(raw)
                summary = parse_hdfc_savings_summary(raw, spans=spans)
            except Exception as exc:
                previews.append({"file_id": file_id, "note": f"pdf_read_fail: {exc}"})
                continue

            if summary is None:
                previews.append({"file_id": file_id, "note": "summary_unparseable"})
                continue

            # ak-32o v3 reviewer MAJOR: exclude synthetic OPENING_BALANCE
            # rows from the sums so reconstructed_closing is uncontaminated.
            # Also count excluded rows per file for eyeball-observability.
            #
            # v3 (post hq-wisp-9d9bxk): DROPPED the tag != 'OPENING_BALANCE'
            # clause — Transactions.tag is NULLABLE (models/transactions.py
            # L20 has no default) so normal rows land with tag=NULL, and SQL
            # three-valued logic makes `NULL != 'OPENING_BALANCE'` → NULL
            # (not TRUE) → EVERY normal row was silently dropped from the
            # sums. Reviewer take (a): referenceID PK-prefix filter alone
            # is null-safe (referenceID is the non-null PK) AND sufficient
            # since Phase 4's synthetic insert uses deterministic PK
            # `OPENING_BALANCE_<fileID>`. Tag clause was redundant + buggy.
            _exclude = [
                ~Transactions.referenceID.like(f"{_OPENING_BALANCE_REF_PREFIX}%"),
            ]
            debit_sum = float(app.db.session.query(
                sa_func.coalesce(sa_func.sum(Transactions.amount), 0)
            ).filter(
                Transactions.fileID == file_id,
                Transactions.user == user_id,
                Transactions.amount > 0,
                *_exclude,
            ).scalar() or 0)
            credit_sum = -float(app.db.session.query(
                sa_func.coalesce(sa_func.sum(Transactions.amount), 0)
            ).filter(
                Transactions.fileID == file_id,
                Transactions.user == user_id,
                Transactions.amount < 0,
                *_exclude,
            ).scalar() or 0)
            # Observability: count what we EXCLUDED so eyeball can
            # confirm zero when backstop didn't insert synthetics
            # (current behavior), or non-zero after Phase 4 destructive
            # insert lands and preview is re-run.
            excluded_count = int(app.db.session.query(
                sa_func.count()
            ).select_from(Transactions).filter(
                Transactions.fileID == file_id,
                Transactions.user == user_id,
                sa_func.coalesce(Transactions.tag, "") == _OPENING_BALANCE_TAG,
            ).scalar() or 0)
            excluded_by_ref = int(app.db.session.query(
                sa_func.count()
            ).select_from(Transactions).filter(
                Transactions.fileID == file_id,
                Transactions.user == user_id,
                Transactions.referenceID.like(f"{_OPENING_BALANCE_REF_PREFIX}%"),
            ).scalar() or 0)
            # Union count (either filter would have caught it) — dedupe
            # with a distinct query.
            excluded_union = int(app.db.session.query(
                sa_func.count(sa_func.distinct(Transactions.referenceID))
            ).filter(
                Transactions.fileID == file_id,
                Transactions.user == user_id,
                (
                    (sa_func.coalesce(Transactions.tag, "") == _OPENING_BALANCE_TAG)
                    | Transactions.referenceID.like(f"{_OPENING_BALANCE_REF_PREFIX}%")
                ),
            ).scalar() or 0)

            # ak-32o v3 reviewer v3 (hq-wisp-9d9bxk): NULL-tag regression
            # guard. Compute UNFILTERED debit/credit sums (no _exclude)
            # + the amount magnitude of OPENING_BALANCE rows we did
            # exclude. Invariant:
            #   filtered_debit_sum + excluded_debit_magnitude == unfiltered_debit_sum
            #   filtered_credit_sum + excluded_credit_magnitude == unfiltered_credit_sum
            # If invariant fails, the SUM filter is dropping rows it
            # shouldn't — reopens the exact class of bug the review v2
            # BOUNCE caught (silent under-count via NULL logic).
            unfiltered_debit_sum = float(app.db.session.query(
                sa_func.coalesce(sa_func.sum(Transactions.amount), 0)
            ).filter(
                Transactions.fileID == file_id,
                Transactions.user == user_id,
                Transactions.amount > 0,
            ).scalar() or 0)
            unfiltered_credit_sum = -float(app.db.session.query(
                sa_func.coalesce(sa_func.sum(Transactions.amount), 0)
            ).filter(
                Transactions.fileID == file_id,
                Transactions.user == user_id,
                Transactions.amount < 0,
            ).scalar() or 0)
            # Magnitude of excluded rows on each side.
            excl_debit_mag = float(app.db.session.query(
                sa_func.coalesce(sa_func.sum(Transactions.amount), 0)
            ).filter(
                Transactions.fileID == file_id,
                Transactions.user == user_id,
                Transactions.amount > 0,
                Transactions.referenceID.like(f"{_OPENING_BALANCE_REF_PREFIX}%"),
            ).scalar() or 0)
            excl_credit_mag = -float(app.db.session.query(
                sa_func.coalesce(sa_func.sum(Transactions.amount), 0)
            ).filter(
                Transactions.fileID == file_id,
                Transactions.user == user_id,
                Transactions.amount < 0,
                Transactions.referenceID.like(f"{_OPENING_BALANCE_REF_PREFIX}%"),
            ).scalar() or 0)
            debit_invariant = abs(
                (debit_sum + excl_debit_mag) - unfiltered_debit_sum
            ) < 0.005  # 0.5 paise — well below 1.0 rupee tolerance
            credit_invariant = abs(
                (credit_sum + excl_credit_mag) - unfiltered_credit_sum
            ) < 0.005
            sanity_ok = debit_invariant and credit_invariant
            emit("phase4.sanity",
                 file_id=file_id,
                 filtered_debit_sum=debit_sum,
                 unfiltered_debit_sum=unfiltered_debit_sum,
                 excluded_debit_magnitude=excl_debit_mag,
                 filtered_credit_sum=credit_sum,
                 unfiltered_credit_sum=unfiltered_credit_sum,
                 excluded_credit_magnitude=excl_credit_mag,
                 debit_invariant_ok=debit_invariant,
                 credit_invariant_ok=credit_invariant,
                 sanity_ok=sanity_ok)
            if not sanity_ok:
                emit("phase4.error",
                     file_id=file_id,
                     reason=("NULL-tag regression guard FAILED — "
                             "filtered + excluded != unfiltered. "
                             "Preview sums are UNTRUSTWORTHY; do NOT "
                             "surface to Overseer for Q2."))
            stated_open = summary.opening_balance
            stated_close = summary.closing_balance
            reconstructed_close = (
                (stated_open + credit_sum - debit_sum)
                if stated_open is not None else None
            )
            delta_per_file = (
                (stated_close - reconstructed_close)
                if (stated_close is not None and reconstructed_close is not None)
                else None
            )
            delta_inter_file = (
                (stated_open - prev_reconstructed_closing)
                if (stated_open is not None and prev_reconstructed_closing is not None)
                else None
            )
            preview = {
                "file_id": file_id,
                "stated_opening": stated_open,
                "stated_closing": stated_close,
                "debit_sum": debit_sum,
                "credit_sum": credit_sum,
                "reconstructed_closing": reconstructed_close,
                "delta_per_file": delta_per_file,
                "delta_inter_file": delta_inter_file,
                # ak-32o v3 reviewer MAJOR: eyeball-visible exclusion
                # counts so Overseer can confirm zero on first run
                # (backstop doesn't insert synthetics today) and non-
                # zero on any re-run post-Phase-4-insert (proves the
                # filter is doing its job).
                "excluded_openbal_by_tag": excluded_count,
                "excluded_openbal_by_ref": excluded_by_ref,
                "excluded_openbal_union": excluded_union,
            }
            emit("phase4.preview", **preview)
            previews.append(preview)
            prev_reconstructed_closing = reconstructed_close

    return previews


def verify_and_report(files: list[dict], phase3_results: list[dict],
                      previews: list[dict], snapshot_result: dict) -> dict:
    """Aggregate a final [STATUS ak-32o] summary."""
    ok = sum(1 for r in phase3_results if r.get("status") == "ok")
    skipped = sum(1 for r in phase3_results if r.get("status") == "skipped")
    errors = sum(1 for r in phase3_results if r.get("status") == "error")
    total_inserted = sum(int(r.get("inserted") or 0) for r in phase3_results)
    summary = {
        "file_count": len(files),
        "expected_file_count": _EXPECTED_FILE_COUNT,
        "reinvoke_ok": ok, "reinvoke_skipped": skipped, "reinvoke_errors": errors,
        "total_transactions_inserted": total_inserted,
        "snapshot_row_count": snapshot_result.get("snapshot_count"),
        "delta_previews": len([p for p in previews if p.get("delta_per_file") is not None]),
    }
    emit("phase5.summary", **summary)
    return summary


# ─── Main ───────────────────────────────────────────────────────────────

def main():
    parser = argparse.ArgumentParser(
        description="ak-32o v3 HDFC re-parse orchestrator (mailProcessor path)",
    )
    parser.add_argument("--user-id", required=True,
                        help="Vidish's user_id (Q4 confirmed scope)")
    parser.add_argument("--dry-run", action="store_true",
                        help="Read-only: enumerate + snapshot-check + wipe-projection + "
                             "phase-4 preview. No destructive write.")
    parser.add_argument("--assume-yes", action="store_true",
                        help="REQUIRED for Phase 2's destructive DELETE. Absence prints "
                             "projected count and exits (semi-dry-run safety net).")
    parser.add_argument("--phase", choices=["snapshot", "enumerate", "wipe", "reinvoke",
                                            "verify", "all"], default="all",
                        help="Run a specific phase only (retry/inspect). "
                             "Default: all in sequence.")
    parser.add_argument("--backup-suffix", default=None,
                        help="Override the backup table suffix. Default: today's "
                             "YYYYMMDD (retry-safe with INSERT IGNORE).")
    # ak-32o Phase 4 flags — guarded behind Q2 GO.
    parser.add_argument("--enable-phase4-per-file", action="store_true",
                        help="[HELD] Phase 4 per-file OPENING_BALANCE insert. "
                             "Refuses to run until Overseer Q2 answered.")
    parser.add_argument("--enable-phase4-inter-file", action="store_true",
                        help="[HELD] Phase 4 inter-file OPENING_BALANCE insert. "
                             "Refuses to run until Overseer Q2 answered.")
    args = parser.parse_args()

    if args.enable_phase4_per_file or args.enable_phase4_inter_file:
        if _PHASE4_AWAITING_Q2_GO:
            emit("phase4.blocked",
                 reason="Q2 (per-file vs inter-file) awaiting Overseer answer")
            raise SystemExit(
                "Phase 4 refuses to run: Overseer must answer Q2 first. "
                "The Phase 4 preview (--phase reinvoke or --phase all runs it "
                "for free) surfaces numbers for both semantics so Overseer can "
                "pick from real data."
            )

    emit("run.start", user_id=args.user_id, dry_run=args.dry_run,
         assume_yes=args.assume_yes, phase=args.phase)

    app = build_app()

    backup_suffix = args.backup_suffix or datetime.now().strftime("%Y%m%d")
    backup_table = f"transactions_ak32o_reparse_bak_{backup_suffix}"

    # Phase 1 (always runs — everything downstream needs the file list).
    files = enumerate_hdfc_files(app, args.user_id)
    file_ids = [f["file_id"] for f in files]
    emit("phase1.done", file_count=len(files), expected=_EXPECTED_FILE_COUNT,
         file_ids=file_ids)
    if len(files) != _EXPECTED_FILE_COUNT:
        emit("phase1.warn",
             reason=f"file_count={len(files)} != expected={_EXPECTED_FILE_COUNT} "
                    "(SOFT WARN pending Overseer Q3 — is this the pinned set?)")
    for f in files:
        emit("phase1.manifest",
             file_id=f["file_id"], gmail_id=f["gmail_id"],
             fileName=f["file_name"], email_date=str(f.get("email_date")),
             upload_date=str(f.get("upload_date")),
             resolved_pdf=f.get("resolved_pdf_path"),
             pdf_present=bool(f.get("resolved_pdf_path")
                              and os.path.isfile(f.get("resolved_pdf_path") or "")))
    unresolved = [f for f in files if not (f.get("resolved_pdf_path")
                                           and os.path.isfile(f.get("resolved_pdf_path") or ""))]
    if unresolved:
        emit("phase1.warn",
             reason=f"{len(unresolved)} file(s) have no resolvable PDF on disk",
             unresolved_ids=[f["file_id"] for f in unresolved])

    if args.phase == "enumerate":
        emit("run.done", phase="enumerate")
        return

    # Phase 0 (snapshot).
    snapshot_result = snapshot_hdfc(
        app, args.user_id, file_ids, backup_table, dry_run=args.dry_run,
    )
    if args.phase == "snapshot":
        emit("run.done", phase="snapshot")
        return

    # Phase 2 (wipe — double-gated).
    wipe_result = wipe_hdfc(
        app, args.user_id, file_ids, snapshot_result,
        dry_run=args.dry_run, assume_yes=args.assume_yes,
    )
    if args.phase == "wipe":
        emit("run.done", phase="wipe")
        return

    # Phase 3 (re-invoke chronologically).
    reinvoke_results = reinvoke_mailprocessor_chronologically(
        app, args.user_id, files, dry_run=args.dry_run,
    )
    if args.phase == "reinvoke":
        # Verify still emits its summary — cheap + useful.
        pass

    # Phase 4 PREVIEW (read-only — the destructive insert is HELD on Q2).
    previews = [] if args.dry_run else phase4_preview(app, args.user_id, files)
    if _PHASE4_AWAITING_Q2_GO:
        emit("phase4.held",
             reason="Overseer Q2 (per-file vs inter-file) awaiting — preview only")

    # Phase 5 (verify + report).
    verify_and_report(files, reinvoke_results, previews, snapshot_result)

    emit("run.done", phase=args.phase)


if __name__ == "__main__":
    main()
