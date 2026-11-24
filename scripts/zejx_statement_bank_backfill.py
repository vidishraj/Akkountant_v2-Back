"""ak-zejx: re-derive the bank id of existing statement rows (backfill).

DRY-RUN by default: prints counts only (outcomes, old -> new mappings,
skipped sender DOMAINS). Never prints statement content, subjects, amounts
or addresses. Idempotent: rows already in the right bank family are
untouched, so re-running is a no-op.

What it changes (only rows the plan marks FIX; see
utils/statement_bank_backfill.py for the rules):
  * processedEmails.extraction_summary["bank"] for category='bank_statement'
    rows (what the statements view shows as the bank);
  * with --fix-linked: also fileDetails.bank and transactions.bank on the
    rows linked to that email (fileID LIKE '%\\_<gmail_id>' OR fileID in the
    fileDetails rows with gmail_message_id = <gmail_id>), only where their
    family differs from the email's. NOTE: transactions.referenceID hashes
    are NOT recomputed (they embed the bank used at ingest). A later
    re-ingest under the new bank id still dedups against these rows via
    the bank-agnostic statement dedup (ak-zejx), but only on an exact
    (date, amount, normalised description) match: a row whose description
    the model words differently on the re-read is NOT matched and will
    duplicate. Whether to touch linked rows is an Overseer decision; the
    flag is off by default.

Usage (operator / infra; the prod write needs Overseer approval):
    # 1. dry run (no writes):
    python scripts/zejx_statement_bank_backfill.py [--user-id <uid>] [--fix-linked]
    # 2. apply, after the dry-run counts are approved:
    python scripts/zejx_statement_bank_backfill.py [--user-id <uid>] [--fix-linked] \\
        --commit --assume-yes
"""
from __future__ import annotations

import argparse
import os
import sys

# ak-3oh review (same risk here): never boot the app. Importing it runs
# db.create_all() (DDL on prod) and can START the schedulers, which write to
# prod. Flag set before any project import; the session is a bare engine.
os.environ["AK_SKIP_SCHEDULERS"] = "1"

HERE = os.path.dirname(os.path.dirname(os.path.abspath(__file__)))
if HERE not in sys.path:
    sys.path.insert(0, HERE)

from utils import statement_bank_backfill as SB  # noqa: E402


def _like_escape(value):
    return str(value).replace("\\", "\\\\").replace("%", "\\%").replace("_", "\\_")


def _linked(session, user_id, gmail_id):
    """(fileDetails rows, transaction rows) linked to one statement email.

    Review minor: no loose suffix match. Rows are linked by the exact
    fileDetails.gmail_message_id join, or by the exact ingest fileID shape
    mail_pipeline_<user>_<BANK>_<gmail_id> (anchored at both ends; only
    the bank segment is a wildcard, and _ / % in ids are escaped)."""
    from sqlalchemy import or_
    from models.fileDetails import FileDetails
    from models.transactions import Transactions
    files = session.query(FileDetails).filter(
        FileDetails.user == user_id, FileDetails.gmail_message_id == gmail_id).all()
    file_ids = {f.fileID for f in files}
    pattern = f"mail\\_pipeline\\_{_like_escape(user_id)}\\_%\\_{_like_escape(gmail_id)}"
    conds = [Transactions.fileID.like(pattern, escape="\\")]
    if file_ids:
        conds.append(Transactions.fileID.in_(file_ids))
    txns = session.query(Transactions).filter(
        Transactions.user == user_id, or_(*conds)).all()
    return files, txns


def build_plan(session, user_id=None):
    from models.processedEmails import ProcessedEmails
    from services.bankFormatRules import sender_domain
    q = session.query(ProcessedEmails).filter(ProcessedEmails.category == "bank_statement")
    if user_id:
        q = q.filter(ProcessedEmails.user_id == user_id)
    plan = SB.Plan()
    for row in q.all():
        _files, txns = _linked(session, row.user_id, row.gmail_id)
        current = SB.current_bank_of(row.extraction_summary, row.category)
        new, outcome = SB.decide(row.sender, current, {t.bank for t in txns})
        plan.add((row.user_id, row.gmail_id), current, new, outcome, sender_domain(row.sender))
        if outcome == SB.FIX and row.status != "processed":
            # Review MAJOR (a): these will be RE-INGESTED (sweep / revive)
            # under the corrected bank. The referenceID hash folds the bank
            # in; the bank-agnostic re-ingest dedup catches exact
            # (date, amount, normalised description) matches only.
            plan.reingest_risk.append((row.gmail_id, len(txns)))
    return plan


def apply_plan(session, plan, fix_linked=False):
    """Apply plan.changes; returns counts. Caller commits."""
    from models.processedEmails import ProcessedEmails
    counts = {"processedEmails": 0, "fileDetails": 0, "transactions": 0}
    for (user_id, gmail_id), _old, new in plan.changes:
        row = session.query(ProcessedEmails).filter_by(user_id=user_id, gmail_id=gmail_id).one()
        summary = dict(row.extraction_summary) if isinstance(row.extraction_summary, dict) else {}
        summary["bank"] = new
        row.extraction_summary = summary
        counts["processedEmails"] += 1
        if fix_linked:
            files, txns = _linked(session, user_id, gmail_id)
            fam = SB.family(new)
            for f in files:
                if SB.family(f.bank) != fam:
                    f.bank = new
                    counts["fileDetails"] += 1
            for t in txns:
                if SB.family(t.bank) != fam:
                    t.bank = new
                    counts["transactions"] += 1
    return counts


def linked_mismatch_counts(session, plan):
    """For the dry run: linked rows whose bank family differs (would be
    touched by --fix-linked)."""
    files_n = txns_n = 0
    for (user_id, gmail_id), _old, new in plan.changes:
        files, txns = _linked(session, user_id, gmail_id)
        fam = SB.family(new)
        files_n += sum(1 for f in files if SB.family(f.bank) != fam)
        txns_n += sum(1 for t in txns if SB.family(t.bank) != fam)
    return files_n, txns_n


def main(argv=None):
    p = argparse.ArgumentParser(description=__doc__.split("\n")[0])
    p.add_argument("--user-id", help="restrict to one user (default: all statement rows)")
    p.add_argument("--fix-linked", action="store_true",
                   help="also correct fileDetails.bank / transactions.bank (see NOTE)")
    p.add_argument("--commit", action="store_true", help="write (default: dry run)")
    p.add_argument("--assume-yes", action="store_true", help="required with --commit")
    args = p.parse_args(argv)
    if args.commit and not args.assume_yes:
        print("[zejx] --commit requires --assume-yes", file=sys.stderr)
        return 2

    session, close = _open_session(read_only=not args.commit)
    try:
        return _run(session, args)
    finally:
        try:
            session.rollback()
        finally:
            close()


def _open_session(read_only):
    """A bare SQLAlchemy session on ONE connection from DATABASE_URL (no app
    init, no create_all, no schedulers). Dry runs are READ ONLY on MySQL.
    Returns (session, close)."""
    try:
        from dotenv import load_dotenv     # same .env the app reads
        load_dotenv()
    except ImportError:
        pass
    url = os.getenv("DATABASE_URL")
    if not url:
        raise SystemExit("[zejx] DATABASE_URL is not set")
    from sqlalchemy import create_engine, text
    from sqlalchemy.orm import Session
    engine = create_engine(url, pool_pre_ping=True)
    conn = engine.connect()
    if read_only and engine.dialect.name == "mysql":
        conn.execute(text("SET SESSION TRANSACTION READ ONLY"))
    session = Session(bind=conn)

    def close():
        session.close()
        conn.close()
        engine.dispose()
    return session, close


def _run(session, args):
    plan = build_plan(session, args.user_id)
    print(f"[zejx] mode={'COMMIT' if args.commit else 'DRY-RUN'} "
          f"user={'all' if not args.user_id else 'one'} fix_linked={args.fix_linked}")
    for line in plan.report_lines():
        print(f"[zejx] {line}")
    risk_txns = sum(n for _g, n in plan.reingest_risk)
    print(f"[zejx] statements to be fixed that are NOT 'processed' (will be re-ingested "
          f"under the new bank): {len(plan.reingest_risk)}, with {risk_txns} linked "
          f"transaction row(s). Re-ingest is deduped bank-agnostically (ak-zejx) on "
          f"an exact (date, amount, normalised description) match; a row whose "
          f"description is worded differently on the re-read is not matched and "
          f"WILL duplicate, so review these statements after the re-ingest.")
    files_n, txns_n = linked_mismatch_counts(session, plan)
    print(f"[zejx] linked rows with a different bank family: fileDetails={files_n} "
          f"transactions={txns_n} ({'WILL be fixed' if args.fix_linked else 'not touched without --fix-linked'})")
    if not args.commit:
        print("[zejx] DRY-RUN complete: no writes.")
        return 0
    try:
        counts = apply_plan(session, plan, fix_linked=args.fix_linked)
        session.commit()
    except Exception as exc:
        session.rollback()
        print(f"[zejx] apply failed, rolled back ({type(exc).__name__})", file=sys.stderr)
        return 1
    print(f"[zejx] COMMITTED: {counts}")
    return 0


if __name__ == "__main__":
    sys.exit(main())
