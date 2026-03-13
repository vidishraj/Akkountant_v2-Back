#!/usr/bin/env python3
"""
Reprocess all saved bank statement PDFs through the PDF agent.

1. Deletes all Statement-source transactions and resets filedetails
2. Reads each saved PDF from disk
3. Passes it directly to the PDF processing pipeline (skipping email fetch + classification)

Usage:
    python3 reprocess_pdfs.py              # Full run
    python3 reprocess_pdfs.py --dry-run    # Preview what would be done
    python3 reprocess_pdfs.py --resume-from <gmail_id>  # Resume from a specific email
"""

import argparse
import signal
import sys
import os
import time
from datetime import datetime

sys.path.insert(0, os.path.dirname(os.path.abspath(__file__)))

from dotenv import load_dotenv
load_dotenv()

from flask import g
from sqlalchemy import text
from utils.logger import Logger

logger = Logger("reprocess_pdfs").get_logger()

BASE_DIR = os.path.dirname(os.path.abspath(__file__))
STATEMENTS_DIR = os.path.join(BASE_DIR, "claude_statements")
BASE_DELAY = 3  # seconds between PDFs


def build_app():
    """Build Flask app without HTTP server."""
    original_env = os.environ.get("ENV")
    os.environ["ENV"] = "LOCAL"

    from app import Akkountant
    app = Akkountant(__name__)

    if original_env is not None:
        os.environ["ENV"] = original_env
    else:
        os.environ.pop("ENV", None)

    return app


def get_pdfs_to_process(app, user_id):
    """Get list of bank_statement processedEmails with saved PDFs."""
    with app.app_context():
        g.db = app.db
        rows = app.db.session.execute(text("""
            SELECT gmail_id, sender, subject, email_date, pdf_filename, category,
                   JSON_UNQUOTE(JSON_EXTRACT(extraction_summary, '$.bank')) as bank
            FROM processedEmails
            WHERE category = 'bank_statement'
              AND pdf_filename IS NOT NULL AND pdf_filename != ''
              AND user_id = :uid
            ORDER BY email_date
        """), {"uid": user_id}).fetchall()

    return [dict(row._mapping) for row in rows]


def nuke_statement_transactions(app):
    """Delete all Statement-source transactions and reset filedetails counts."""
    with app.app_context():
        g.db = app.db

        stmt_count = app.db.session.execute(
            text("SELECT COUNT(*) FROM transactions WHERE source = 'Statement'")
        ).scalar()

        # Delete Statement transactions
        app.db.session.execute(text("DELETE FROM transactions WHERE source = 'Statement'"))

        # Reset statementCount in filedetails for mail_pipeline entries
        app.db.session.execute(text("""
            UPDATE filedetails SET statementCount = 0
            WHERE fileID LIKE 'mail_pipeline_%'
        """))

        app.db.session.commit()

        print(f"  Deleted {stmt_count} Statement transactions")
        print(f"  Reset filedetails statementCounts")
        return stmt_count


def process_single_pdf(app, user_id, pdf_info):
    """Process a single saved PDF through the pipeline."""
    gmail_id = pdf_info["gmail_id"]
    pdf_rel_path = pdf_info["pdf_filename"]
    sender = pdf_info["sender"] or ""
    subject = pdf_info["subject"] or ""
    email_date = pdf_info["email_date"]
    category = pdf_info["category"]

    # Build path: claude_statements/<user_id>/<pdf_rel_path>
    user_dir = os.path.join(STATEMENTS_DIR, user_id)
    pdf_path = os.path.join(user_dir, pdf_rel_path)
    if not os.path.exists(pdf_path):
        return {"status": "error", "message": f"PDF not found: {pdf_path}"}

    # Build the email dict that _process_single_pdf_email expects
    email_dict = {
        "gmail_id": gmail_id,
        "sender": sender,
        "subject": subject,
        "message": "",
        "time": email_date.strftime("%Y-%m-%d %H:%M:%S") if email_date else "",
        "_category": category,
        "_local_pdf_paths": [(pdf_path, os.path.basename(pdf_path))],
    }

    with app.app_context():
        g.db = app.db
        g.firebase_id = user_id

        try:
            app.mailProcessor._process_single_pdf_email(email_dict, user_id)
            return {"status": "success"}
        except Exception as e:
            return {"status": "error", "message": str(e)}


def main():
    parser = argparse.ArgumentParser(description="Reprocess saved bank statement PDFs")
    parser.add_argument("--dry-run", action="store_true", help="Preview only")
    parser.add_argument("--resume-from", default=None, help="Resume from gmail_id")
    parser.add_argument("--user", default=None, help="Firebase user ID")
    args = parser.parse_args()

    # State tracking
    completed = []
    failed = []
    start_time = time.time()
    interrupted = False

    def signal_handler(sig, frame):
        nonlocal interrupted
        interrupted = True
        print("\n\nInterrupted! Printing summary...\n")
        print_summary()
        sys.exit(0)

    def print_summary():
        elapsed = time.time() - start_time
        print("\n" + "=" * 70)
        print("REPROCESS SUMMARY")
        print("=" * 70)
        print(f"  Runtime:     {elapsed / 60:.1f} minutes")
        print(f"  Completed:   {len(completed)} / {len(pdfs)}")
        print(f"  Failed:      {len(failed)}")
        if failed:
            for f in failed:
                print(f"    - {f}")
        if completed:
            print(f"  Last done:   {completed[-1]}")
        print("=" * 70)

    signal.signal(signal.SIGINT, signal_handler)
    signal.signal(signal.SIGTERM, signal_handler)

    # Build app
    print("Building Flask app...")
    app = build_app()

    # Resolve user
    with app.app_context():
        g.db = app.db
        if args.user:
            user_id = args.user
        else:
            from models import UserToken
            from enums.ServiceTypeEnum import ServiceTypeEnum
            users = app.db.session.query(UserToken.user_id).filter_by(
                service_type=ServiceTypeEnum.Gmail.value
            ).distinct().all()
            if not users:
                print("ERROR: No users found.")
                sys.exit(1)
            user_id = users[0][0]
            print(f"Using user: {user_id}")

    # Get PDFs to process
    pdfs = get_pdfs_to_process(app, user_id)
    print(f"Found {len(pdfs)} bank statement PDFs to reprocess")

    # Apply resume filter
    if args.resume_from:
        idx = next((i for i, p in enumerate(pdfs) if p["gmail_id"] == args.resume_from), None)
        if idx is None:
            print(f"ERROR: gmail_id {args.resume_from} not found")
            sys.exit(1)
        pdfs = pdfs[idx:]
        print(f"Resuming from {args.resume_from}: {len(pdfs)} remaining")

    if args.dry_run:
        print("\n[DRY RUN] PDFs to process:")
        for i, p in enumerate(pdfs, 1):
            bank = p.get("bank", "?")
            print(f"  {i:3d}. [{bank}] {p['subject'][:60]} ({p['pdf_filename']})")
        return

    # Nuke existing Statement transactions
    print("\nDeleting existing Statement transactions...")
    nuke_statement_transactions(app)

    # Process each PDF
    print(f"\nProcessing {len(pdfs)} PDFs at {datetime.now().strftime('%H:%M:%S')}...")
    print("=" * 70)

    for idx, pdf in enumerate(pdfs):
        if interrupted:
            break

        label = f"[{idx + 1}/{len(pdfs)}]"
        subject_short = pdf["subject"][:50]
        bank = pdf.get("bank", "?")

        print(f"{datetime.now().strftime('%H:%M:%S')} {label} [{bank}] {subject_short}...",
              end="", flush=True)

        t0 = time.time()
        result = process_single_pdf(app, user_id, pdf)
        elapsed = time.time() - t0

        if result["status"] == "success":
            completed.append(f"{pdf['gmail_id']} - {subject_short}")
            print(f" done ({elapsed:.0f}s)")
        else:
            failed.append(f"{pdf['gmail_id']}: {result.get('message', '')[:80]}")
            print(f" FAILED ({result.get('message', '')[:60]})")

        # Delay between PDFs
        if idx + 1 < len(pdfs) and not interrupted:
            time.sleep(BASE_DELAY)

        # Progress every 10
        if (idx + 1) % 10 == 0:
            total_elapsed = time.time() - start_time
            avg = total_elapsed / (idx + 1)
            remaining = avg * (len(pdfs) - idx - 1)
            print(f"    >>> Progress: {idx + 1}/{len(pdfs)}, "
                  f"~{remaining / 60:.0f}min remaining")

    print_summary()


if __name__ == "__main__":
    main()
