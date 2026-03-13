#!/usr/bin/env python3
"""
Reprocess only ICICI_AMAZON_PAY bank statement PDFs.
Assumes ICICI Statement transactions and filedetails have already been cleaned.
"""

import os
import sys
import time
import signal
from datetime import datetime

sys.path.insert(0, os.path.dirname(os.path.abspath(__file__)))

from dotenv import load_dotenv
load_dotenv()

from flask import g
from sqlalchemy import text

BASE_DIR = os.path.dirname(os.path.abspath(__file__))
STATEMENTS_DIR = os.path.join(BASE_DIR, "claude_statements")
BASE_DELAY = 3


def build_app():
    original_env = os.environ.get("ENV")
    os.environ["ENV"] = "LOCAL"
    from app import Akkountant
    app = Akkountant(__name__)
    if original_env is not None:
        os.environ["ENV"] = original_env
    else:
        os.environ.pop("ENV", None)
    return app


def main():
    app = build_app()

    with app.app_context():
        g.db = app.db
        from models import UserToken
        from enums.ServiceTypeEnum import ServiceTypeEnum
        users = app.db.session.query(UserToken.user_id).filter_by(
            service_type=ServiceTypeEnum.Gmail.value
        ).distinct().all()
        user_id = users[0][0]
        print(f"User: {user_id}")

    # Get ICICI PDFs
    with app.app_context():
        g.db = app.db
        rows = app.db.session.execute(text("""
            SELECT gmail_id, sender, subject, email_date, pdf_filename, category,
                   JSON_UNQUOTE(JSON_EXTRACT(extraction_summary, '$.bank')) as bank
            FROM processedEmails
            WHERE category = 'bank_statement'
              AND pdf_filename IS NOT NULL AND pdf_filename != ''
              AND user_id = :uid
              AND (subject LIKE '%ICICI%' OR subject LIKE '%Amazon Pay ICICI%')
            ORDER BY email_date
        """), {"uid": user_id}).fetchall()

    pdfs = [dict(row._mapping) for row in rows]
    print(f"Found {len(pdfs)} ICICI PDFs to reprocess\n")

    completed = []
    failed = []
    start_time = time.time()
    interrupted = False

    def signal_handler(sig, frame):
        nonlocal interrupted
        interrupted = True
        print("\nInterrupted!")
        print_summary()
        sys.exit(0)

    def print_summary():
        elapsed = time.time() - start_time
        print(f"\n{'='*60}")
        print(f"ICICI REPROCESS SUMMARY")
        print(f"{'='*60}")
        print(f"  Runtime:   {elapsed/60:.1f} min")
        print(f"  OK:        {len(completed)}")
        print(f"  Failed:    {len(failed)}")
        for f in failed:
            print(f"    - {f}")
        print(f"{'='*60}")

    signal.signal(signal.SIGINT, signal_handler)

    for idx, pdf in enumerate(pdfs):
        if interrupted:
            break

        gmail_id = pdf["gmail_id"]
        pdf_rel = pdf["pdf_filename"]
        subject = pdf["subject"] or ""
        sender = pdf["sender"] or ""
        email_date = pdf["email_date"]
        category = pdf["category"]

        user_dir = os.path.join(STATEMENTS_DIR, user_id)
        pdf_path = os.path.join(user_dir, pdf_rel)

        label = f"[{idx+1}/{len(pdfs)}]"
        short = subject[:55]
        print(f"{datetime.now().strftime('%H:%M:%S')} {label} {short}...", end="", flush=True)

        if not os.path.exists(pdf_path):
            failed.append(f"{gmail_id}: PDF not found")
            print(" MISSING")
            continue

        email_dict = {
            "gmail_id": gmail_id,
            "sender": sender,
            "subject": subject,
            "message": "",
            "time": email_date.strftime("%Y-%m-%d %H:%M:%S") if email_date else "",
            "_category": category,
            "_local_pdf_paths": [(pdf_path, os.path.basename(pdf_path))],
        }

        t0 = time.time()
        with app.app_context():
            g.db = app.db
            g.firebase_id = user_id
            try:
                app.mailProcessor._process_single_pdf_email(email_dict, user_id)
                completed.append(gmail_id)
                print(f" done ({time.time()-t0:.0f}s)")
            except Exception as e:
                failed.append(f"{gmail_id}: {str(e)[:80]}")
                print(f" FAILED ({str(e)[:60]})")

        if idx + 1 < len(pdfs) and not interrupted:
            time.sleep(BASE_DELAY)

    print_summary()


if __name__ == "__main__":
    main()
