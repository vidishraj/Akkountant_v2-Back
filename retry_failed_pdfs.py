#!/usr/bin/env python3
"""Retry the 12 failed PDFs with PAGES_PER_CHUNK=1."""

import sys
import os
import time
from datetime import datetime

sys.path.insert(0, os.path.dirname(os.path.abspath(__file__)))
from dotenv import load_dotenv
load_dotenv()

from flask import g
from sqlalchemy import text

FAILED_IDS = [
    "1849cc8db7981f18", "187a91fbbf0a611c", "18b93f83bd13acf1",
    "18ea24af56da15e7", "18f3dce44a55ad73", "18fdb6c2b4f33778",
    "18fdc3d758815ee5", "19077072b37a1ced", "19116fe94646dc41",
    "19252686b52b374d", "192f1268169b4e2e", "1938b68db58ae530",
]

STATEMENTS_DIR = os.path.join(os.path.dirname(os.path.abspath(__file__)), "claude_statements")


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
    print("Building Flask app...")
    app = build_app()

    with app.app_context():
        g.db = app.db
        from models import UserToken
        from enums.ServiceTypeEnum import ServiceTypeEnum
        user_id = app.db.session.query(UserToken.user_id).filter_by(
            service_type=ServiceTypeEnum.Gmail.value
        ).first()[0]
        print(f"User: {user_id}")

    # Get PDF info for failed IDs
    with app.app_context():
        g.db = app.db
        placeholders = ",".join([f"'{x}'" for x in FAILED_IDS])
        rows = app.db.session.execute(text(f"""
            SELECT gmail_id, sender, subject, email_date, pdf_filename, category
            FROM processedEmails
            WHERE gmail_id IN ({placeholders})
            ORDER BY email_date
        """)).fetchall()

    pdfs = [dict(r._mapping) for r in rows]
    print(f"Retrying {len(pdfs)} failed PDFs (PAGES_PER_CHUNK=1)\n")

    completed = 0
    failed = 0

    for idx, pdf in enumerate(pdfs):
        gmail_id = pdf["gmail_id"]
        subject = pdf["subject"][:55]
        pdf_path = os.path.join(STATEMENTS_DIR, user_id, pdf["pdf_filename"])

        if not os.path.exists(pdf_path):
            print(f"[{idx+1}/{len(pdfs)}] SKIP - PDF not found: {pdf_path}")
            failed += 1
            continue

        email_dict = {
            "gmail_id": gmail_id,
            "sender": pdf["sender"] or "",
            "subject": pdf["subject"] or "",
            "message": "",
            "time": pdf["email_date"].strftime("%Y-%m-%d %H:%M:%S") if pdf["email_date"] else "",
            "_category": pdf["category"],
            "_local_pdf_paths": [(pdf_path, os.path.basename(pdf_path))],
        }

        print(f"{datetime.now().strftime('%H:%M:%S')} [{idx+1}/{len(pdfs)}] {subject}...",
              end="", flush=True)

        t0 = time.time()
        try:
            with app.app_context():
                g.db = app.db
                g.firebase_id = user_id
                app.mailProcessor._process_single_pdf_email(email_dict, user_id)
            print(f" done ({time.time()-t0:.0f}s)")
            completed += 1
        except Exception as e:
            print(f" FAILED ({str(e)[:70]})")
            failed += 1

        if idx + 1 < len(pdfs):
            time.sleep(3)

    print(f"\nDone: {completed} succeeded, {failed} failed")


if __name__ == "__main__":
    main()
