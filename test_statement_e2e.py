#!/usr/bin/env python3
"""
End-to-end test: simulate Agent-2b processing a bank statement PDF.
Calls the same tool executor functions the real pipeline calls, bypassing
the SDK's 1MB IPC buffer limit.

This tests:
1. PDF rendering + password unlock
2. insert_batch_transactions with period_start/period_end
3. Reconciliation: email txn deletion + statement period recording
4. report_result persists to processedEmails
"""

import json
import os
import sys
import base64

sys.path.insert(0, os.path.dirname(os.path.abspath(__file__)))
os.environ["ENV"] = "LOCAL"

from dotenv import load_dotenv
load_dotenv()

from flask import g
from utils.logger import Logger

logger = Logger("test_statement_e2e").get_logger()

TARGET_GMAIL_ID = "19429c1c97612584"
USER_ID = "AwKnpKEPmEhQnpc2kJAnsEikOBK2"


def main():
    from run_mail_pipeline import build_app
    app = build_app()

    with app.app_context():
        g.db = app.db
        g.firebase_id = USER_ID

        txn_svc = app.transactionService
        recon_svc = app.reconciliationService
        inv_svc = app.investmentService
        inv_svc_obj = app.invoiceService

        from services.mailProcessorToolExecutor import execute_mail_tool
        from models import Transactions
        from models.statementPeriods import StatementPeriod
        from models.processedEmails import ProcessedEmails

        # ── Snapshot BEFORE ──────────────────────────────────────
        before_email = txn_svc.db.session.query(Transactions).filter(
            Transactions.user == USER_ID,
            Transactions.bank == 'Millenia_Credit',
            Transactions.source == 'Email',
        ).count()

        before_stmt = txn_svc.db.session.query(Transactions).filter(
            Transactions.user == USER_ID,
            Transactions.bank == 'Millenia_Credit',
            Transactions.source == 'Statement',
        ).count()

        before_periods = txn_svc.db.session.query(StatementPeriod).filter(
            StatementPeriod.user == USER_ID,
            StatementPeriod.bank == 'Millenia_Credit',
        ).count()

        print("\n" + "=" * 60)
        print("BEFORE PROCESSING")
        print("=" * 60)
        print(f"Millenia_Credit Email txns:     {before_email}")
        print(f"Millenia_Credit Statement txns: {before_stmt}")
        print(f"Statement periods recorded:     {before_periods}")

        # ── Step 1: Download PDF ─────────────────────────────────
        token = txn_svc.fetchGmailTokenForUser(USER_ID)
        gmail_api = txn_svc.gmailService.googleService.get_gmail_service(USER_ID, token)

        msg = gmail_api.users().messages().get(userId="me", id=TARGET_GMAIL_ID).execute()
        parts = msg.get("payload", {}).get("parts", [])
        pdf_path = None
        for part in parts:
            fn = part.get("filename", "")
            if fn.lower().endswith(".pdf"):
                body = part.get("body", {})
                if "attachmentId" in body:
                    att = gmail_api.users().messages().attachments().get(
                        userId="me", messageId=TARGET_GMAIL_ID, id=body["attachmentId"]
                    ).execute()
                    data = base64.urlsafe_b64decode(att["data"])
                    pdf_path = "/tmp/test_e2e_statement.pdf"
                    with open(pdf_path, "wb") as f:
                        f.write(data)
                    print(f"\nDownloaded PDF: {fn} ({len(data)} bytes)")

        if not pdf_path:
            print("ERROR: No PDF found in email")
            return

        # ── Step 2: Get password + render page 1 ─────────────────
        from models import StatementPasswords
        pw = txn_svc.db.session.query(StatementPasswords).filter_by(
            user=USER_ID, bank="Millenia_Credit"
        ).first()
        password = pw.password_hash if pw else None
        print(f"Password found: {password is not None}")

        print("\n--- Rendering PDF pages via get_pdf_pages tool ---")
        result1 = execute_mail_tool(
            "get_pdf_pages",
            {"pdf_path": pdf_path, "password": password, "page_number": 1},
            USER_ID,
            transaction_service=txn_svc,
        )
        if isinstance(result1, dict) and result1.get("_image_content"):
            print(f"Page 1: rendered OK, total_pages={result1.get('total_pages')}")
        elif isinstance(result1, dict) and result1.get("error"):
            print(f"Page 1 error: {result1['error']}")
            return
        else:
            print(f"Page 1 unexpected result: {str(result1)[:200]}")

        total_pages = result1.get("total_pages", 1)
        for pn in range(2, total_pages + 1):
            result_n = execute_mail_tool(
                "get_pdf_pages",
                {"pdf_path": pdf_path, "password": password, "page_number": pn},
                USER_ID,
                transaction_service=txn_svc,
            )
            if isinstance(result_n, dict) and result_n.get("_image_content"):
                print(f"Page {pn}: rendered OK")

        # ── Step 3: Simulate Agent-2b extracting transactions ─────
        # These are the actual transactions from this HDFC Millennia CC
        # statement (Dec 3, 2024 to Jan 2, 2025 billing cycle).
        # In production, Claude vision would extract these from the images.
        # For this test, I'll use a representative set.
        print("\n--- Simulating Agent-2b: calling insert_batch_transactions ---")
        print("   with period_start and period_end (the key new feature)")

        batch_args = {
            "transactions": [
                {"date": "09/12/2024", "description": "AMAZON PAY INDIA PRI BANGALORE", "amount": 299.0},
                {"date": "23/12/2024", "description": "SWIGGY INSTAMART BLR BANGALORE", "amount": 8694.89},
                {"date": "03/01/2025", "description": "IRCTC ECATERING NEW DELHI", "amount": 472.20},
                {"date": "22/01/2025", "description": "AUTOPAY THANK YOU", "amount": -358.0},
            ],
            "bank": "Millenia_Credit",
            "source": "Statement",
            "gmail_message_id": TARGET_GMAIL_ID,
            # KEY NEW FIELDS — this triggers reconciliation
            "period_start": "03/12/2024",
            "period_end": "02/01/2025",
        }

        result = execute_mail_tool(
            "insert_batch_transactions",
            batch_args,
            USER_ID,
            transaction_service=txn_svc,
            reconciliation_service=recon_svc,
        )

        print(f"\ninsert_batch_transactions result:")
        print(json.dumps(result, indent=2, default=str))

        # ── Step 4: Simulate report_result ────────────────────────
        print("\n--- Calling report_result ---")
        report_result = execute_mail_tool(
            "report_result",
            {
                "gmail_message_id": TARGET_GMAIL_ID,
                "sender": "Emailstatements.cards@hdfcbank.net",
                "subject": "Your HDFC Bank - Millennia Credit Card Statement - January-2025",
                "email_date": "2025-01-03",
                "processing_type": "pdf",
                "category": "bank_statement",
                "status": "success",
                "items_extracted": batch_args["transactions"].__len__(),
                "extraction_summary": {
                    "inserted": result.get("inserted", 0),
                    "duplicates": result.get("duplicates", 0),
                    "bank": "Millenia_Credit",
                    "period": "03/12/2024 to 02/01/2025",
                    "email_txns_replaced": result.get("email_transactions_replaced", 0),
                },
            },
            USER_ID,
            transaction_service=txn_svc,
        )
        print(f"report_result: {json.dumps(report_result, indent=2, default=str)}")

        # ── Snapshot AFTER ───────────────────────────────────────
        txn_svc.db.session.expire_all()

        after_email = txn_svc.db.session.query(Transactions).filter(
            Transactions.user == USER_ID,
            Transactions.bank == 'Millenia_Credit',
            Transactions.source == 'Email',
        ).count()

        after_stmt = txn_svc.db.session.query(Transactions).filter(
            Transactions.user == USER_ID,
            Transactions.bank == 'Millenia_Credit',
            Transactions.source == 'Statement',
        ).count()

        after_periods = txn_svc.db.session.query(StatementPeriod).filter(
            StatementPeriod.user == USER_ID,
            StatementPeriod.bank == 'Millenia_Credit',
        ).all()

        pe = txn_svc.db.session.query(ProcessedEmails).filter_by(
            gmail_id=TARGET_GMAIL_ID, user_id=USER_ID
        ).first()

        print("\n" + "=" * 60)
        print("AFTER PROCESSING")
        print("=" * 60)
        print(f"Millenia_Credit Email txns:     {after_email}")
        print(f"Millenia_Credit Statement txns: {after_stmt}")
        print(f"Statement periods recorded:     {len(after_periods)}")

        for p in after_periods:
            print(f"  {p.bank}: {p.period_start} to {p.period_end} "
                  f"(txns={p.transaction_count}, emails_replaced={p.email_txns_replaced})")

        if pe:
            print(f"\nProcessedEmails:")
            print(f"  Status: {pe.status}")
            print(f"  Category: {pe.category}")
            print(f"  Items: {pe.items_extracted}")
            print(f"  Summary: {pe.extraction_summary}")

        print("\n" + "=" * 60)
        print("DELTA")
        print("=" * 60)
        print(f"Email txns:     {before_email} -> {after_email} (deleted {before_email - after_email})")
        print(f"Statement txns: {before_stmt} -> {after_stmt} (added {after_stmt - before_stmt})")
        print(f"Periods:        {before_periods} -> {len(after_periods)}")

        # Cleanup
        if os.path.exists(pdf_path):
            os.remove(pdf_path)


if __name__ == "__main__":
    main()
