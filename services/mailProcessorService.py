"""
Unified AI-powered email processing pipeline.
Reads financial emails via Gmail API, classifies them with Claude, extracts data,
and inserts into the database via MCP tools.

Uses claude_agent_sdk following the same pattern as AgentService and CronAgent.
"""

import hashlib
import json
import os
import base64
import shutil
import tempfile
from datetime import datetime
from urllib.parse import urlparse, parse_qs

import anyio
import fitz  # PyMuPDF
from flask import g

from claude_agent_sdk import (
    ClaudeAgentOptions,
    SdkMcpTool,
    create_sdk_mcp_server,
)

from services.mailProcessorTools import (
    MAIL_PROCESSOR_TOOLS,
    TEXT_EMAIL_SYSTEM_PROMPT,
    PDF_SYSTEM_PROMPT,
)
from services.mailProcessorToolExecutor import execute_mail_tool
from utils.PdfPasswordUtil import generate_password_candidates, try_unlock_pdf, extract_password_hint
from utils.logger import Logger
from utils.sdk_runner import run_query_collect

MCP_SERVER_NAME = "mail_processor_tools"
SAVED_EMAILS_DIR = os.path.join(os.path.dirname(os.path.dirname(os.path.abspath(__file__))), "saved_emails")
MAX_TURNS = 50  # Higher than interactive agent — autonomous processing needs more turns

# Batch size for text emails — balance between cost and context window
TEXT_BATCH_SIZE = 15

# Batch size for Agent-1 classification — lightweight (sender+subject+snippet only)
CLASSIFICATION_BATCH_SIZE = 50

# Agent-1 system prompt — classifies ALL emails, replaces domain pre-filter
CLASSIFICATION_SYSTEM_PROMPT = """You are a financial email classifier for an Indian personal finance app. You receive a numbered list of emails and must identify EVERY financial email. Missing a real financial email is worse than including a borderline one.

TASK: Return a JSON array of objects for each financial email:
[{"gmail_id": "<gmail_id>", "category": "<category>"}, ...]

CATEGORIES — classify each financial email into exactly one:

transaction_alert
  Bank debit/credit notifications, UPI transaction alerts, card swipe alerts,
  balance update notifications, account update alerts, payment received/sent confirmations,
  NEFT/RTGS/IMPS confirmations, standing instruction executions, auto-debit notifications.
  KEY: any email from a bank confirming money moved or showing a balance change.

bank_statement
  Monthly/periodic bank account statements, credit card statements (usually PDF attachments).
  Includes e-statements from any bank (HDFC, ICICI, SBI, YES Bank, Bank of Baroda, BOI, etc.).

investment_confirmation
  Stock/MF/ETF buy/sell trade confirmations, contract notes, SIP execution confirmations,
  XSIP mandate registrations, demat account transaction notifications (CDSL/NSDL),
  e-voting notifications for held securities, folio verification emails.

brokerage_statement
  Weekly/monthly/quarterly equity or demat account statements, margin statements,
  retention statements, holdings statements, P&L reports, funds/securities balance alerts
  from exchanges (BSE, NSE) or brokers (Zerodha, Angel One, Groww).
  Also: mutual fund portfolio disclosure reports (e.g. PPFAS, BOI MF, HDFC MF).

epf_passbook
  EPF/PF passbook updates, EPFO contribution notifications.

nps_statement
  NPS account statements, contribution confirmations, tier-I/tier-II updates.
  NOT password expiry alerts or informational policy notices from NPS/Protean.

gold_receipt
  Digital gold purchase/sale receipts (MMTC-PAMP, Augmont, SafeGold).

freelance_payment
  Freelance payment receipts, invoice payment notifications from PayPal, Wise, Stripe, Razorpay.

freelance_contract
  Freelance contract, SOW (Statement of Work), agreement, NDA, or engagement letter emails from clients.

freelance_correspondence
  General freelance client correspondence — project updates, scope discussions, deliverable feedback, meeting requests from known freelance clients.

ALWAYS INCLUDE — these ARE financial even though they may look routine:
- "You have done a UPI txn" / "Account update for your Bank A/c" → transaction_alert
- "New A/c Balance Notification" / "Balance update" → transaction_alert
- "Payment received on your Credit Card" → transaction_alert
- "Monthly Portfolio Disclosure" from any AMC → brokerage_statement
- "Funds / Securities Balance" from BSE/NSE → brokerage_statement
- "Transactions In Your Demat Account" from CDSL/NSDL → investment_confirmation

NEVER INCLUDE — these are NOT financial:
- EMI/loan amortization schedules (e.g. "EMI on Card", "EMI conversion", "EMI amortization schedule") — these are NOT bank statements
- Promotional/marketing emails (offers, cashback, rewards expiry, card upgrade promos)
- Market newsletters and commentary (daily market roundups, stock tips, "Nifty rises", "Rupee falls")
- Insurance marketing (health/auto/travel insurance promos from ICICI Lombard, etc.)
- OTP / verification codes / password expiry alerts
- Security alerts (login attempts, MPIN blocked, password changed)
- Bank service downtime alerts ("Scheduled Downtime Alert")
- Feedback surveys ("we'd love to hear from you")
- Credit card renewal reminders (promotional, not a transaction)
- App notifications (CRED, PhonePe, Google Pay)
- Shopping/travel/food (AJIO, Flipkart, Swiggy, BookMyShow, IRCTC, YouTube)
- Apartment/society bookings (MyGate, NoBroker)
- Job/career emails (LinkedIn, LeetCode, Naukri)
- Social media notifications

If no emails are financial, return: []
Respond with ONLY valid JSON — no markdown, no explanation, no extra text."""


class MailProcessorService:
    """
    Core orchestrator for the unified email processing pipeline.
    Fetches emails, pre-filters by financial domain, classifies via Claude,
    extracts data, and inserts into the database.
    """

    def __init__(self, flask_app, transaction_service, investment_service,
                 invoice_service=None, reconciliation_service=None):
        self.logger = Logger(__name__).get_logger()
        self.flask_app = flask_app
        self.transaction_service = transaction_service
        self.investment_service = investment_service
        self.invoice_service = invoice_service
        self.reconciliation_service = reconciliation_service

    def reprocess_pdf(self, user_id, gmail_id, pdf_path=None, password=None,
                      processing_mode="text", bank="HDFC_DEBIT", only_chunks=None):
        """Reprocess a saved PDF directly — bypasses email fetching.

        If pdf_path is not given, searches claude_statements for the file.
        Automatically tries password candidates if password is not given.
        bank: Bank identifier for format rule injection (default HDFC_DEBIT).
        only_chunks: Optional list of [start, end] page ranges to process,
            e.g. [[31,32],[11,12]]. Skips all other chunks.

        ak-uvy blocking guarantee: this method is SYNCHRONOUS. All
        chunk processing happens inside anyio.run(...) inside
        _run_pdf_analysis (which is called below), so this method
        does not return until every chunk has finished (success or
        failure). Direct callers (scripts, backfill drivers) can
        trust the return value the moment reprocess_pdf returns.
        The async completion race that leaked 618 rows in the
        ak-32o run only affects callers that hit the HTTP endpoint
        /readEmails (which spawns a thread); use
        TransactionController.wait_for_scan_completion(scan_id) or
        GET /readEmails/status?scan_id=…&wait=true to block on
        that path.
        """
        import glob as _glob

        # 1. Resolve PDF path
        if not pdf_path:
            base = os.path.join(os.getcwd(), "claude_statements", user_id)
            matches = _glob.glob(os.path.join(base, "**", f"{gmail_id}_*"), recursive=True)
            if not matches:
                return {"error": f"No saved PDF found for gmail_id {gmail_id}"}
            pdf_path = matches[0]
            self.logger.info(f"Resolved PDF: {pdf_path}")

        if not os.path.exists(pdf_path):
            return {"error": f"PDF not found: {pdf_path}"}

        # 2. Try password if needed
        import fitz as _fitz
        doc = _fitz.open(pdf_path)
        needs_pass = doc.needs_pass
        doc.close()

        if needs_pass and not password:
            # ak-nyd: try the per-file override FIRST. Some PDFs are
            # protected with a value that doesn't match the bank's
            # personal-info strategy (e.g. F6 Jan-2026 HDFC, Overseer
            # set a custom password). The per_file_passwords config
            # is a JSON-backed dict keyed on gmail_id and/or fileID.
            # Falls through cleanly if the config is absent or has no
            # matching entry.
            try:
                from utils.per_file_passwords import lookup_password
                password = lookup_password(gmail_id=gmail_id)
                if password:
                    self.logger.info(
                        f"ak-nyd: applying per-file password override "
                        f"for gmail_id={gmail_id!r}"
                    )
            except Exception as e:
                self.logger.warning(
                    f"ak-nyd per-file password lookup failed: {e}"
                )
                password = None

            # Fall back to the bank's personal-info strategy.
            if not password:
                try:
                    password = self._try_personal_info_passwords(user_id, pdf_path)
                except Exception as e:
                    self.logger.warning(f"Password lookup failed: {e}")
                    password = None
            if not password:
                return {"error": "PDF is password-protected and no password could be determined"}
            # Pre-unlock
            from utils.PdfPasswordUtil import try_unlock_pdf
            doc = _fitz.open(pdf_path)
            doc.authenticate(password)
            unlocked_path = pdf_path.replace(".pdf", "_unlocked.pdf")
            doc.save(unlocked_path)
            doc.close()
            pdf_path = unlocked_path
            password = None  # Already unlocked
            self.logger.info(f"Pre-unlocked PDF: {pdf_path}")

        # 3. Build minimal email dict for the pipeline
        email = {
            "gmail_id": gmail_id,
            "sender": "reprocess@local",
            "subject": f"Reprocessed PDF {gmail_id}",
            "date": "",
            "_bank": bank,  # Override for bank detection
        }

        # 4. Run analysis. ak-bwe v3 gave us a summary dict
        # {"success", "failed_chunks", "total_chunks", "reason"}
        # that ak-1rh (this fix) now consumes to build a structured
        # return.
        self.logger.info(f"Reprocessing PDF {gmail_id} in {processing_mode} mode: {pdf_path}")
        analysis_summary = self._run_pdf_analysis(
            pdf_path, email, user_id, password=password,
            processing_mode=processing_mode, only_chunks=only_chunks,
        )

        # 5. Check results — find the actual file_id from inserted transactions
        from models.transactions import Transactions
        from sqlalchemy import func
        db = self.transaction_service.db
        file_id_row = db.session.query(Transactions.fileID).filter(
            Transactions.user == user_id,
            Transactions.fileID.like(f"%{gmail_id}%"),
        ).first()
        file_id = file_id_row[0] if file_id_row else f"mail_pipeline_{user_id}_UNKNOWN_{gmail_id}"
        count = db.session.query(func.count(Transactions.referenceID)).filter(
            Transactions.fileID == file_id,
            Transactions.user == user_id,
        ).scalar()

        # ── ak-1rh: structured return contract ─────────────────────
        # Pre-ak-1rh returned {"status": "success", ...} unconditionally.
        # ak-32o run 1 reported all 12 files "success" while files
        # 4/7/8/9 silently produced 0 tx (chunks died with MySQL
        # 1040 / SDK Fatal error). Callers had no way to detect
        # failure without post-hoc DB counting. Now the status field
        # differentiates success / partial / failed / empty so any
        # caller can meaningfully assess correctness.
        from utils.reprocess_status import summarize_reprocess_result
        result = summarize_reprocess_result(
            analysis_summary, transactions_inserted=count,
        )
        # Enrich with the fields callers depended on pre-ak-1rh so
        # the diff to existing consumers stays compatible.
        result["gmail_id"] = gmail_id
        result["file_id"] = file_id
        result["processing_mode"] = processing_mode

        self.logger.info(
            f"ak-1rh: reprocess_pdf finished gmail_id={gmail_id!r} "
            f"status={result['status']!r} "
            f"chunks_ok={result['chunks_ok']}/{result['chunks_total']} "
            f"chunks_failed={result['chunks_failed']} "
            f"transactions_inserted={result['transactions_inserted']} "
            f"reason={result.get('analysis_reason')!r}"
        )
        return result

    def process_emails(self, user_id, date_from, date_to, processing_mode="image",
                       progress_callback=None):
        """
        Main entry point. Fetches emails, filters, and processes.
        Returns a summary dict with counts of processed items.
        processing_mode: "image" (vision-based) or "text" (text extraction)
        progress_callback: optional callable(dict) to report progress updates
        """
        summary = {
            "total_emails_fetched": 0,
            "financial_emails": 0,
            "text_emails_processed": 0,
            "pdf_emails_processed": 0,
            "errors": [],
        }

        def report(update):
            if progress_callback:
                try:
                    progress_callback(update)
                except Exception:
                    pass

        try:
            # 1. Fetch Gmail token
            report({"stage": "fetching_emails", "status": "processing"})
            token = self.transaction_service.fetchGmailTokenForUser(user_id)

            # 2. Fetch all emails in range
            gmail_svc = self.transaction_service.gmailService
            all_emails = list(gmail_svc.findAllEmailsInInterval(user_id, token, date_from, date_to))
            summary["total_emails_fetched"] = len(all_emails)
            self.logger.info(f"Fetched {len(all_emails)} emails for {date_from} to {date_to}")
            report({"stage": "fetched_emails", "total_emails_fetched": len(all_emails)})

            # 2.5 Pre-filter: skip emails already processed (before classification)
            unprocessed = self._filter_already_processed(all_emails, user_id)
            pre_skipped = len(all_emails) - len(unprocessed)
            if pre_skipped > 0:
                self.logger.info(f"Pre-filter: skipped {pre_skipped} already-processed emails, {len(unprocessed)} remaining")
                summary["pre_skipped"] = pre_skipped
            report({"pre_skipped": pre_skipped})

            if not unprocessed:
                self.logger.info("All emails already processed, nothing to classify")
                return summary

            # 3. Classify remaining emails via Claude (Agent-1)
            report({"stage": "classifying", "status": "processing"})
            financial_emails = self._classify_emails(unprocessed)
            summary["financial_emails"] = len(financial_emails)
            self.logger.info(f"Agent-1 classified {len(financial_emails)} financial emails")
            report({"stage": "classified", "emails_classified": len(financial_emails)})

            # 3.1 Save classified emails to disk
            self._save_emails_to_disk(financial_emails, user_id, date_from, date_to)

            if not financial_emails:
                self.logger.info("No financial emails found in the date range")
                return summary

            # 4. Separate emails with PDF attachments from text-only
            text_emails, pdf_emails = self._separate_emails(financial_emails, user_id, token)
            self.logger.info(f"Split: {len(text_emails)} text emails, {len(pdf_emails)} PDF emails")
            report({
                "stage": "processing_emails",
                "text_emails_total": len(text_emails),
                "pdf_emails_total": len(pdf_emails),
            })

            # 5. Process text emails in batches
            if text_emails:
                text_count = self._process_text_emails_batch(text_emails, user_id)
                summary["text_emails_processed"] = text_count
                report({"text_emails_processed": text_count})

            # 6. Process PDF emails individually
            if pdf_emails:
                pdf_count = self._process_pdf_emails(pdf_emails, user_id, processing_mode,
                                                     progress_callback=progress_callback)
                summary["pdf_emails_processed"] = pdf_count
                report({"pdf_emails_processed": pdf_count})

        except Exception as e:
            self.logger.error(f"Mail processing pipeline error: {e}", exc_info=True)
            summary["errors"].append(str(e))

        self.logger.info(f"Mail processing complete: {summary}")
        return summary

    @staticmethod
    def _extract_domain(sender):
        """Extract domain from sender string like 'Bank <noreply@hdfcbank.net>'."""
        if "@" in sender:
            # Handle "Name <email@domain>" format
            at_part = sender.split("@")[-1]
            # Remove any trailing > or whitespace
            return at_part.strip().rstrip(">").lower()
        return ""

    @staticmethod
    def _lookup_domain_map(domain_map, sender_domain):
        """Subdomain-aware lookup against a {domain: [bank_ids]} map.

        ak-x6p fix (hq-wisp-i30l3w): the password-lookup helpers below
        maintain their own local dicts (they carry per-domain
        bank-order hints that differ from bankFormatRules.DOMAIN_TO_BANK,
        so we don't consolidate them). Historically both hit sender's
        raw domain against the map with exact match — which misses
        subdomain-prefixed senders like `alerts.bankofindia.bank.in`.

        Now:
          - Exact match first (backward-compatible: preserves the
            ordered list of bank ids the map author encoded).
          - Dot-boundary suffix match otherwise, so
            `alerts.bankofindia.bank.in` resolves to
            `bankofindia.bank.in`'s entry list.
        Returns a list of bank ids or [] if unresolved.
        """
        if not sender_domain:
            return []
        domain = sender_domain.strip().rstrip(">").lower()
        if not domain:
            return []
        # Exact match — preserves prior ordering + behavior for
        # senders whose domain IS a canonical key.
        exact = domain_map.get(domain)
        if exact:
            return list(exact)
        # Dot-boundary suffix match — same rule get_bank_from_sender
        # uses. Guards `evilbank.foo.bank.in` from silently matching
        # `foo.bank.in`.
        for known_domain, bank_ids in domain_map.items():
            if domain.endswith("." + known_domain):
                return list(bank_ids)
        return []

    # ── Agent-1: Claude-powered classification ─────────────────────

    def _classify_emails(self, all_emails):
        """
        Agent-1: Classify ALL emails via Claude haiku (no tools, pure text→JSON).
        Returns only the emails classified as financial, with '_category' attached.
        """
        if not all_emails:
            return []

        # Build a gmail_id → email lookup for fast retrieval
        email_lookup = {}
        for e in all_emails:
            gid = e.get("gmail_id") or e.get("message_id")
            if gid:
                email_lookup[gid] = e

        financial_emails = []

        for i in range(0, len(all_emails), CLASSIFICATION_BATCH_SIZE):
            batch = all_emails[i:i + CLASSIFICATION_BATCH_SIZE]
            batch_num = i // CLASSIFICATION_BATCH_SIZE + 1
            self.logger.info(
                f"Classifying batch {batch_num} ({len(batch)} emails)"
            )

            try:
                classified = self._run_classification_batch(batch)
                for item in classified:
                    gid = item.get("gmail_id")
                    category = item.get("category", "unknown")
                    if gid and gid in email_lookup:
                        email = email_lookup[gid]
                        email["_category"] = category
                        financial_emails.append(email)
            except Exception as e:
                self.logger.error(
                    f"Classification batch {batch_num} failed: {e}", exc_info=True
                )

        return financial_emails

    def _run_classification_batch(self, batch):
        """
        Run a single classification batch through Claude haiku.
        Returns list of dicts: [{"gmail_id": "...", "category": "..."}, ...]
        """
        # Format emails as a lightweight numbered list
        lines = []
        for i, email in enumerate(batch, 1):
            lines.append(
                f"{i}. gmail_id: {email.get('gmail_id', 'N/A')}\n"
                f"   From: {email.get('sender', 'Unknown')}\n"
                f"   Subject: {email.get('subject', 'No subject')}\n"
                f"   Date: {email.get('time', 'Unknown')}\n"
                f"   Snippet: {email.get('message', '')[:200]}"
            )
        email_text = "\n\n".join(lines)

        options = ClaudeAgentOptions(
            model="opus",
            system_prompt=CLASSIFICATION_SYSTEM_PROMPT,
            max_turns=1,
            permission_mode="bypassPermissions",
        )

        prompt_text = (
            f"Classify the following {len(batch)} emails. "
            f"Return a JSON array of only the financial ones.\n\n"
            f"{email_text}"
        )

        async def make_prompt():
            yield {
                "type": "user",
                "session_id": "",
                "message": {"role": "user", "content": prompt_text},
                "parent_tool_use_id": None,
            }

        result_box = {"result": None, "timeout": False}

        async def run_with_timeout():
            import asyncio
            try:
                result_box["result"] = await asyncio.wait_for(
                    run_query_collect(
                        agent="mail.classify", options=options, prompt=make_prompt(),
                    ),
                    timeout=120,
                )
            except asyncio.TimeoutError:
                result_box["timeout"] = True

        anyio.run(run_with_timeout)

        if result_box["timeout"]:
            err = "Classification error: rate_limit"
            self.logger.error(err)
            raise RuntimeError(err)

        result = result_box["result"]
        if result.error:
            err = f"Classification error: {result.error}"
            self.logger.error(err)
            if "rate_limit" in err:
                raise RuntimeError(err)
            return []

        raw = result.text.strip()
        if not raw:
            self.logger.warning("Classification returned empty response")
            return []

        return self._parse_classification_response(raw)

    def _parse_classification_response(self, raw):
        """Parse JSON array from Claude's classification response."""
        import re

        # Try direct parse
        try:
            result = json.loads(raw)
            if isinstance(result, list):
                return result
        except json.JSONDecodeError:
            pass

        # Try inside ```json ... ```
        fence = re.search(r'```(?:json)?\s*(\[.*?\])\s*```', raw, re.DOTALL)
        if fence:
            try:
                return json.loads(fence.group(1))
            except json.JSONDecodeError:
                pass

        # Try first [ to last ]
        first = raw.find('[')
        last = raw.rfind(']')
        if first != -1 and last > first:
            try:
                return json.loads(raw[first:last + 1])
            except json.JSONDecodeError:
                pass

        self.logger.warning(f"Could not parse classification response: {raw[:300]}")
        return []

    # ── Save emails to disk ───────────────────────────────────────────

    def _save_emails_to_disk(self, emails, user_id, date_from, date_to):
        """
        Save classified financial emails as a JSON file on disk.
        File: saved_emails/<user_id>/<date_from>_to_<date_to>_<timestamp>.json
        """
        if not emails:
            return

        user_dir = os.path.join(SAVED_EMAILS_DIR, user_id)
        os.makedirs(user_dir, exist_ok=True)

        # Sanitize date strings for filename (2025/01/01 → 2025-01-01)
        safe_from = date_from.replace("/", "-")
        safe_to = date_to.replace("/", "-")
        ts = datetime.now().strftime("%Y%m%d_%H%M%S")
        filename = f"{safe_from}_to_{safe_to}_{ts}.json"
        filepath = os.path.join(user_dir, filename)

        # Build serializable records — strip internal keys
        records = []
        for e in emails:
            records.append({
                "gmail_id": e.get("gmail_id") or e.get("message_id"),
                "sender": e.get("sender", ""),
                "subject": e.get("subject", ""),
                "date": e.get("time", ""),
                "body": e.get("message", ""),
                "category": e.get("_category", ""),
            })

        try:
            with open(filepath, "w", encoding="utf-8") as f:
                json.dump(records, f, indent=2, ensure_ascii=False, default=str)
            self.logger.info(f"Saved {len(records)} emails to {filepath}")
        except Exception as e:
            self.logger.error(f"Failed to save emails to disk: {e}", exc_info=True)

    def _filter_already_processed(self, emails, user_id):
        """Remove emails that have already been successfully processed.

        Only skips emails with status='processed'. Failed and skipped emails
        are allowed through so they can be retried (e.g., after adding personal
        info for PDF password unlocking).
        """
        from models.processedEmails import ProcessedEmails

        gmail_ids = [e.get("gmail_id") or e.get("message_id") for e in emails]
        gmail_ids = [gid for gid in gmail_ids if gid]

        if not gmail_ids:
            return emails

        try:
            db = self.transaction_service.db
            existing = db.session.query(ProcessedEmails.gmail_id).filter(
                ProcessedEmails.user_id == user_id,
                ProcessedEmails.gmail_id.in_(gmail_ids),
                ProcessedEmails.status == 'processed',
            ).all()
            existing_ids = {row.gmail_id for row in existing}

            return [
                e for e in emails
                if (e.get("gmail_id") or e.get("message_id")) not in existing_ids
            ]
        except Exception as e:
            self.logger.warning(f"Dedup lookup failed, processing all emails: {e}")
            return emails

    # ── Email separation ───────────────────────────────────────────────

    def _separate_emails(self, emails, user_id, token):
        """
        Separate emails into text-only and PDF-attachment categories.
        Uses the _has_pdf flag set during fetch (zero extra API calls).
        Falls back to Gmail API check if the flag is missing.
        """
        text_emails = []
        pdf_emails = []
        gmail_api = None  # lazy — only built if fallback needed

        for email in emails:
            gmail_id = email.get("gmail_id") or email.get("message_id")
            if not gmail_id:
                text_emails.append(email)
                continue

            has_pdf = email.get("_has_pdf")

            # Fallback: if _has_pdf wasn't set (e.g. old email dicts), check via API
            if has_pdf is None:
                if gmail_api is None:
                    gmail_api = self._get_gmail_api(user_id, token)
                try:
                    msg = gmail_api.users().messages().get(
                        userId="me", id=gmail_id, format="full",
                        fields="payload/parts(filename,mimeType,parts/filename,parts/mimeType)"
                    ).execute()
                    from utils.GmailServiceUtils import GmailServiceUtils
                    has_pdf = GmailServiceUtils._check_for_pdf_in_payload(msg.get("payload", {}))
                except Exception as e:
                    self.logger.warning(f"Error checking email {gmail_id} for attachments: {e}")
                    has_pdf = False

            if has_pdf:
                if gmail_api is None:
                    gmail_api = self._get_gmail_api(user_id, token)
                email["_gmail_api"] = gmail_api
                pdf_emails.append(email)
            else:
                # For bank_statement emails without PDF attachments,
                # try to extract link-based PDF (e.g., HDFC Smart Statements)
                category = email.get("_category", "")
                if category == "bank_statement":
                    if gmail_api is None:
                        gmail_api = self._get_gmail_api(user_id, token)
                    local_pdf = self._try_download_link_based_pdf(email, gmail_api, gmail_id, user_id)
                    if local_pdf:
                        email["_gmail_api"] = gmail_api
                        email["_local_pdf_paths"] = [local_pdf]
                        pdf_emails.append(email)
                        continue
                text_emails.append(email)

        return text_emails, pdf_emails

    def _get_gmail_api(self, user_id, token):
        """Lazily build and return a Gmail API service object."""
        gmail_svc = self.transaction_service.gmailService
        return gmail_svc.googleService.get_gmail_service(user_id, token)

    # ── Text email processing ──────────────────────────────────────────

    def _process_text_emails_batch(self, emails, user_id):
        """Process text-only emails in batches using Claude (haiku for speed)."""
        total_processed = 0

        for i in range(0, len(emails), TEXT_BATCH_SIZE):
            batch = emails[i:i + TEXT_BATCH_SIZE]
            self.logger.info(f"Processing text email batch {i // TEXT_BATCH_SIZE + 1} ({len(batch)} emails)")

            try:
                batch_result = self._run_text_batch(batch, user_id)
                total_processed += batch_result
            except Exception as e:
                self.logger.error(f"Error processing text email batch: {e}", exc_info=True)

        return total_processed

    def _run_text_batch(self, emails, user_id):
        """Run a single batch of text emails through Claude."""
        # Build the email content for the prompt
        email_content = self._format_emails_for_prompt(emails)

        # Build lookup map: gmail_id -> email metadata so the tool handler
        # can auto-inject the authoritative timestamp and gmail_message_id
        email_lookup = {}
        for email in emails:
            gid = email.get('gmail_id')
            if gid:
                email_lookup[gid] = email

        # Build MCP tools
        sdk_tools = self._build_sdk_tools(user_id, email_lookup=email_lookup)
        mcp_server = create_sdk_mcp_server(
            name=MCP_SERVER_NAME,
            tools=sdk_tools,
        )

        options = ClaudeAgentOptions(
            model="haiku",
            system_prompt=TEXT_EMAIL_SYSTEM_PROMPT,
            max_turns=MAX_TURNS,
            mcp_servers={MCP_SERVER_NAME: mcp_server},
            permission_mode="bypassPermissions",
            # MCP tool names listed explicitly (defensive — see doc §7.2)
            allowed_tools=[
                f"mcp__{MCP_SERVER_NAME}__{t['name']}" for t in MAIL_PROCESSOR_TOOLS
            ],
        )

        prompt_text = (
            f"Process the following {len(emails)} financial emails. "
            f"For each email, classify it and extract data using the MCP tools.\n\n"
            f"{email_content}"
        )

        async def make_prompt():
            yield {
                "type": "user",
                "session_id": "",
                "message": {"role": "user", "content": prompt_text},
                "parent_tool_use_id": None,
            }

        result_box = {"result": None, "timeout": False}

        async def run_with_timeout():
            import asyncio
            try:
                result_box["result"] = await asyncio.wait_for(
                    run_query_collect(
                        agent="mail.text_extract", options=options, prompt=make_prompt(),
                    ),
                    timeout=300,
                )
            except asyncio.TimeoutError:
                result_box["timeout"] = True

        anyio.run(run_with_timeout)

        if result_box["timeout"]:
            err = "Claude error: rate_limit"
            self.logger.error(f"Text batch processing error: {err}")
            raise RuntimeError(err)

        result = result_box["result"]
        if result.error:
            err = f"Claude error: {result.error}"
            self.logger.error(f"Text batch processing error: {err}")
            if "rate_limit" in err:
                raise RuntimeError(err)
            return 0

        summary = result.text.strip()
        self.logger.info(f"Text batch complete: {summary[:200]}...")
        return len(emails)

    def _format_emails_for_prompt(self, emails):
        """Format a batch of emails into a structured prompt string."""
        parts = []
        for i, email in enumerate(emails, 1):
            parts.append(
                f"--- Email {i} ---\n"
                f"From: {email.get('sender', 'Unknown')}\n"
                f"Subject: {email.get('subject', 'No subject')}\n"
                f"Date: {email.get('time', 'Unknown')}\n"
                f"Gmail ID: {email.get('gmail_id', 'N/A')}\n"
                f"Body snippet: {email.get('message', '')}\n"
            )
        return "\n".join(parts)

    # ── PDF email processing ───────────────────────────────────────────

    def _process_pdf_emails(self, emails, user_id, processing_mode="image",
                            progress_callback=None):
        """Process emails with PDF attachments individually using Claude."""
        total_processed = 0

        for i, email in enumerate(emails):
            try:
                self._process_single_pdf_email(email, user_id, processing_mode)
                total_processed += 1
                if progress_callback:
                    try:
                        progress_callback({
                            "pdf_emails_processed": total_processed,
                            "stage": f"processing_pdf ({total_processed}/{len(emails)})",
                        })
                    except Exception:
                        pass
            except Exception as e:
                self.logger.error(
                    f"Error processing PDF email {email.get('gmail_id', 'unknown')}: {e}",
                    exc_info=True,
                )

        return total_processed

    def _process_single_pdf_email(self, email, user_id, processing_mode="image"):
        """Download PDF attachment, then send to Claude for extraction."""
        gmail_api = email.get("_gmail_api")
        gmail_id = email.get("gmail_id") or email.get("message_id")

        if not gmail_id:
            self.logger.warning(f"PDF email missing gmail_id, skipping")
            return

        # 1. Extract password hint from email body (e.g. "Password is your DOB in DDMMYYYY")
        email_body = email.get("message", "")
        hint_tags = extract_password_hint(email_body)

        # 2. Get PDF paths — either pre-downloaded (link-based) or from Gmail attachment
        pdf_items = email.get("_local_pdf_paths")
        if pdf_items:
            # Legacy format: list of paths — convert to tuples
            pdf_items = [(p, os.path.basename(p)) if isinstance(p, str) else p for p in pdf_items]
        else:
            if not gmail_api:
                self.logger.warning(f"PDF email missing gmail_api, skipping")
                return
            pdf_items = self._download_pdf_attachments(gmail_api, gmail_id)
        if not pdf_items:
            self.logger.warning(f"No PDF found for email {gmail_id}")
            return

        # 3. Process each PDF through Claude with vision
        for item in pdf_items:
            pdf_path, original_filename = item if isinstance(item, tuple) else (item, os.path.basename(item))
            unlocked_path = None
            try:
                # Get password and pre-unlock the PDF so Claude doesn't need to handle passwords
                password = self._get_statement_password(user_id, email, pdf_path=pdf_path, hint_tags=hint_tags)
                if password:
                    unlocked_path = self._pre_unlock_pdf(pdf_path, password)
                    if unlocked_path:
                        pdf_path = unlocked_path
                        password = None  # PDF is now unlocked
                analysis_summary = self._run_pdf_analysis(
                    pdf_path, email, user_id, password, processing_mode,
                )

                # ak-bwe v2 (reviewer BOUNCE hq-wisp-b48kot):
                # GATE the stamp on genuine ingest success. Only
                # stamp status='success' when _run_pdf_analysis
                # reports success=True AND no failed_chunks
                # residual. On partial/persistent failure DO NOT
                # stamp — _filter_already_processed re-processes
                # anything that isn't 'processed' → next run
                # retries the file → ak-8l5 dedup absorbs
                # already-done chunks and only the missed rows
                # land. Symmetric with ak-wty (retry) + ak-ifc
                # (reconciliation fallback): all three lean on
                # re-runs to close the loop.
                from utils.processed_emails_upsert import should_stamp_success
                if should_stamp_success(analysis_summary):
                    self._stamp_processed_email(
                        email, user_id,
                        category=email.get("_category", "bank_statement"),
                        status="success",
                    )
                else:
                    self.logger.warning(
                        f"ak-bwe v2: NOT stamping processedEmails "
                        f"for gmail_id={gmail_id!r} — ingest not fully "
                        f"successful (summary={analysis_summary!r}). "
                        f"Next run will re-attempt; ak-8l5 dedup "
                        f"absorbs the already-done chunks."
                    )

                # Only persist PDF after successful processing
                try:
                    persist_path = self._persist_pdf(pdf_path, user_id, email, original_filename)
                    if persist_path:
                        self._update_processed_email_pdf(gmail_id, user_id, persist_path)
                except Exception as e:
                    self.logger.warning(f"Failed to persist PDF for {gmail_id}: {e}")
            except Exception as e:
                self.logger.error(f"PDF analysis failed for {gmail_id}: {e}")
            finally:
                # Clean up temp files
                if os.path.exists(pdf_path):
                    os.remove(pdf_path)
                if unlocked_path and unlocked_path != pdf_path and os.path.exists(unlocked_path):
                    os.remove(unlocked_path)

    def _persist_pdf(self, pdf_path, user_id, email, original_filename):
        """Copy processed PDF to persistent storage under claude_statements/."""
        if not os.path.exists(pdf_path):
            return None
        category = email.get("_category", "unknown")
        gmail_id = email.get("gmail_id", "unknown")
        dest_dir = os.path.join(os.getcwd(), "claude_statements", user_id, category)
        os.makedirs(dest_dir, exist_ok=True)
        # Use gmail_id prefix to ensure uniqueness
        dest_name = f"{gmail_id}_{original_filename}"
        dest_path = os.path.join(dest_dir, dest_name)
        shutil.copy2(pdf_path, dest_path)
        self.logger.info(f"Persisted PDF: {dest_path}")
        # Return relative path (category/filename) for DB storage
        return os.path.join(category, dest_name)

    def _stamp_processed_email(
        self, email, user_id, *,
        category="bank_statement",
        status="success",
        items_extracted=0,
        extraction_summary=None,
    ):
        """ak-bwe: ensure a processedEmails row exists for `email`
        after a successful text-mode / image-mode PDF ingest.

        Pre-ak-bwe the row only landed if the LLM invoked the
        report_result tool — text-mode structured-output path never
        called that tool, so backfill re-runs saw the gmail_id as
        "never processed" and re-ingested.

        Uses the shared utils.processed_emails_upsert.upsert_processed_email
        helper (same code path _handle_report_result uses on the LLM
        side) so behavior is identical whether the LLM signals
        completion or the service layer stamps it directly.

        Non-critical: any failure is logged as WARNING; the
        transactions have already landed and the primary goal
        (idempotency) is best-effort — the next re-run may see the
        file as unprocessed but ak-8l5's storage-layer dedup catches
        the duplicate insertion.
        """
        try:
            from models.processedEmails import ProcessedEmails
            from utils.processed_emails_upsert import upsert_processed_email
            session = self.transaction_service.db.session
            if not session:
                return
            gmail_id = email.get("gmail_id") or email.get("message_id")
            if not gmail_id:
                return
            result = upsert_processed_email(
                session,
                ProcessedEmails,
                gmail_id=gmail_id,
                user_id=user_id,
                sender=email.get("sender"),
                subject=email.get("subject"),
                email_date=email.get("date"),
                category=category,
                processing_type=email.get("_processing_type", "pdf"),
                status=status,
                items_extracted=items_extracted,
                extraction_summary=extraction_summary,
            )
            self.logger.info(
                f"ak-bwe: processedEmails {result.get('status')} "
                f"for gmail_id={gmail_id!r} user={user_id!r} "
                f"(db_status={result.get('db_status')!r})"
            )
        except Exception as e:
            self.logger.warning(
                f"ak-bwe: _stamp_processed_email failed for "
                f"gmail_id={email.get('gmail_id')!r}: {e}. Idempotency "
                f"may be reduced but ak-8l5 dedup catches duplicates."
            )

    def _update_processed_email_pdf(self, gmail_id, user_id, pdf_filename):
        """Update the processedEmails row with the PDF filename."""
        try:
            from models.processedEmails import ProcessedEmails
            session = self.transaction_service.db.session
            if not session:
                return
            row = session.query(ProcessedEmails).filter_by(
                gmail_id=gmail_id, user_id=user_id
            ).first()
            if row:
                row.pdf_filename = pdf_filename
                session.commit()
                self.logger.info(f"Updated pdf_filename for {gmail_id}: {pdf_filename}")
            else:
                self.logger.warning(f"No processedEmails row found for {gmail_id} to update pdf_filename")
        except Exception as e:
            self.logger.warning(f"Failed to update pdf_filename for {gmail_id}: {e}")

    def _download_pdf_attachments(self, gmail_api, gmail_id):
        """Download PDF attachments from a Gmail message to temp directory.
        Returns list of (pdf_path, original_filename) tuples."""
        pdf_paths = []
        try:
            msg = gmail_api.users().messages().get(userId="me", id=gmail_id).execute()
            parts = msg.get("payload", {}).get("parts", [])

            for i, part in enumerate(parts):
                filename = part.get("filename", "")
                if not filename or not filename.lower().endswith(".pdf"):
                    continue

                # Get attachment data
                body = part.get("body", {})
                if "data" in body:
                    data = base64.urlsafe_b64decode(body["data"].encode("UTF-8"))
                elif "attachmentId" in body:
                    attachment = gmail_api.users().messages().attachments().get(
                        userId="me", messageId=gmail_id, id=body["attachmentId"]
                    ).execute()
                    data = base64.urlsafe_b64decode(attachment["data"].encode("UTF-8"))
                else:
                    continue

                # Save to temp
                tmp_dir = os.path.join(os.getcwd(), "tmp")
                os.makedirs(tmp_dir, exist_ok=True)
                safe_name = f"mail_pipeline_{gmail_id}_{i}.pdf"
                pdf_path = os.path.join(tmp_dir, safe_name)
                with open(pdf_path, "wb") as f:
                    f.write(data)

                pdf_paths.append((pdf_path, filename))
                self.logger.info(f"Downloaded PDF attachment: {filename} -> {pdf_path}")

        except Exception as e:
            self.logger.error(f"Error downloading PDF attachments: {e}", exc_info=True)

        return pdf_paths

    def _pre_unlock_pdf(self, pdf_path, password):
        """
        Unlock a password-protected PDF and save an unprotected copy.
        Returns the path to the unlocked file, or None on failure.
        """
        try:
            doc = fitz.open(pdf_path)
            if not doc.needs_pass:
                doc.close()
                return pdf_path  # Not password-protected

            if not doc.authenticate(password):
                doc.close()
                self.logger.warning(f"Failed to authenticate PDF with provided password")
                return None

            # Save unlocked copy
            unlocked_path = pdf_path.rsplit(".", 1)[0] + "_unlocked.pdf"
            doc.save(unlocked_path)
            doc.close()

            # Replace original with unlocked version
            os.remove(pdf_path)
            os.rename(unlocked_path, pdf_path)
            self.logger.info(f"Pre-unlocked PDF: {pdf_path}")
            return pdf_path

        except Exception as e:
            self.logger.warning(f"Failed to pre-unlock PDF: {e}")
            return None

    # ── Link-based PDF extraction (HDFC Smart Statements, etc.) ──────

    def _try_download_link_based_pdf(self, email, gmail_api, gmail_id, user_id=None):
        """
        For bank_statement emails without PDF attachments (e.g., HDFC Smart Statements),
        try to extract a download link from the HTML body and download the PDF.
        Returns the local PDF path if successful, None otherwise.
        """
        try:
            sender = email.get("sender", "")
            domain = self._extract_domain(sender)

            # Currently only handles HDFC Smart Statements. ak-x6p fix:
            # dot-boundary suffix match so subdomain senders like
            # `alerts.hdfcbank.bank.in` still route through the Smart
            # Statement extractor (prior exact-match would silently
            # skip them).
            _hdfc_domains = ("hdfcbank.net", "hdfcbank.com", "hdfcbank.bank.in")
            if domain not in _hdfc_domains and not any(
                domain.endswith("." + d) for d in _hdfc_domains
            ):
                return None

            # Fetch full message to get HTML body
            msg = gmail_api.users().messages().get(userId="me", id=gmail_id).execute()

            # Extract download link from HTML body
            link = self._extract_smart_statement_link(msg)
            if not link:
                self.logger.debug(f"No Smart Statement link found in email {gmail_id}")
                return None

            self.logger.info(f"Found Smart Statement download link for email {gmail_id}")

            # Look up Customer ID password for HDFC Smart Statement portal
            password = self._get_smart_statement_password(user_id, domain)

            # Download PDF from HDFC server
            return self._download_from_smart_statement_link(link, gmail_id, password)

        except Exception as e:
            self.logger.warning(f"Failed to extract link-based PDF for {gmail_id}: {e}")
            return None

    def _extract_smart_statement_link(self, msg):
        """Extract the Smart Statement download link from email HTML body."""
        from bs4 import BeautifulSoup

        def _search_parts(parts_list):
            """Recursively search all MIME parts for the Smart Statement link."""
            for part in parts_list:
                data = part.get("body", {}).get("data")
                if data:
                    decoded_data = base64.urlsafe_b64decode(data).decode("utf-8")
                    soup = BeautifulSoup(decoded_data, "html.parser")

                    # Strategy 1: Find HDFC's styled download button (brand color #004b8d)
                    td_tag = soup.find("td", style=lambda s: s and "004b8d" in s)
                    if td_tag:
                        a_tag = td_tag.find("a")
                        if a_tag and "href" in a_tag.attrs:
                            return a_tag["href"]

                    # Strategy 2: Find any link to smartstatements portal
                    for a_tag in soup.find_all("a", href=True):
                        href = a_tag["href"]
                        if "smartstatements.hdfcbank.com" in href or "smartstatements.hdfc.bank.in" in href:
                            return href

                # Recurse into nested parts
                sub_parts = part.get("parts", [])
                if sub_parts:
                    result = _search_parts(sub_parts)
                    if result:
                        return result
            return None

        # Check payload body directly (non-multipart emails)
        payload = msg.get("payload", {})
        payload_data = payload.get("body", {}).get("data")
        if payload_data:
            decoded = base64.urlsafe_b64decode(payload_data).decode("utf-8")
            soup = BeautifulSoup(decoded, "html.parser")
            for a_tag in soup.find_all("a", href=True):
                href = a_tag["href"]
                if "smartstatements.hdfcbank.com" in href or "smartstatements.hdfc.bank.in" in href:
                    return href

        # Search through all MIME parts
        parts = payload.get("parts", [])
        return _search_parts(parts) if parts else None

    def _download_from_smart_statement_link(self, link, gmail_id, password=None):
        """Download PDF from HDFC Smart Statement link."""
        import requests
        from bs4 import BeautifulSoup

        user_agent = (
            "Mozilla/5.0 (Macintosh; Intel Mac OS X 10_15_7) "
            "AppleWebKit/537.36 (KHTML, like Gecko) Chrome/126.0.0.0 Safari/537.36"
        )

        # Parse the link to get jobkey and base URL
        parsed = urlparse(link)
        query_params = parse_qs(parsed.query)
        job_key = query_params.get("jobkey", [None])[0]
        base_url = f"{parsed.scheme}://{parsed.hostname}"

        if not job_key:
            self.logger.warning(f"No jobkey found in Smart Statement link for {gmail_id}")
            return None

        session = requests.Session()
        session.headers.update({"User-Agent": user_agent})

        # Visit link to get the initial page
        response = session.get(link, timeout=30)
        if response.status_code != 200:
            self.logger.warning(
                f"Smart Statement page returned HTTP {response.status_code} for {gmail_id}"
            )
            return None

        soup = BeautifulSoup(response.text, "html.parser")

        # Check if we already have the sequence (no password needed)
        seq_element = soup.find("input", {"type": "hidden", "name": "seqence", "id": "seqence"})

        # If no sequence found, try password submission
        if (not seq_element or "value" not in seq_element.attrs) and password:
            self.logger.info(f"Smart Statement portal requires login, submitting password for {gmail_id}")

            # Extract form action and hidden fields from the login page
            form = soup.find("form")
            if form:
                action = form.get("action", "")
                if action and not action.startswith("http"):
                    action = f"{base_url}{action}" if action.startswith("/") else f"{base_url}/{action}"
                elif not action:
                    action = f"{base_url}/HDFCRestFulService/webresources/app/validateCustomerID"

                # Collect all hidden form fields
                login_data = {}
                for hidden in form.find_all("input", {"type": "hidden"}):
                    name = hidden.get("name")
                    if name:
                        login_data[name] = hidden.get("value", "")

                # Add Customer ID — try common field names
                custid_field = form.find("input", {"type": "text"}) or form.find("input", {"name": "custid"})
                field_name = custid_field.get("name", "custid") if custid_field else "custid"
                login_data[field_name] = password

                self.logger.debug(f"Submitting login to {action} with fields: {list(login_data.keys())}")
            else:
                # Fallback: try direct API call
                action = f"{base_url}/HDFCRestFulService/webresources/app/validateCustomerID"
                login_data = {"jobkey": job_key, "custid": password}

            login_response = session.post(action, data=login_data, timeout=30)

            if login_response.status_code != 200:
                self.logger.warning(
                    f"Smart Statement login returned HTTP {login_response.status_code} for {gmail_id}"
                )
                return None

            soup = BeautifulSoup(login_response.text, "html.parser")
            seq_element = soup.find("input", {"type": "hidden", "name": "seqence", "id": "seqence"})

        if not seq_element or "value" not in seq_element.attrs:
            # Log the page content for debugging
            self.logger.warning(
                f"Could not extract sequence from Smart Statement page for {gmail_id} "
                f"(link may have expired or password incorrect). "
                f"Page title: {soup.title.string if soup.title else 'N/A'}"
            )
            return None

        req_id = seq_element["value"]

        # Download PDF using same base domain as the link
        pdf_url = (
            f"{base_url}/HDFCRestFulService/webresources/app/pdfformat"
            f"?jobkey={job_key}&reqid={req_id}&format=pdf"
        )
        pdf_response = session.post(pdf_url, timeout=60)

        if pdf_response.status_code != 200:
            self.logger.warning(
                f"Failed to download Smart Statement PDF: HTTP {pdf_response.status_code}"
            )
            return None

        # Validate we actually got a PDF (not an error page)
        if len(pdf_response.content) < 1000 or not pdf_response.content[:5].startswith(b"%PDF"):
            self.logger.warning(
                f"Smart Statement response does not appear to be a valid PDF for {gmail_id}"
            )
            return None

        # Save to temp
        tmp_dir = os.path.join(os.getcwd(), "tmp")
        os.makedirs(tmp_dir, exist_ok=True)
        safe_name = f"smart_statement_{gmail_id}.pdf"
        pdf_path = os.path.join(tmp_dir, safe_name)

        with open(pdf_path, "wb") as f:
            f.write(pdf_response.content)

        self.logger.info(
            f"Downloaded Smart Statement PDF: {pdf_path} ({len(pdf_response.content)} bytes)"
        )
        return pdf_path

    def _get_smart_statement_password(self, user_id, domain):
        """Look up the Customer ID / password for the Smart Statement portal from StatementPasswords."""
        if not user_id:
            return None
        try:
            from models import StatementPasswords
            domain_to_bank = {
                "hdfcbank.net": ["HDFC_DEBIT", "Millenia_Credit", "HDFC_REGALIA"],
                "hdfcbank.com": ["HDFC_DEBIT", "Millenia_Credit", "HDFC_REGALIA"],
                "hdfcbank.bank.in": ["HDFC_DEBIT", "Millenia_Credit", "HDFC_REGALIA"],
            }
            # ak-x6p fix: subdomain-aware lookup (alerts.hdfcbank.bank.in
            # etc.); prior `.get(domain, [])` missed these.
            bank_names = self._lookup_domain_map(domain_to_bank, domain)
            for bank_name in bank_names:
                pw = self.transaction_service.db.session.query(StatementPasswords).filter_by(
                    user=user_id, bank=bank_name
                ).first()
                if pw and pw.password_hash:
                    self.logger.debug(f"Found Smart Statement password for bank {bank_name}")
                    return pw.password_hash
        except Exception as e:
            self.logger.debug(f"Could not look up Smart Statement password: {e}")
        return None

    def _get_statement_password(self, user_id, email, pdf_path=None, hint_tags=None):
        """
        Try to find a statement password for the sender's bank.
        Strategy:
        1. Collect ALL explicit bank passwords for the sender domain and try each against the PDF
        2. If that fails, try personal info-based candidates using the email's password hint
        """
        # Extract domain from sender (don't rely on _sender_domain being pre-set)
        sender = email.get("sender", "")
        domain = email.get("_sender_domain") or self._extract_domain(sender)

        domain_to_bank = {
            "hdfcbank.net": ["Millenia_Credit", "HDFC_DEBIT", "HDFC_REGALIA"],
            "hdfcbank.com": ["Millenia_Credit", "HDFC_DEBIT", "HDFC_REGALIA"],
            "hdfcbank.bank.in": ["Millenia_Credit", "HDFC_DEBIT", "HDFC_REGALIA"],
            "icicibank.com": ["ICICI_AMAZON_PAY"],
            "icicibank.co.in": ["ICICI_AMAZON_PAY"],
            "yesbank.in": ["YES_BANK_DEBIT", "YES_BANK_ACE"],
            "yesbank.co.in": ["YES_BANK_DEBIT", "YES_BANK_ACE"],
            "bankofindia.co.in": ["BOI"],
            "bankofindia.com": ["BOI"],
            # ak-2ql: 2026-03-07 RBI-mandated `.bank.in` TLD migration
            # (precedent: hdfcbank.bank.in above).
            "bankofindia.bank.in": ["BOI"],
        }

        # ── Strategy 1: Explicit bank passwords from DB ─────────────
        # Collect ALL passwords for matching banks and try each against the PDF
        explicit_passwords = []
        try:
            from models import StatementPasswords
            # ak-x6p fix: subdomain-aware lookup so senders like
            # `noreply-estatement@alerts.bankofindia.bank.in` still
            # resolve to the BOI password candidates.
            bank_names = self._lookup_domain_map(domain_to_bank, domain)
            for bank_name in bank_names:
                pw = self.transaction_service.db.session.query(StatementPasswords).filter_by(
                    user=user_id, bank=bank_name
                ).first()
                if pw and pw.password_hash:
                    explicit_passwords.append(pw.password_hash)
        except Exception as e:
            self.logger.debug(f"Error fetching explicit passwords: {e}")

        if explicit_passwords and pdf_path:
            password = try_unlock_pdf(pdf_path, explicit_passwords)
            if password:
                self.logger.info(f"PDF unlocked with explicit bank password for domain {domain}")
                return password
        elif explicit_passwords:
            # No pdf_path to test against — return first password
            return explicit_passwords[0]

        # ── Strategy 2: Personal info-based password candidates ────
        if pdf_path:
            password = self._try_personal_info_passwords(user_id, pdf_path, hint_tags=hint_tags)
            if password:
                return password

        return None

    def _try_personal_info_passwords(self, user_id, pdf_path, hint_tags=None):
        """
        Fetch user personal info from DB, generate common Indian bank
        PDF password patterns, and try them against the PDF.
        hint_tags from the email body are used to prioritize the right pattern.
        """
        try:
            from models.userPersonalInfo import UserPersonalInfo
            db = self.flask_app.extensions.get("sqlalchemy")
            if not db:
                return None

            personal_info = db.session.query(UserPersonalInfo).filter_by(user_id=user_id).first()
            if not personal_info:
                self.logger.debug(f"No personal info stored for user {user_id}")
                return None

            candidates = generate_password_candidates(personal_info, hint_tags=hint_tags)
            if not candidates:
                return None

            self.logger.info(
                f"Trying {len(candidates)} personal-info password candidates for {pdf_path}"
                f"{f' (hint: {hint_tags})' if hint_tags else ''}"
            )
            password = try_unlock_pdf(pdf_path, candidates)
            if password:
                self.logger.info(f"PDF unlocked using personal info password (length={len(password)})")
            return password

        except Exception as e:
            self.logger.debug(f"Personal info password attempt failed: {e}")
            return None

    # Maximum pages per chunk — keeps image context small per query.
    PAGES_PER_CHUNK = 1

    def _post_chunk_reconciliation(self, file_id, gmail_id, user_id):
        """Run reconciliation once after all chunks of a PDF are processed.

        Looks up the actual date range from inserted transactions and runs
        the overlap guard + statement period recording in one pass.
        """
        from models.transactions import Transactions
        from sqlalchemy import func

        session = self.transaction_service.db.session
        try:
            # Get bank and date range from inserted transactions for this file
            row = session.query(
                Transactions.bank,
                func.min(Transactions.date),
                func.max(Transactions.date),
                func.count(Transactions.referenceID),
            ).filter(
                Transactions.fileID == file_id,
                Transactions.user == user_id,
            ).group_by(Transactions.bank).first()

            if not row or not row[0]:
                self.logger.warning(f"No transactions found for file {file_id}, skipping reconciliation")
                return

            bank, period_start, period_end, txn_count = row
            self.logger.info(
                f"Post-chunk reconciliation for {bank}: {period_start} to {period_end} "
                f"({txn_count} txns in file {file_id})"
            )

            if self.reconciliation_service:
                result = self.reconciliation_service.replace_email_transactions_with_statement(
                    user_id=user_id,
                    bank=bank,
                    period_start=period_start,
                    period_end=period_end,
                    file_id=file_id,
                    gmail_message_id=gmail_id,
                    transaction_count=txn_count,
                )
                email_replaced = result.get('email_transactions_deleted', 0)
                stmt_replaced = result.get('statement_transactions_deleted', 0)
                self.logger.info(
                    f"Post-chunk reconciliation complete: replaced {email_replaced} email txns, "
                    f"{stmt_replaced} overlapping statement txns"
                )
        except Exception as e:
            self.logger.error(f"Post-chunk reconciliation error: {e}", exc_info=True)

    # Pages per chunk for text mode — text is tiny (~1-3KB/page) but we keep
    # chunks small so the agent doesn't need to format too many transactions.
    TEXT_PAGES_PER_CHUNK = 2

    @staticmethod
    def _count_expected_transactions(text):
        """Count transaction-date lines in extracted text to estimate expected count.

        HDFC format: each transaction has exactly 2 date lines (DD/MM/YYYY):
        the Transaction Date and the Value Date. So total dates / 2 = transactions.
        """
        import re
        date_pattern = re.compile(r'^\d{2}/\d{2}/\d{4}\s*$')
        date_count = sum(
            1 for line in text.strip().split('\n')
            if date_pattern.match(line.strip())
        )
        return date_count // 2

    @staticmethod
    def _pdf_has_usable_text(pdf_path, password=None, min_chars_per_page=100, sample_pages=3):
        """Check if a PDF has extractable text on its content pages.
        Samples up to sample_pages pages (skipping page 1 which may be a cover)
        and checks if average text length exceeds min_chars_per_page."""
        import fitz as _fitz
        try:
            doc = _fitz.open(pdf_path)
            if doc.needs_pass and password:
                doc.authenticate(password)
            total = doc.page_count
            if total == 0:
                doc.close()
                return False
            # Sample pages: try first few content pages
            pages_to_check = range(0, min(sample_pages, total))
            total_chars = 0
            checked = 0
            for i in pages_to_check:
                text = doc[i].get_text().strip()
                total_chars += len(text)
                checked += 1
            doc.close()
            avg = total_chars / max(checked, 1)
            return avg >= min_chars_per_page
        except Exception:
            return False

    def _run_pdf_analysis(self, pdf_path, email, user_id, password=None,
                          processing_mode="image", only_chunks=None):
        """Run Claude on a single PDF file via MCP tools.

        For PDFs with more than pages_per_chunk pages, splits processing into
        multiple independent queries. processing_mode controls whether the agent
        receives rendered images ("image") or extracted text ("text").
        only_chunks: Optional list of [start, end] page ranges to process.
            If set, only those chunks are processed (others are skipped).

        Uses a single anyio.run() event loop for ALL chunks to prevent
        connection/resource leaks from repeated event loop creation.

        ak-bwe v2 (reviewer BOUNCE hq-wisp-b48kot): returns a summary
        dict {success: bool, failed_chunks: list, total_chunks: int,
        reason: str} so the caller (_process_single_pdf_email) can
        gate the processedEmails stamp on genuine ingest success.
        Prior version returned None and callers hardcoded status=
        'success', which stamped 'processed' for persistently-failed
        ingests → _filter_already_processed skipped forever → silent
        transaction loss. Inverted the exact recovery ak-wty + ak-ifc
        were built to provide.

        Any unhandled exception is caught and reflected as
        success=False so a caller-facing raise can't accidentally
        skip the stamping decision.
        """
        import fitz as _fitz

        # Count pages to decide if chunking is needed
        try:
            doc = _fitz.open(pdf_path)
            if doc.needs_pass and password:
                doc.authenticate(password)
            total_pages = doc.page_count
            doc.close()
        except Exception as e:
            self.logger.error(
                f"ak-bwe v2: PDF open failed for {pdf_path!r}: {e}. "
                f"Reporting ingest as FAILED."
            )
            return {"success": False, "failed_chunks": [],
                    "total_chunks": 0,
                    "reason": f"pdf_open_failed: {e}"}

        # Auto-detect: if PDF has extractable text, prefer text mode
        if processing_mode == "image" and self._pdf_has_usable_text(pdf_path, password):
            self.logger.info(f"PDF has usable text layer — auto-switching to text mode")
            processing_mode = "text"

        gmail_id = email.get("gmail_id", "")

        # Detect bank — use override if present, otherwise detect from sender
        from services.bankFormatRules import get_bank_from_sender
        bank = email.get("_bank") or get_bank_from_sender(email.get("sender", ""))

        # Text mode can handle more pages per chunk since text is small
        pages_per_chunk = self.TEXT_PAGES_PER_CHUNK if processing_mode == "text" else self.PAGES_PER_CHUNK

        summary = {"success": False, "failed_chunks": [], "total_chunks": 0}
        try:
            if total_pages <= pages_per_chunk and processing_mode != "text":
                # Small PDF in image mode — single MCP query handles everything
                chunk_result = anyio.run(self._run_pdf_chunk_async,
                    pdf_path, email, user_id, password,
                    1, total_pages, total_pages,
                    True, False, None, processing_mode,
                )
                # ak-bwe v2: derive success from the single-chunk result.
                # `called=False` OR an `sdk_error` field means the chunk
                # didn't land tx cleanly.
                if isinstance(chunk_result, dict):
                    called = chunk_result.get("called", False)
                    sdk_error = chunk_result.get("sdk_error")
                    summary = {
                        "success": bool(called) and not sdk_error,
                        "failed_chunks": [] if called and not sdk_error else [[1, total_pages]],
                        "total_chunks": 1,
                    }
                else:
                    # ak-bwe v3 (reviewer BOUNCE hq-wisp-97i5qm):
                    # unknown / None return → default to fail-CLOSED.
                    # Prior fail-OPEN default was the exact regression
                    # class v2's MAJOR fix closed at the outer gate:
                    # a chunk function returning None (or a shape
                    # future refactors don't preserve) would flow
                    # through should_stamp_success → True → stamp
                    # 'processed' → skip forever → silent loss.
                    # should_stamp_success is fail-closed; the
                    # multi-chunk path is fail-closed; this spot must
                    # match. Zero-loss preference: prefer to NOT
                    # stamp, re-run absorbs the already-done chunk
                    # via ak-8l5 dedup.
                    summary = {
                        "success": False,
                        "failed_chunks": [[1, total_pages]],
                        "total_chunks": 1,
                        "reason": (
                            f"unknown_chunk_result_shape: "
                            f"{type(chunk_result).__name__}"
                        ),
                    }
            elif total_pages <= pages_per_chunk and processing_mode == "text":
                # Small PDF in text mode — use structured output path
                file_id = f"mail_pipeline_{user_id}_{bank}_{gmail_id}"
                summary = anyio.run(
                    self._run_all_chunks_async,
                    pdf_path, email, user_id, password,
                    total_pages, pages_per_chunk, file_id,
                    processing_mode, only_chunks,
                ) or summary
                self._post_chunk_reconciliation(file_id, gmail_id, user_id)
            else:
                # Large PDF — split into chunks with separate queries
                # Pre-create a single file_id so all chunks share it
                file_id = f"mail_pipeline_{user_id}_{bank}_{gmail_id}"

                self.logger.info(
                    f"Large PDF ({total_pages} pages) — splitting into "
                    f"chunks of {pages_per_chunk} ({processing_mode} mode)"
                )

                # Run ALL chunks inside a single event loop
                summary = anyio.run(
                    self._run_all_chunks_async,
                    pdf_path, email, user_id, password,
                    total_pages, pages_per_chunk, file_id,
                    processing_mode, only_chunks,
                ) or summary

                # After all chunks: run reconciliation once using actual date range
                self._post_chunk_reconciliation(file_id, gmail_id, user_id)
        except Exception as e:
            self.logger.error(
                f"ak-bwe v2: _run_pdf_analysis top-level failure: {e}. "
                f"Reporting ingest as FAILED so processedEmails stamp "
                f"is skipped (next run retries the file)."
            )
            summary = {"success": False, "failed_chunks": [],
                       "total_chunks": summary.get("total_chunks", 0),
                       "reason": f"analysis_exception: {e}"}
        return summary

    async def _run_all_chunks_async(self, pdf_path, email, user_id, password,
                                     total_pages, pages_per_chunk, file_id,
                                     processing_mode, only_chunks):
        """Process all PDF chunks within a single async event loop."""
        import fitz as _fitz
        from models.transactions import Transactions
        from sqlalchemy import func as sa_func
        db = self.transaction_service.db

        # Build set of allowed chunk ranges for filtering
        allowed_chunks = None
        if only_chunks:
            allowed_chunks = set(tuple(c) for c in only_chunks)
            self.logger.info(f"Only processing chunks: {only_chunks}")

        chunk_num = 0
        consecutive_failures = 0
        failed_chunks = []

        # For text mode: use query() with structured output (no MCP)
        # For image mode: fall back to MCP-based _run_pdf_chunk_async
        text_mode_options = None
        if processing_mode == "text":
            text_mode_options = ClaudeAgentOptions(
                model="sonnet",
                system_prompt=PDF_SYSTEM_PROMPT,
                max_turns=3,  # Allow retries for structured output validation
                permission_mode="bypassPermissions",
                output_format={
                    "type": "json_schema",
                    "schema": self._TXN_OUTPUT_SCHEMA,
                },
            )
            self.logger.info("Text-mode: will use query() with structured output per chunk")

        try:
            # Build chunk list — no overlap; header stripping and prompt
            # improvements handle page-boundary transactions.
            chunks = []
            for start in range(1, total_pages + 1, pages_per_chunk):
                end = min(start + pages_per_chunk - 1, total_pages)
                chunks.append((start, end))

            for start, end in chunks:
                chunk_num += 1
                is_final = (end >= total_pages)

                if allowed_chunks and (start, end) not in allowed_chunks:
                    self.logger.info(f"Skipping chunk {chunk_num}: pages {start}-{end}")
                    continue

                # Abort after 5 consecutive chunk failures
                if consecutive_failures >= 5:
                    failed_chunks.append([start, end])
                    self.logger.warning(
                        f"Skipping chunk {start}-{end} — "
                        f"{consecutive_failures} consecutive failures"
                    )
                    continue

                self.logger.info(
                    f"Processing chunk {chunk_num}: pages {start}-{end}"
                )

                # Track transaction count before this chunk
                txn_before = db.session.query(
                    sa_func.count(Transactions.referenceID)
                ).filter(
                    Transactions.fileID == file_id,
                    Transactions.user == user_id,
                ).scalar() or 0

                if processing_mode == "text":
                    chunk_result = await self._run_text_chunk(
                        text_mode_options, pdf_path, email, user_id, password,
                        page_start=start, page_end=end,
                        total_pages=total_pages,
                        preset_file_id=file_id,
                    )
                else:
                    chunk_result = await self._run_pdf_chunk_async(
                        pdf_path, email, user_id, password,
                        page_start=start, page_end=end, total_pages=total_pages,
                        is_final_chunk=is_final,
                        skip_reconciliation=True,
                        preset_file_id=file_id,
                        processing_mode=processing_mode,
                    )

                # Post-chunk verification (text mode only)
                if processing_mode == "text":
                    doc = _fitz.open(pdf_path)
                    if doc.needs_pass and password:
                        doc.authenticate(password)
                    chunk_text = "\n".join(
                        doc[p - 1].get_text("text") for p in range(start, end + 1)
                    )
                    doc.close()

                    expected = self._count_expected_transactions(chunk_text)
                    txn_after = db.session.query(
                        sa_func.count(Transactions.referenceID)
                    ).filter(
                        Transactions.fileID == file_id,
                        Transactions.user == user_id,
                    ).scalar() or 0
                    actual = txn_after - txn_before

                    coverage = actual / expected if expected > 0 else 1.0
                    self.logger.info(
                        f"Chunk {start}-{end} verification: {actual}/{expected} "
                        f"transactions ({coverage:.0%} coverage)"
                    )

                    # Skip retry if chunk is already fully processed
                    chunk_dupes = chunk_result.get("duplicates", 0) if chunk_result else 0
                    chunk_inserted = chunk_result.get("inserted", 0) if chunk_result else 0
                    if chunk_dupes > 0 and chunk_inserted + chunk_dupes >= expected * 0.8:
                        self.logger.info(
                            f"Chunk {start}-{end} already processed "
                            f"({chunk_inserted} new, {chunk_dupes} dupes) — skipping retry"
                        )
                        consecutive_failures = 0
                    elif coverage < 0.8 and expected >= 5:
                        self.logger.warning(
                            f"RETRY TRIGGERED for chunk {start}-{end}: "
                            f"coverage={coverage:.0%} ({actual}/{expected})"
                        )
                        if processing_mode == "text":
                            retry_result = await self._run_text_chunk(
                                text_mode_options, pdf_path, email, user_id, password,
                                page_start=start, page_end=end,
                                total_pages=total_pages,
                                preset_file_id=file_id,
                            )
                        else:
                            retry_result = await self._run_pdf_chunk_async(
                                pdf_path, email, user_id, password,
                                page_start=start, page_end=end,
                                total_pages=total_pages,
                                is_final_chunk=is_final,
                                skip_reconciliation=True,
                                preset_file_id=file_id,
                                processing_mode=processing_mode,
                            )
                        txn_after_retry = db.session.query(
                            sa_func.count(Transactions.referenceID)
                        ).filter(
                            Transactions.fileID == file_id,
                            Transactions.user == user_id,
                        ).scalar() or 0
                        actual_retry = txn_after_retry - txn_before
                        self.logger.info(
                            f"Chunk {start}-{end} after retry: "
                            f"{actual_retry}/{expected} transactions"
                        )
                        if actual_retry == 0:
                            consecutive_failures += 1
                            failed_chunks.append([start, end])
                        else:
                            consecutive_failures = 0
                    else:
                        consecutive_failures = 0

            if failed_chunks:
                # ak-n44: emit one structured chunk_error per failed
                # range so a per-file per-chunk report is a single
                # grep away. Follows the aggregate free-form
                # WARNING below for backward compat with any log
                # consumer that already parses the aggregate line.
                from utils.pipeline_logging import emit_chunk_error
                for i, pair in enumerate(failed_chunks):
                    try:
                        page_range = list(pair) if isinstance(pair, (list, tuple)) else [pair]
                    except Exception:
                        page_range = None
                    emit_chunk_error(
                        self.logger,
                        file_id=file_id,
                        chunk_index=i,
                        chunks_total=len(failed_chunks),
                        page_range=page_range,
                        message="chunk failed after inner retries — "
                                "aggregated by _run_all_chunks_async",
                        error_type="analysis_exception",
                        extra={"processing_mode": processing_mode},
                    )
                self.logger.warning(
                    f"Failed chunks (retry with only_chunks): {failed_chunks}"
                )

            # ── ak-ifc v3: file-level savings-summary reconciliation ──
            # After ALL chunks have inserted, compare the aggregate
            # extracted totals to the PDF's own savings summary. If
            # they diverge, re-run every chunk with force_no_mask=True
            # so partially-missed savings rows are recovered. This is
            # the whole point of the ak-ifc effort — hoisting from
            # v2's per-chunk gate (which only fired on ≤2 page files)
            # to file scope means combined statements (always > 2
            # pages) actually get protection.
            if processing_mode == "text":
                await self._run_hdfc_file_level_reconciliation(
                    pdf_path, email, user_id, password,
                    total_pages, file_id, chunks, text_mode_options,
                )
        except Exception as e:
            self.logger.error(
                f"PDF chunk processing error: {e}"
            )
            # ak-bwe v2 (reviewer BOUNCE hq-wisp-b48kot): the top-level
            # exception path also constitutes a failed ingest —
            # propagate as False so _process_single_pdf_email doesn't
            # stamp 'processed' for a file that lost tx.
            return {"success": False, "failed_chunks": failed_chunks,
                    "reason": f"top_level_exception: {e}"}
        finally:
            pass  # query() manages its own lifecycle per chunk

        # ak-bwe v2: return a summary so callers can gate the
        # processedEmails stamp on genuine ingest success. Zero
        # failed_chunks AND no exception → success.
        return {
            "success": not failed_chunks,
            "failed_chunks": list(failed_chunks),
            "total_chunks": len(chunks),
        }

    async def _run_hdfc_file_level_reconciliation(
        self, pdf_path, email, user_id, password,
        total_pages, file_id, chunks, text_mode_options,
    ):
        """ak-ifc v3 MAJOR: file-scope reconciliation backstop.

        Reviewer's proposal (BOUNCE ak-ifc-v3):

          After all chunks of a text-mode HDFC_DEBIT run have inserted:
            (a) Aggregate extracted savings totals from the DB
                (all rows for this fileID+user).
            (b) Parse the file's savings summary from the FULL raw
                text (not chunk-scoped).
            (c) check_hdfc_savings_reconciliation with default ₹1
                tolerance.
            (d) IF divergent:
                - WARN with structured fields (fileID, extracted_*,
                  stated_*, delta, num_chunks).
                - Mark fileDetails.reconciliation_fallback = True so
                  a follow-up sweep can flag the file for manual
                  review of the acknowledged over-parse.
                - Re-run every chunk with force_no_mask=True. The
                  ak-8l5-v2 storage-layer backstop handles the
                  redundant inserts (existing rows collapse via
                  primary hash; missed rows land).
            (e) IF summary missing: log SKIP, no fallback.
            (f) IF clean: log OK, no action.

        Skipped when detected_bank != HDFC_DEBIT — this backstop is
        HDFC-specific and there's no BOI equivalent yet (BOI's own
        combined-statement follow-up is tracked separately).
        """
        import fitz as _fitz
        from services.bankFormatRules import get_bank_from_sender
        from models.transactions import Transactions
        from sqlalchemy import func as sa_func
        from utils.statement_sections import (
            check_hdfc_reconciliation_from_totals,
            detect_hdfc_sections,
            parse_hdfc_savings_summary,
        )

        detected_bank = email.get("_bank") or get_bank_from_sender(
            email.get("sender", "")
        )
        if detected_bank != "HDFC_DEBIT":
            return  # nothing to reconcile

        # Read the full raw text (unmasked) to parse the summary and
        # detect spans. This is a fresh open — cheaper than plumbing
        # the earlier per-chunk doc reference.
        try:
            doc = _fitz.open(pdf_path)
            if doc.needs_pass and password:
                doc.authenticate(password)
            full_lines = []
            for pn in range(1, doc.page_count + 1):
                # Marker line matches _compute_hdfc_section_keep_mask
                # so span indices are consistent (defensive — we don't
                # index across the two here, but keeps the shape).
                full_lines.append(f"\f<PAGE:{pn}>")
                page_text = doc[pn - 1].get_text("text")
                full_lines.extend(page_text.split("\n"))
            doc.close()
        except Exception as e:
            self.logger.warning(
                f"ak-ifc file-reconciliation: could not read PDF for "
                f"summary parse (file={file_id!r}): {e}. Skipping."
            )
            return

        full_text = "\n".join(full_lines)
        spans = detect_hdfc_sections(full_text)
        summary = parse_hdfc_savings_summary(full_text, spans=spans)

        # Aggregate extracted totals from the DB for this fileID+user.
        # Portable pattern: two straight sum-filter queries, no
        # dialect-specific CASE / IIF juggling.
        db = self.transaction_service.db
        try:
            debit_sum = db.session.query(
                sa_func.coalesce(sa_func.sum(Transactions.amount), 0)
            ).filter(
                Transactions.fileID == file_id,
                Transactions.user == user_id,
                Transactions.amount > 0,
            ).scalar() or 0
            credit_sum_signed = db.session.query(
                sa_func.coalesce(sa_func.sum(Transactions.amount), 0)
            ).filter(
                Transactions.fileID == file_id,
                Transactions.user == user_id,
                Transactions.amount < 0,
            ).scalar() or 0
            # summary convention: credits as positive magnitude.
            credit_sum = -float(credit_sum_signed or 0)
            debit_sum = float(debit_sum or 0)
        except Exception as e:
            self.logger.warning(
                f"ak-ifc file-reconciliation: could not aggregate "
                f"extracted totals (file={file_id!r}): {e}. Skipping."
            )
            return

        recon = check_hdfc_reconciliation_from_totals(
            debit_sum, credit_sum, summary,
        )

        num_chunks = len(chunks) if chunks is not None else 0

        if summary is None:
            self.logger.info(
                f"ak-ifc file-reconciliation: SKIPPED — HDFC savings "
                f"summary not parseable from file {file_id!r} "
                f"(num_chunks={num_chunks}). Extraction stands."
            )
            return

        if not recon.diverged:
            self.logger.info(
                f"ak-ifc file-reconciliation: CLEAN — extracted totals "
                f"within tolerance of PDF summary. "
                f"file={file_id!r} num_chunks={num_chunks} "
                f"debits: extracted={recon.extracted_debits} vs "
                f"stated={recon.stated_debits}; "
                f"credits: extracted={recon.extracted_credits} vs "
                f"stated={recon.stated_credits}"
            )
            return

        # DIVERGED — fallback.
        # ak-n44: structured event so a grep for
        # '"event":"reconciliation_divergent"' produces a
        # per-file ledger of every divergence + the extracted vs
        # stated totals + which checks fired.
        from utils.pipeline_logging import emit_reconciliation_divergent
        emit_reconciliation_divergent(
            self.logger,
            file_id=file_id,
            bank=detected_bank,
            extracted_debits=recon.extracted_debits,
            extracted_credits=recon.extracted_credits,
            stated_debits=recon.stated_debits,
            stated_credits=recon.stated_credits,
            checked_fields=recon.checked_fields,
            reason=recon.reason,
            extra={"num_chunks": num_chunks},
        )
        self.logger.warning(
            f"ak-ifc file-reconciliation: DIVERGED — masked extraction "
            f"totals disagree with the PDF's savings summary at file "
            f"level. file={file_id!r} bank={detected_bank} "
            f"num_chunks={num_chunks} reason={recon.reason} "
            f"checked={list(recon.checked_fields)}. "
            f"Falling back to unfiltered extraction on ALL chunks "
            f"(over-parse is recoverable via ak-8l5 dedup; under-parse "
            f"is silent loss under Overseer's zero-loss constraint)."
        )

        # MINOR 1: mark the file for follow-up review.
        try:
            self.transaction_service.mark_reconciliation_fallback(
                file_id, user_id=user_id,
            )
        except Exception as e:  # pragma: no cover — defensive
            self.logger.warning(
                f"ak-ifc file-reconciliation: mark_reconciliation_fallback "
                f"threw: {e}. Continuing with re-run regardless."
            )

        # Re-run each chunk in no-mask mode. ak-8l5-v2 dedup at the
        # storage layer handles the redundant inserts — existing rows
        # collapse via primary hash; previously-missed savings rows
        # land. The final DB state reflects the union of both passes.
        for start, end in chunks:
            self.logger.info(
                f"ak-ifc file-reconciliation: re-running chunk "
                f"{start}-{end} with force_no_mask=True"
            )
            try:
                await self._run_text_chunk(
                    text_mode_options, pdf_path, email, user_id, password,
                    page_start=start, page_end=end,
                    total_pages=total_pages,
                    preset_file_id=file_id,
                    force_no_mask=True,
                )
            except Exception as e:  # pragma: no cover — defensive
                self.logger.error(
                    f"ak-ifc file-reconciliation: no-mask re-run of "
                    f"chunk {start}-{end} failed: {e}. Data partial; "
                    f"file is tagged for manual review."
                )

    # JSON schema for structured output in text mode.
    # ak-8l5: adds bank_reference_id + line_position per row.
    # reference_number stays for informational purposes; the dedup
    # engine uses bank_reference_id + line_position instead.
    _TXN_OUTPUT_SCHEMA = {
        "type": "object",
        "properties": {
            "transactions": {
                "type": "array",
                "items": {
                    "type": "object",
                    "properties": {
                        "date": {"type": "string", "description": "DD/MM/YYYY"},
                        "description": {"type": "string"},
                        "amount": {"type": "number", "description": "positive=debit, negative=credit"},
                        "reference_number": {"type": "string", "description": "Chq./Ref.No. or UPI/IMPS/NEFT reference number from the statement"},
                        "bank_reference_id": {
                            "type": ["string", "null"],
                            "description": (
                                "ak-8l5 primary dedup key. Per-tx "
                                "bank-native ref extracted from the "
                                "narration (UPI/IMPS/NEFT/MBSF/cheque). "
                                "null for genuinely ref-less rows."
                            ),
                        },
                        "line_position": {
                            "type": ["integer", "null"],
                            "description": (
                                "ak-8l5 positional fallback anchor. "
                                "File-level 1-indexed line number "
                                "matching the [Ln] marker in the "
                                "source text. Same row across chunk "
                                "re-reads must emit the same number."
                            ),
                        },
                    },
                    "required": ["date", "description", "amount", "reference_number"],
                },
            },
        },
        "required": ["transactions"],
    }

    def _compute_hdfc_section_keep_mask(self, doc):
        """ak-ifc: identify the SAVINGS section(s) in an HDFC combined
        statement and return a per-file-line keep mask.

        HDFC monthly PDFs are combined statements containing 5 sub-
        accounts (savings, credit card, fixed deposit, mutual fund,
        recurring deposit / PPF, etc.). The pre-ak-ifc extractor
        treated the whole file as one savings statement, letting
        other sub-accounts leak into the savings fileID:
          - Apr 2026: +₹292k over-parse (non-savings debits treated
            as savings)
          - May 2026: -₹50k / -₹109k under-parse
          - All 12 HDFC files affected

        Fix: detect section headers ("Statement of Account for :
        SAVINGS ...") in the raw text, mark only savings-section
        lines as kept, blank the rest.

        Returns a tuple (full_lines, keep_mask, page_line_ranges,
        spans):
          - full_lines: list[str] of every line in the doc, in
            reading order, with a synthetic '\f<PAGE:N>' marker line
            preceding each page's raw text
          - keep_mask: list[bool] parallel to full_lines
          - page_line_ranges: dict[page_num → (start_idx, end_idx)]
            into full_lines
          - spans: list[SectionSpan] from utils.statement_sections
            for logging + auditing

        Line count is PRESERVED per page (non-savings lines are
        blanked, not deleted) so the ak-8l5 [Ln] file-level markers
        the chunker adds later still point at stable positions.
        """
        from utils.statement_sections import (
            detect_hdfc_sections,
            SectionType,
        )

        # Build a doc-wide line array with synthetic per-page markers
        # (a form-feed char + page tag) so we can map full-doc line
        # indices back to their originating page for the chunk slice.
        # Form feed is chosen because real bank text never contains
        # it; safe as a sentinel.
        full_lines: list[str] = []
        page_line_ranges: dict[int, tuple[int, int]] = {}
        for pn in range(1, doc.page_count + 1):
            marker_idx = len(full_lines)
            full_lines.append(f"\f<PAGE:{pn}>")
            page_start_idx = len(full_lines)
            raw_page = doc[pn - 1].get_text("text")
            for line in raw_page.split("\n"):
                full_lines.append(line)
            page_end_idx = len(full_lines) - 1
            page_line_ranges[pn] = (page_start_idx, page_end_idx)

        full_text = "\n".join(full_lines)
        spans = detect_hdfc_sections(full_text)

        keep_mask = [False] * len(full_lines)
        savings_span_count = 0
        for span in spans:
            if span.section == SectionType.SAVINGS:
                savings_span_count += 1
                # end_line is inclusive, but clamp defensively.
                stop = min(span.end_line + 1, len(keep_mask))
                for i in range(span.start_line, stop):
                    keep_mask[i] = True

        # Also keep the page-marker lines themselves — they're
        # invisible to the LLM (we don't include them in the chunk
        # text) but callers may want to walk the mask directly.
        return full_lines, keep_mask, page_line_ranges, spans, savings_span_count

    @staticmethod
    def _extract_masked_page_text(full_lines, page_line_ranges, keep_mask, pn):
        """Reconstruct one page's raw text with non-kept lines
        blanked to empty strings.

        Line count is preserved so the [Ln] file-level annotator that
        runs downstream still produces stable positions across chunk
        re-reads.
        """
        start, end = page_line_ranges[pn]
        kept = [
            full_lines[i] if keep_mask[i] else ""
            for i in range(start, end + 1)
        ]
        return "\n".join(kept)

    @staticmethod
    def _strip_page_header(page_text):
        """Strip the repeated HDFC Smart Statement header from a page.

        Each page starts with account details (name, address, branch, etc.)
        followed by transaction data. We strip everything up to and including
        the 'Expected AMB' line to avoid confusing the LLM.
        """
        lines = page_text.split('\n')
        # Find the end of header — look for 'Expected AMB' or 'MICR' lines
        header_end = 0
        for i, line in enumerate(lines):
            if 'Expected AMB' in line or 'MICR' in line:
                header_end = i + 1
        # Also skip address block lines after MICR (city, state, country, pincode)
        while header_end < len(lines):
            stripped = lines[header_end].strip()
            if stripped and (
                stripped.isdigit() or  # pincode
                stripped in ('India', 'Karnataka', 'Bengaluru') or
                len(stripped) < 3  # short fragments
            ):
                header_end += 1
            else:
                break
        if header_end > 5:
            return '\n'.join(lines[header_end:])
        return page_text

    async def _run_text_chunk(self, options, pdf_path, email, user_id, password,
                              page_start, page_end, total_pages,
                              preset_file_id=None, *, force_no_mask=False):
        """Process a text-mode PDF chunk using query() + structured output.

        No MCP tools — Claude returns structured JSON via output_format,
        we parse and insert directly.

        ak-ifc v2 (`force_no_mask`): when the reconciliation backstop
        detects a divergence between extracted savings totals and the
        PDF's own summary, this method calls ITSELF a second time with
        `force_no_mask=True` — the recursive call bypasses the section
        filter entirely so no savings row can be silently lost. The
        recursion is bounded (the recursed call has hdfc_mask=None so
        it can't recurse further).

        Returns dict with insert stats: {"called": bool, "inserted": int, "duplicates": int}
        """
        import fitz as _fitz
        from services.bankFormatRules import get_bank_from_sender, get_format_rules
        from services.mailProcessorToolExecutor import _handle_insert_batch_transactions

        detected_bank = email.get("_bank") or get_bank_from_sender(email.get("sender", ""))
        bank_rules = get_format_rules(detected_bank)

        # ak-8l5: compute file-level line offsets so `line_position`
        # in the extractor output points at the RAW FILE, not the chunk.
        # We open the doc twice — once to compute per-page starting
        # line offsets across the whole file, then again below to
        # pull the chunk's text. It's an O(N) scan but the file is
        # typically <20 pages; the offset dict is tiny.
        doc = _fitz.open(pdf_path)
        if doc.needs_pass and password:
            doc.authenticate(password)
        page_start_line = {}  # page_num → 1-indexed line offset of that page's first line
        line_ptr = 1
        for pn in range(1, doc.page_count + 1):
            page_start_line[pn] = line_ptr
            raw_page_text = doc[pn - 1].get_text("text")
            # +1 for the trailing newline PyMuPDF appends; consistent
            # across pages so re-reads collapse identically.
            line_ptr += raw_page_text.count("\n") + 1

        # ak-ifc: for HDFC combined statements, precompute a per-file-
        # line mask that keeps only the SAVINGS section. Non-savings
        # lines are blanked to empty strings so line counts stay stable
        # (ak-8l5 [Ln] markers depend on that).
        # ak-ifc v2: the reconciliation backstop may re-enter this
        # method with force_no_mask=True; honor that flag by skipping
        # the mask computation entirely.
        hdfc_mask = None
        hdfc_spans_for_reconciliation = None
        hdfc_full_text_for_reconciliation = None
        if detected_bank == "HDFC_DEBIT" and not force_no_mask:
            (full_lines, keep_mask, page_line_ranges, spans,
             savings_span_count) = self._compute_hdfc_section_keep_mask(doc)
            hdfc_mask = (full_lines, keep_mask, page_line_ranges)
            # ak-ifc v2: keep the spans + full text around so the
            # reconciliation backstop can parse the savings summary
            # from the same view of the doc without re-scanning it.
            hdfc_spans_for_reconciliation = spans
            hdfc_full_text_for_reconciliation = "\n".join(full_lines)
            from utils.statement_sections import summarize_sections
            self.logger.info(
                f"HDFC combined-statement section split: "
                f"{summarize_sections(spans)}; "
                f"savings_spans={savings_span_count} "
                f"(pages {page_start}-{page_end} of {doc.page_count})"
            )
            if savings_span_count == 0:
                self.logger.warning(
                    f"HDFC statement has 0 detected SAVINGS sections — "
                    f"either not a combined-statement layout OR the "
                    f"header regex missed. Falling back to full-text "
                    f"extraction (pre-ak-ifc behavior) to avoid dropping "
                    f"all transactions."
                )
                hdfc_mask = None
                # ak-dby v2 (reviewer BOUNCE hq-wisp-*): the flag is
                # only appropriate when the detector saw a
                # combined-statement shape but MISSED the savings
                # header specifically — i.e. non-savings sub-accounts
                # WERE detected. A pure single-account statement (no
                # section headers at all) also produces
                # savings_spans==0 but shouldn't be flagged, because
                # there's no non-savings contamination for the strip
                # endpoint to clean up.
                #
                # Compute non_savings_span_count from the same spans
                # the mask computation returned.
                from utils.statement_sections import SectionType
                non_savings_span_count = sum(
                    1 for s in spans
                    if s.section not in (
                        SectionType.SAVINGS,
                        SectionType.UNKNOWN,
                    )
                )
                if preset_file_id and non_savings_span_count >= 1:
                    try:
                        self.transaction_service.mark_reconciliation_fallback(
                            preset_file_id, user_id=user_id,
                        )
                        self.logger.info(
                            f"ak-dby: tagged fileID={preset_file_id!r} "
                            f"reconciliation_fallback=True "
                            f"(savings_spans=0 AND "
                            f"non_savings_spans={non_savings_span_count} — "
                            f"combined-statement layout whose savings "
                            f"header regex missed) so "
                            f"/admin/stripFallbackRows can clean up "
                            f"leaked non-savings tx."
                        )
                    except Exception as e:  # pragma: no cover — defensive
                        self.logger.warning(
                            f"ak-dby: mark_reconciliation_fallback failed "
                            f"for fileID={preset_file_id!r}: {e}. Data "
                            f"lands regardless; strip endpoint won't fire "
                            f"until the file is tagged manually."
                        )
                else:
                    # savings_spans=0 AND non_savings_spans=0 →
                    # pure single-account statement (or full-doc
                    # UNKNOWN). No contamination possible; nothing
                    # to strip. Skip the tag.
                    self.logger.info(
                        f"ak-dby: NOT tagging fileID={preset_file_id!r} "
                        f"(savings_spans=0, non_savings_spans="
                        f"{non_savings_span_count}) — likely a "
                        f"pure single-account statement with no "
                        f"combined-layout contamination."
                    )

        # Now pull the chunk's text, annotate each line with its
        # file-level line number so the LLM can echo the correct
        # position onto each extracted tx.
        page_texts = []
        for pn in range(page_start, page_end + 1):
            page = doc[pn - 1]
            if hdfc_mask is not None:
                # Masked text: non-savings lines are blanked so the
                # LLM only sees savings-section rows. Line-count is
                # preserved so downstream annotations stay stable
                # (ak-8l5 [Ln] markers below depend on this).
                full_lines, keep_mask, page_line_ranges = hdfc_mask
                text = self._extract_masked_page_text(
                    full_lines, page_line_ranges, keep_mask, pn,
                )
            else:
                text = page.get_text("text")
            clean_text = self._strip_page_header(text)
            annotated_lines = []
            local_line = 0
            for raw_line in clean_text.split("\n"):
                # Use the file-level line number for THIS page's
                # first line as anchor; increment per local line.
                # Header stripping may drop lines — that's fine; the
                # marker still points at the raw-file position of the
                # line that survived.
                file_line = page_start_line[pn] + local_line
                annotated_lines.append(f"[L{file_line}] {raw_line}")
                local_line += 1
            page_texts.append(
                f"--- Page {pn} ---\n" + "\n".join(annotated_lines)
            )
        doc.close()
        extracted_text = "\n\n".join(page_texts)

        prompt_text = (
            f"Extract ALL financial transactions from this bank statement text.\n\n"
            f"```\n{extracted_text}\n```\n\n"
        )
        if bank_rules:
            prompt_text += f"{bank_rules}\n"
        prompt_text += (
            f"Return a JSON object with a \"transactions\" array.\n"
            f"Each transaction: {{\"date\": \"DD/MM/YYYY\", \"description\": \"...\", "
            f"\"amount\": N, \"reference_number\": \"...\", "
            f"\"bank_reference_id\": \"...\", \"line_position\": N}}\n"
            f"where positive=debit (withdrawal), negative=credit (deposit).\n"
            f"\n"
            f"### ak-8l5 dedup fields (REQUIRED per row)\n"
            f"\n"
            f"- `bank_reference_id` (string or null): per-tx bank-native "
            f"identifier extracted from the narration. Examples: "
            f"HDFC UPI ref (digits after 'UPI-'), IMPS ref, NEFT UTR, "
            f"BOI MBSF number (middle numeric block of MBSF/…/…), cheque "
            f"number. Return null for genuinely ref-less rows (cash "
            f"deposit / interest / fee) — never guess. The same tx read "
            f"from two adjacent chunks MUST extract to the same ref.\n"
            f"- `line_position` (integer): the file-level line number "
            f"marked in the source above as `[Ln]`. Every line in the "
            f"extracted text carries a `[Ln]` prefix — echo the number "
            f"from the line where the tx's date/amount appears. This "
            f"lets the dedup engine collapse chunk re-reads of "
            f"ref-less rows via (file, line_position).\n"
            f"\n"
            f"The reference_number is the Chq./Ref.No. from the statement — the digits-only line "
            f"that appears between the narration and the Value Date. Keep this field too; it's "
            f"informational and doesn't participate in dedup post-ak-8l5.\n"
            f"IMPORTANT: Every row is a UNIQUE transaction. Do NOT skip transactions even if "
            f"the merchant name, amount, or description looks similar to another — each has a "
            f"unique reference number and must be extracted separately.\n"
            f"Extract from the FIRST transaction on each page to the LAST. Do not stop early.\n"
            f"ONLY extract transactions that appear in the text. Do NOT invent any.\n"
            f"\n"
            f"### ak-ifc combined-statement caveat (HDFC)\n"
            f"HDFC monthly statements bundle 5 sub-accounts (savings, "
            f"credit card, fixed deposit, mutual fund, recurring deposit). "
            f"The extractor pre-filters the raw text to keep ONLY the "
            f"SAVINGS section before you see it — non-savings lines are "
            f"replaced with blanks. So the only rows in the text below "
            f"are legitimate savings-account transactions; extract them "
            f"all. If you see a 'Statement of Account for : <non-savings>' "
            f"header slip through, ignore the section under it.\n"
        )

        # Health probe (hq-wisp-wzagg): mailProcessor SDK call diagnostic so
        # we can independently confirm this codepath is alive while
        # chat.* and rate.* are misbehaving. Captures model + sys_prompt sha
        # for cross-correlation with the agent_run log line.
        _sp_text = options.system_prompt or ""
        _sp_sha_text = hashlib.sha256(_sp_text.encode()).hexdigest()[:12]
        self.logger.info(
            f"Chunk {page_start}-{page_end} prompt size: {len(prompt_text)} chars, "
            f"mode=text (structured output), bank={detected_bank}, "
            f"model={options.model} sys_prompt_sha={_sp_sha_text}"
        )

        # Send query via run_query_collect — one-shot, structured output.
        # ak-wty: F12 "Fatal error in message reader" is transient on
        # ~5-10% of chunk runs under production load. Backoff-retry up
        # to MAX_RETRIES on a known-transient signature; if all
        # attempts fail (or the error is non-retryable) fall through
        # to the existing error-return path. A structured log records
        # the fileID + chunk indices + attempt count so a follow-up
        # sweep can find any file that exhausted its retry budget.
        import asyncio as _asyncio
        from utils.sdk_retry import (
            MAX_RETRIES,
            is_retryable_sdk_error,
            retry_delay_seconds,
        )
        run_result = await run_query_collect(
            agent="pdf.text", options=options, prompt=prompt_text,
        )
        attempts_used = 1
        while (
            run_result.error
            and is_retryable_sdk_error(run_result.error)
            and attempts_used <= MAX_RETRIES
        ):
            delay = retry_delay_seconds(attempts_used - 1)
            # ak-n44: structured chunk-retry event so a single
            # `grep '"event":"chunk_retry"' | jq` produces a
            # per-file per-chunk retry ledger.
            from utils.pipeline_logging import emit_chunk_retry
            emit_chunk_retry(
                self.logger,
                file_id=preset_file_id,
                page_range=[page_start, page_end],
                attempt=attempts_used,
                max_attempts=MAX_RETRIES + 1,
                message=str(run_result.error),
                extra={
                    "delay_seconds": delay,
                    "processing_mode": "text",
                },
            )
            self.logger.warning(
                f"ak-wty: transient SDK error on chunk "
                f"{page_start}-{page_end} (file_id={preset_file_id!r}) "
                f"attempt {attempts_used}/{MAX_RETRIES + 1}: "
                f"{run_result.error!r}. Sleeping {delay}s and retrying."
            )
            await _asyncio.sleep(delay)
            attempts_used += 1
            run_result = await run_query_collect(
                agent="pdf.text", options=options, prompt=prompt_text,
            )
        structured_data = run_result.structured_output
        fallback_text = run_result.text
        error_msg = run_result.error
        self.logger.info(
            f"Chunk {page_start}-{page_end} run: "
            f"has_structured_output={structured_data is not None}, "
            f"text_chars={len(fallback_text)}, error={bool(error_msg)}, "
            f"attempts_used={attempts_used}"
        )

        if error_msg:
            # ak-wty: retries exhausted (or non-retryable error).
            # ak-n44: emit structured chunk_error so the operator's
            # grep produces a full per-chunk failure record
            # (fileID + page range + error_type + attempts +
            # retryable flag) without hand-parsing free-form text.
            from utils.pipeline_logging import emit_chunk_error
            emit_chunk_error(
                self.logger,
                file_id=preset_file_id,
                page_range=[page_start, page_end],
                message=str(error_msg),
                extra={
                    "attempts_used": attempts_used,
                    "retryable": is_retryable_sdk_error(error_msg),
                    "processing_mode": "text",
                },
            )
            self.logger.error(
                f"ak-wty: text chunk error unresolved after "
                f"{attempts_used} attempt(s). "
                f"file_id={preset_file_id!r} pages={page_start}-{page_end} "
                f"retryable={is_retryable_sdk_error(error_msg)} "
                f"error={error_msg!r}"
            )
            return {
                "called": False,
                "inserted": 0,
                "duplicates": 0,
                # ak-wty: expose the retry counts + failure signal to
                # callers (e.g. _run_all_chunks_async) so they can
                # tag the file / bump per-chunk failure counters.
                "sdk_retry_attempts": attempts_used,
                "sdk_error": error_msg,
            }

        # Fallback: if structured_output is None, try parsing AssistantMessage text
        if structured_data is None and fallback_text:
            self.logger.warning(
                f"Chunk {page_start}-{page_end}: No structured_output, "
                f"trying fallback text parse ({len(fallback_text)} chars)"
            )
            # Strip markdown code fences if present
            text = fallback_text.strip()
            if text.startswith("```"):
                text = text.split("\n", 1)[1] if "\n" in text else text[3:]
                if text.endswith("```"):
                    text = text[:-3]
                text = text.strip()
            try:
                structured_data = json.loads(text)
            except json.JSONDecodeError:
                self.logger.error(
                    f"Chunk {page_start}-{page_end}: Fallback parse failed. "
                    f"First 300 chars: {fallback_text[:300]}"
                )

        if structured_data is None:
            self.logger.error(
                f"Chunk {page_start}-{page_end}: No structured_output and no parseable fallback"
            )
            return {"called": False, "inserted": 0, "duplicates": 0}

        transactions = structured_data.get("transactions", [])

        if not transactions:
            self.logger.warning(
                f"Chunk {page_start}-{page_end}: Claude returned 0 transactions"
            )
            return {"called": True, "inserted": 0, "duplicates": 0}

        self.logger.info(
            f"Chunk {page_start}-{page_end}: Claude returned {len(transactions)} transactions, inserting..."
        )

        # ak-ifc v3: the per-chunk reconciliation gate that lived
        # here in v2 was hoisted to file level in
        # _run_all_chunks_async._run_hdfc_file_level_reconciliation.
        # Reason: v2's `page_start==1 AND page_end==total_pages` gate
        # meant only HDFC files ≤ pages_per_chunk (default 2) actually
        # exercised the backstop — the combined statements that the
        # whole ak-ifc effort targets are always multi-chunk, so the
        # backstop never fired on them. See ak-ifc-v3 BOUNCE from
        # akkountant/crew/akkountant_lead.
        # We keep `force_no_mask` so the file-level orchestrator can
        # re-invoke each chunk in a no-mask mode.

        # Insert directly via the existing tool executor logic
        insert_args = {
            "transactions": transactions,
            "bank": detected_bank,
            "source": "Statement",
            "file_id": preset_file_id,
            "gmail_message_id": email.get("gmail_id", ""),
        }
        result = _handle_insert_batch_transactions(
            insert_args, user_id,
            self.transaction_service,
            reconciliation_service=self.reconciliation_service,
        )
        inserted = result.get("inserted", 0)
        duplicates = result.get("duplicates", 0)

        self.logger.info(
            f"Chunk {page_start}-{page_end} inserted: {inserted} new, {duplicates} dupes"
        )
        return {"called": True, "inserted": inserted, "duplicates": duplicates}

    async def _run_pdf_chunk_async(self, pdf_path, email, user_id, password,
                       page_start, page_end, total_pages, is_final_chunk,
                       skip_reconciliation=False, preset_file_id=None,
                       processing_mode="image"):
        """Run a single Claude query to process a range of pages from a PDF (MCP mode).

        Used for image mode. Text mode uses _run_text_chunk instead.
        Async version — must be called within an existing event loop.
        Returns dict with insert stats: {"called": bool, "inserted": int, "duplicates": int}
        """
        # Track whether insert_batch_transactions was called
        insert_called = {"value": False, "inserted": 0, "duplicates": 0}

        # Build fresh MCP tools and server for each chunk (clean context)
        sdk_tools = self._build_sdk_tools(
            user_id, chunk_page_end=page_end, insert_called=insert_called,
        )
        mcp_server = create_sdk_mcp_server(
            name=MCP_SERVER_NAME,
            tools=sdk_tools,
        )

        system_prompt = PDF_SYSTEM_PROMPT

        options = self._make_pdf_chunk_options(system_prompt, mcp_server)

        # Detect bank and get format rules for dynamic injection
        from services.bankFormatRules import get_bank_from_sender, get_format_rules
        detected_bank = email.get("_bank") or get_bank_from_sender(email.get("sender", ""))
        bank_rules = get_format_rules(detected_bank)

        # Tool name for image mode (text mode embeds text directly)
        fetch_tool = "get_pdf_pages"

        # Build prompt with email context (omit body to avoid password confusion)
        prompt_text = (
            f"Process this financial email with a PDF attachment.\n\n"
            f"Email details:\n"
            f"From: {email.get('sender', 'Unknown')}\n"
            f"Subject: {email.get('subject', 'No subject')}\n"
            f"Date: {email.get('time', 'Unknown')}\n"
            f"Gmail ID: {email.get('gmail_id', 'N/A')}\n\n"
            f"PDF file path: {pdf_path}\n"
            f"Total pages in this chunk: {page_end - page_start + 1}\n"
        )
        if password:
            prompt_text += f"PDF password: {password}\n"
        else:
            prompt_text += (
                f"The PDF is already unlocked — no password needed. "
                f"Focus on extracting all financial data from the pages.\n"
            )

        # For text mode: extract text and embed directly in the prompt
        # so the agent only needs to call insert_batch_transactions (one tool call)
        # Keep extracted_text accessible for the follow-up prompt if needed
        extracted_text = None
        if processing_mode == "text":
            import fitz as _fitz
            doc = _fitz.open(pdf_path)
            if doc.needs_pass and password:
                doc.authenticate(password)
            page_texts = []
            for pn in range(page_start, page_end + 1):
                page = doc[pn - 1]
                text = page.get_text("text")
                page_texts.append(f"--- Page {pn} ---\n{text}")
            doc.close()
            extracted_text = "\n\n".join(page_texts)

            file_id_instruction = ""
            period_instruction = ""
            if preset_file_id:
                file_id_instruction = f"Use file_id=\"{preset_file_id}\" in insert_batch_transactions.\n"
            if skip_reconciliation:
                period_instruction = (
                    "Do NOT include period_start or period_end in insert_batch_transactions — "
                    "reconciliation will be handled separately after all chunks complete.\n"
                )

            prompt_text += (
                f"\nBelow is the extracted text from pages {page_start}-{page_end} of {total_pages} total pages.\n"
                f"Parse the text and extract ALL transactions.\n\n"
                f"```\n{extracted_text}\n```\n\n"
            )
            if bank_rules:
                prompt_text += f"{bank_rules}\n"
            prompt_text += (
                f"Call insert_batch_transactions with all transactions you found.\n"
                f"{file_id_instruction}"
                f"{period_instruction}"
                f"Use source=\"Statement\", gmail_message_id=\"{email.get('gmail_id', '')}\".\n"
                f"Format each transaction as {{date: \"DD/MM/YYYY\", description: \"...\", amount: N}} "
                f"where positive=debit, negative=credit.\n"
                f"After inserting, call report_result with the outcome.\n"
                f"\nCRITICAL RULES:\n"
                f"1. Do NOT output any text before calling the tool — call insert_batch_transactions IMMEDIATELY as your first action.\n"
                f"2. Do NOT list or describe transactions in text — put them directly in the tool call.\n"
                f"3. ONLY extract transactions that appear in the provided text — do NOT invent any.\n"
            )
        # Image mode: agent needs to call get_pdf_pages tool first
        elif page_start == 1 and page_end == total_pages:
            # Single chunk — process everything
            prompt_text += (
                f"\nPlease:\n"
                f"1. Call {fetch_tool} with page_range=\"all\" to fetch all pages\n"
                f"2. If has_more is true, call {fetch_tool} again starting from next_page\n"
                f"3. Extract all financial data from the pages\n"
                f"4. Call insert_batch_transactions with all extracted data\n"
                f"5. Call report_result with the outcome\n"
                f"\nYou MUST call insert_batch_transactions — do NOT just describe the data in text.\n"
            )
        else:
            # Chunked — process specific page range
            prompt_text += (
                f"\nYou are processing pages {page_start}-{page_end} of a {total_pages}-page document.\n"
                f"This document is being processed in separate chunks. Each chunk is independent.\n"
            )
            if page_start > 1:
                prompt_text += (
                    f"Earlier pages have already been processed and inserted separately. "
                    f"Only extract transactions from pages {page_start}-{page_end}.\n"
                )
            file_id_instruction = ""
            period_instruction = ""
            if preset_file_id:
                file_id_instruction = f"Use file_id=\"{preset_file_id}\" in insert_batch_transactions.\n"
            if skip_reconciliation:
                period_instruction = (
                    "Do NOT include period_start or period_end in insert_batch_transactions — "
                    "reconciliation will be handled separately after all chunks complete.\n"
                )

            prompt_text += (
                f"\nIMPORTANT — you MUST follow these steps in order:\n"
                f"1. Call {fetch_tool} with page_range=\"{page_start}-{page_end}\"\n"
                f"2. If has_more is true, call {fetch_tool} again starting from next_page "
                f"(but do NOT go past page {page_end})\n"
                f"3. Extract all financial data (transactions) from these pages\n"
                f"4. Call insert_batch_transactions with the extracted transactions.\n"
                f"   {file_id_instruction}"
                f"   {period_instruction}"
                f"5. Call report_result with the outcome — set status=\"success\" "
                f"even if this is a partial batch (more pages will follow in a separate query)\n"
                f"\nCRITICAL: You MUST call insert_batch_transactions as a tool call.\n"
                f"Do NOT just describe the transactions in text — you must call the tool.\n"
            )

        # Health probe (hq-wisp-wzagg): mirrors text-mode site at ~L1675
        # for cross-mode comparison.
        _sp_img = options.system_prompt or ""
        _sp_sha_img = hashlib.sha256(_sp_img.encode()).hexdigest()[:12]
        self.logger.info(
            f"Chunk {page_start}-{page_end} prompt size: {len(prompt_text)} chars, "
            f"mode={processing_mode}, bank={detected_bank}, "
            f"model={options.model} sys_prompt_sha={_sp_sha_img}"
        )

        text_parts = []
        error_msg = None

        async def run_query(prompt_content):
            nonlocal error_msg

            async def make_prompt():
                yield {
                    "type": "user",
                    "session_id": "",
                    "message": {"role": "user", "content": prompt_content},
                    "parent_tool_use_id": None,
                }

            res = await run_query_collect(
                agent="pdf.image", options=options, prompt=make_prompt(),
            )
            if res.text:
                text_parts.append(res.text)
            if res.error:
                error_msg = f"Claude error: {res.error}"

        import asyncio
        try:
            await asyncio.wait_for(run_query(prompt_text), timeout=300)
        except asyncio.TimeoutError:
            error_msg = "Claude error: rate_limit"

        # --- Post-query handling: stream errors, follow-ups ---
        agent_output = "".join(text_parts).strip()

        # Detect stream/connection errors that warrant rebuilding MCP
        is_stream_error = False
        stream_keywords = ["stream closed", "broken pipe", "connection reset",
                           "connection refused", "stream error"]
        if error_msg and any(kw in error_msg.lower() for kw in stream_keywords):
            is_stream_error = True
        if not is_stream_error and agent_output:
            if any(kw in agent_output.lower() for kw in stream_keywords):
                is_stream_error = True

        if not insert_called["value"]:
            if is_stream_error:
                # Stream/connection error — rebuild fresh MCP and retry original prompt
                self.logger.warning(
                    f"Stream error detected for chunk {page_start}-{page_end}. "
                    f"Rebuilding MCP server and retrying with original prompt..."
                )
                await asyncio.sleep(3)

                insert_called = {"value": False}
                sdk_tools = self._build_sdk_tools(
                    user_id, chunk_page_end=page_end, insert_called=insert_called,
                )
                mcp_server = create_sdk_mcp_server(
                    name=MCP_SERVER_NAME, tools=sdk_tools,
                )
                options = self._make_pdf_chunk_options(system_prompt, mcp_server)
                error_msg = None
                text_parts.clear()

                try:
                    await asyncio.wait_for(run_query(prompt_text), timeout=300)
                except asyncio.TimeoutError:
                    error_msg = "Claude error: rate_limit"

            elif not error_msg:
                # Agent output text but didn't call tool — send follow-up with fresh MCP
                self.logger.warning(
                    f"FOLLOW-UP TRIGGERED for chunk {page_start}-{page_end}: "
                    f"agent did not call insert_batch_transactions. "
                    f"Agent text output ({len(agent_output)} chars): {agent_output[:500]}..."
                )
                await asyncio.sleep(2)

                # Rebuild fresh MCP server for the follow-up
                insert_called = {"value": False}
                sdk_tools = self._build_sdk_tools(
                    user_id, chunk_page_end=page_end, insert_called=insert_called,
                )
                mcp_server = create_sdk_mcp_server(
                    name=MCP_SERVER_NAME, tools=sdk_tools,
                )
                options = self._make_pdf_chunk_options(system_prompt, mcp_server)

                file_id_hint = ""
                if preset_file_id:
                    file_id_hint = f"Use file_id=\"{preset_file_id}\". "
                followup_prompt = ""
                # Include the original source text so the agent doesn't hallucinate
                if extracted_text:
                    followup_prompt += (
                        f"Here is the extracted text from PDF pages {page_start}-{page_end}:\n"
                        f"```\n{extracted_text}\n```\n\n"
                    )
                if bank_rules:
                    followup_prompt += f"{bank_rules}\n"
                followup_prompt += (
                    f"Do NOT output any text. IMMEDIATELY call insert_batch_transactions with all "
                    f"transactions from the text above. ONLY extract transactions that appear in the text.\n"
                    f"{file_id_hint}"
                    f"Use bank=\"{detected_bank}\", source=\"Statement\", "
                    f"gmail_message_id=\"{email.get('gmail_id', '')}\". "
                    f"Format: {{date: \"DD/MM/YYYY\", description: \"...\", amount: N}} "
                    f"(positive=debit, negative=credit).\n"
                )
                text_parts.clear()

                try:
                    await asyncio.wait_for(run_query(followup_prompt), timeout=300)
                except asyncio.TimeoutError:
                    error_msg = "Claude error: rate_limit"

        if error_msg:
            self.logger.error(
                f"PDF chunk analysis error (pages {page_start}-{page_end}): {error_msg}"
            )
            if "rate_limit" in error_msg:
                raise RuntimeError(error_msg)
        else:
            summary = "".join(text_parts).strip()
            self.logger.info(
                f"PDF chunk complete (pages {page_start}-{page_end}): "
                f"{summary[:200]}..."
            )

        return {
            "called": insert_called["value"],
            "inserted": insert_called.get("inserted", 0),
            "duplicates": insert_called.get("duplicates", 0),
        }

    # ── PDF chunk options ─────────────────────────────────────────────

    @staticmethod
    def _make_pdf_chunk_options(system_prompt, mcp_server):
        """Build the ClaudeAgentOptions used for PDF chunk processing.

        Three sites used to construct this identically (initial run, stream-
        error retry, no-tool-call follow-up). Shared here so a fix in one
        place propagates to all three.
        """
        return ClaudeAgentOptions(
            model="sonnet",
            system_prompt=system_prompt,
            max_turns=MAX_TURNS,
            mcp_servers={MCP_SERVER_NAME: mcp_server},
            permission_mode="bypassPermissions",
            # MCP tool names listed explicitly (defensive — see doc §7.2)
            allowed_tools=[
                f"mcp__{MCP_SERVER_NAME}__{t['name']}" for t in MAIL_PROCESSOR_TOOLS
            ],
        )

    # ── MCP tool building ──────────────────────────────────────────────

    def _build_sdk_tools(self, user_id, chunk_page_end=None, insert_called=None, email_lookup=None):
        """Build SdkMcpTool objects for the mail processor agent.

        Args:
            chunk_page_end: If set, clamp has_more/total_pages to this boundary
                so the agent doesn't try to read beyond its assigned chunk.
            insert_called: If provided, a dict with key "value" that gets set to
                True when insert_batch_transactions is called.
            email_lookup: If provided, a dict mapping gmail_id -> email metadata.
                Used to auto-inject gmail_message_id and email timestamp into
                insert_transaction calls instead of relying on Claude.
        """
        sdk_tools = []

        for tool_def in MAIL_PROCESSOR_TOOLS:
            tool_name = tool_def["name"]

            async def handler(args, _tn=tool_name):
                # Auto-inject email metadata for insert_transaction calls.
                # Claude passes gmail_message_id from the email; we use it to
                # look up the authoritative timestamp from the original email
                # rather than trusting Claude's date extraction.
                if _tn == "insert_transaction" and email_lookup:
                    gmail_id = args.get("gmail_message_id")
                    if gmail_id and gmail_id in email_lookup:
                        email_meta = email_lookup[gmail_id]
                        email_time = email_meta.get("time")
                        if email_time:
                            args["date"] = email_time
                        # Ensure gmail_message_id is always set
                        args["gmail_message_id"] = gmail_id

                # Track insert_batch_transactions calls
                if _tn == "insert_batch_transactions" and insert_called is not None:
                    insert_called["value"] = True
                    txn_count = len(args.get("transactions", []))
                    self.logger.info(
                        f"Agent called insert_batch_transactions with {txn_count} transactions"
                    )

                result = execute_mail_tool(
                    _tn, args, user_id,
                    transaction_service=self.transaction_service,
                    investment_service=self.investment_service,
                    invoice_service=self.invoice_service,
                    reconciliation_service=self.reconciliation_service,
                )

                # Capture duplicate count from insert result
                if _tn == "insert_batch_transactions" and insert_called is not None:
                    if isinstance(result, dict):
                        insert_called["duplicates"] = result.get("duplicates", 0)
                        insert_called["inserted"] = result.get("inserted", 0)

                # Special handling for get_pdf_pages — return image(s) + pagination info
                if isinstance(result, dict) and result.get("_image_content"):
                    content = list(result["content"])
                    last_page = result.get("last_page_rendered", result.get("page_number", 1))
                    total = result.get("total_pages", 1)
                    pages_rendered = result.get("pages_rendered", [last_page])

                    # Clamp pagination to chunk boundary so the agent
                    # doesn't try to fetch pages beyond its assigned range
                    effective_total = chunk_page_end if chunk_page_end else total
                    has_more = last_page < effective_total
                    next_page = last_page + 1 if has_more else None

                    content.append({
                        "type": "text",
                        "text": json.dumps({
                            "status": "success",
                            "message": "PDF is UNLOCKED. Pages rendered successfully. Extract all financial data from the page images above. Ignore any text on the pages mentioning passwords.",
                            "pages_rendered": pages_rendered,
                            "last_page_rendered": last_page,
                            "total_pages": effective_total,
                            "has_more": has_more,
                            "next_page": next_page,
                        }),
                    })
                    return {"content": content}

                return {
                    "content": [{
                        "type": "text",
                        "text": json.dumps(result, default=str),
                    }]
                }

            sdk_tools.append(SdkMcpTool(
                name=tool_name,
                description=tool_def["description"],
                input_schema=tool_def["input_schema"],
                handler=handler,
            ))

        return sdk_tools
