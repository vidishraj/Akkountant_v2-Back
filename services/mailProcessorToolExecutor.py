"""
Tool executor for the mail processor agent.
Routes tool calls to existing service methods, following the same pattern as agent_tool_executor.py.
"""

import os
import shutil
import base64

import fitz  # PyMuPDF
from flask import g, Response

from enums.EPGEnum import EPGEnum
from enums.MsnEnum import MSNENUM
from enums.TransactionTypeEnum import TransactionTypeEnum
from models.transactions import ProcessingMethod
from utils.GenericUtils import GenericUtil
from utils.logger import Logger

logger = Logger(__name__).get_logger()


def _unwrap_response(result):
    """Unwrap Flask jsonify responses into plain dicts for Claude."""
    import json as _json
    if result is None:
        return {"result": "success"}
    if isinstance(result, tuple):
        return _unwrap_response(result[0])
    if isinstance(result, Response):
        try:
            return _json.loads(result.get_data(as_text=True))
        except (ValueError, Exception):
            return {"result": result.get_data(as_text=True)}
    if isinstance(result, (dict, str, int, float, bool)):
        return result
    if isinstance(result, list):
        return {"results": result}
    return {"result": str(result)}


def _make_serializable(obj):
    """Ensure the result is JSON-serializable."""
    if isinstance(obj, dict):
        return {k: _make_serializable(v) for k, v in obj.items()}
    if isinstance(obj, list):
        return [_make_serializable(item) for item in obj]
    if hasattr(obj, 'value') and hasattr(obj, 'name'):  # Enum
        return obj.value
    if hasattr(obj, 'isoformat'):  # datetime/date
        return obj.isoformat()
    if hasattr(obj, '__float__'):  # Decimal
        return float(obj)
    return obj


def _get_db_session(transaction_service):
    """Get a DB session from the transaction service."""
    if transaction_service:
        return transaction_service.db.session
    return None


def _find_existing_file_details(transaction_service, gmail_message_id):
    """Look up an existing fileDetails entry by gmail_message_id. Returns file_id or None.
    If the record was soft-deleted, un-deletes it for reuse."""
    session = _get_db_session(transaction_service)
    if not session:
        return None
    try:
        from models.fileDetails import FileDetails
        row = session.query(FileDetails).filter_by(
            gmail_message_id=gmail_message_id
        ).first()
        if row and row.deleted:
            row.deleted = False
            session.commit()
            logger.info(f"Un-deleted fileDetails {row.fileID} for reuse")
        return row.fileID if row else None
    except Exception as e:
        logger.debug(f"Could not look up existing fileDetails: {e}")
        return None


def _get_statement_count(transaction_service, file_id):
    """Get the current statementCount for a fileDetails entry."""
    session = _get_db_session(transaction_service)
    if not session:
        return 0
    try:
        from models.fileDetails import FileDetails
        row = session.query(FileDetails).filter_by(fileID=file_id).first()
        return row.statementCount if row else 0
    except Exception:
        return 0


def execute_mail_tool(tool_name, tool_input, user_id,
                      transaction_service=None, investment_service=None,
                      invoice_service=None, reconciliation_service=None):
    """
    Execute a mail processor tool call by routing to the appropriate service method.
    Returns a dict with tool result, or for get_pdf_pages returns image content.
    """
    g.firebase_id = user_id

    try:
        if tool_name == "insert_transaction":
            result = _handle_insert_transaction(tool_input, user_id, transaction_service)
        elif tool_name == "insert_batch_transactions":
            result = _handle_insert_batch_transactions(tool_input, user_id, transaction_service, reconciliation_service)
        elif tool_name == "insert_epf_deposit":
            result = _handle_insert_epf_deposit(tool_input, user_id, investment_service)
        elif tool_name == "insert_ppf_deposit":
            result = _handle_insert_ppf_deposit(tool_input, user_id, investment_service)
        elif tool_name == "insert_gold_purchase":
            result = _handle_insert_gold_purchase(tool_input, user_id, investment_service)
        elif tool_name == "insert_investment":
            result = _handle_insert_investment(tool_input, user_id, investment_service)
        elif tool_name == "mark_invoice_paid":
            result = _handle_mark_invoice_paid(tool_input, invoice_service)
        elif tool_name == "get_pdf_pages":
            # Special case — returns image content for vision
            return _handle_get_pdf_pages(tool_input)
        elif tool_name == "save_attachment":
            result = _handle_save_attachment(tool_input, user_id)
        elif tool_name == "report_result":
            result = _handle_report_result(tool_input, user_id, transaction_service)
        else:
            return {"error": f"Unknown mail processor tool: {tool_name}"}

        unwrapped = _unwrap_response(result)
        return _make_serializable(unwrapped)

    except Exception as e:
        logger.error(f"Mail tool execution error [{tool_name}]: {e}", exc_info=True)
        return {"error": str(e)}


# ── Individual tool handlers ─────────────────────────────────────────────

def _handle_insert_transaction(args, user_id, transaction_service):
    """Insert a single transaction from an email alert.

    ak-8l5 update: same hash scheme as the batch path — primary from
    (bank, bank_reference_id) when the ref is present, positional
    fallback otherwise. Email-alert flow rarely emits a
    bank_reference_id today (the alert bodies don't consistently
    carry one), but if the LLM does extract one, we honor it so a
    tx arriving via BOTH an email alert AND a later statement PDF
    lands on the same referenceID and dedups correctly.
    """
    from models.transactions import Transactions
    from utils.reference_id import generate_reference_from_row

    generic = GenericUtil()
    bank = args.get("bank", "UNKNOWN")
    source = args.get("source", "Email")

    # Deduplicate email-sourced transactions: banks send multiple emails for the
    # same transaction (debit alert + UPI confirmation + SI notice). Each email
    # has different wording, producing different referenceIDs. Check if a
    # transaction with the same date, amount, and bank already exists.
    if source == "Email":
        session = _get_db_session(transaction_service)
        if session:
            try:
                parsed_date = transaction_service.dateTimeUtil.convert_to_sql_datetime(
                    args["date"], bank
                )
                amount_val = round(float(args["amount"]), 2)
                existing = session.query(Transactions).filter(
                    Transactions.user == user_id,
                    Transactions.date == parsed_date,
                    Transactions.amount == amount_val,
                    Transactions.bank == bank,
                    Transactions.source == TransactionTypeEnum.Email.value,
                ).first()
                if existing:
                    logger.info(
                        f"Email dedup: skipping duplicate for {bank} on {parsed_date} "
                        f"amount={amount_val} (existing ref={existing.referenceID}, "
                        f"desc='{existing.details[:50]}')"
                    )
                    return {
                        "result": "duplicate",
                        "inserted": 0,
                        "duplicates": 1,
                        "message": f"Transaction already exists: {existing.details[:80]}",
                    }
            except Exception as e:
                logger.warning(f"Email dedup check failed, proceeding with insert: {e}")

    # ak-8l5: build the row dict + resolve referenceID via the v2
    # helper. Email path never has a statement file_id, so the
    # positional fallback is unreachable here — if the extractor
    # DIDN'T give us a ref, fall back to the legacy content hash so
    # the row still lands. The behavioral email dedup above catches
    # the alert-duplicate class regardless of the ID scheme.
    row = {
        "date": args["date"],
        "description": args["description"],
        "amount": args["amount"],
        "bank_reference_id": args.get("bank_reference_id"),
    }
    if row["bank_reference_id"]:
        # Primary v2 path — same ref → same PK across email alert +
        # statement PDF ingest of the same tx.
        from utils.reference_id import generate_reference_v2
        ref_id = generate_reference_v2(bank, row["bank_reference_id"])
    else:
        # No ref → legacy content hash. Email path doesn't get a
        # positional fallback (no file_id).
        ref_id = generic.generate_reference_id(
            args["date"], args["description"], args["amount"],
        )
    transactions = [{
        "reference": ref_id,
        "date": args["date"],
        "description": args["description"],
        "amount": args["amount"],
        "processed_via": "CLAUDE_CODE",
        "bank_reference_id": row["bank_reference_id"],
    }]

    gmail_id = args.get("gmail_message_id")

    # Build source_emails list for gmail_message_id tracking
    source_emails = [{"message_id": gmail_id}] if gmail_id else None

    errors = transaction_service.insertTransactions(
        transactions, bank, user_id, [],
        source, fileId=None, source_emails=source_emails,
    )
    return {
        "result": "success",
        "inserted": 1 if errors == 0 else 0,
        "duplicates": errors,
    }


def _handle_insert_batch_transactions(args, user_id, transaction_service, reconciliation_service=None):
    """Insert multiple transactions at once (from a statement).

    ak-8l5: dedup engine is (bank, bank_reference_id) when a ref is
    present, positional-fallback otherwise. The extractor was updated
    to emit `bank_reference_id` and `line_position` per row (see the
    ak-8l5 section in mailProcessorTools.py PDF_SYSTEM_PROMPT). Chunk
    re-reads of the same tx must now collapse via the PK conflict path
    while legitimate same-tuple tx stay distinct.

    ak-tik (superseded by ak-8l5) had used a normalized content hash
    that silently merged legitimate same-tuple rows — the reviewer
    MAJOR that got the whole approach withdrawn. Do NOT reintroduce
    that shape here.
    """
    from utils.reference_id import (
        generate_reference_v2,
        generate_reference_v2_fallback,
    )

    generic = GenericUtil()
    bank = args.get("bank", "UNKNOWN")
    source = args.get("source", "Statement")
    file_id = args.get("file_id")
    gmail_id = args.get("gmail_message_id")
    period_start_raw = args.get("period_start")
    period_end_raw = args.get("period_end")

    transactions = []
    # Track how each row was hashed so the batch summary log makes it
    # obvious when a chunk re-read collapses vs. a novel row lands.
    primary_hash_count = 0
    fallback_hash_count = 0
    for idx, txn in enumerate(args.get("transactions", [])):
        # ak-8l5 primary — LLM extracted a per-tx bank ref. Chunk
        # re-reads should emit the SAME ref, so same PK, so the
        # PK-conflict path collapses.
        raw_ref = txn.get("bank_reference_id")
        if raw_ref:
            ref_id = generate_reference_v2(bank, raw_ref)
            primary_hash_count += 1
        else:
            # ak-8l5 fallback — positional. Requires file_id +
            # line_position. If the LLM didn't emit a line_position
            # (older prompts, or the extractor missed it) we STILL
            # need a stable key — the reviewer's zero-loss rule says
            # we can't drop the row. Fall back further to the ak-tik
            # content hash for this row and log loudly so the
            # extractor prompt drift is visible.
            line_pos = txn.get("line_position")
            if line_pos is not None and file_id:
                ref_id = generate_reference_v2_fallback(
                    bank, file_id, line_pos,
                    txn["date"], txn["amount"], txn["description"],
                )
                fallback_hash_count += 1
            else:
                # Extractor didn't give us a positional anchor.
                # Fall back to the legacy content hash so the row
                # still lands (Overseer: no tx loss). Adds `idx` so
                # two ref-less same-content rows in the SAME batch
                # don't accidentally collide — chunk re-reads emit
                # rows in the same order so this stays deterministic
                # per-batch, at the cost of not collapsing an
                # inter-batch re-read of a ref-less row. That's
                # acceptable — ref-less rows are the minority tail;
                # the majority already collapse via v2 primary.
                ref_id = generic.generate_reference_id(
                    txn["date"], f"{txn['description']}|no_pos|{idx}",
                    txn["amount"],
                )
                fallback_hash_count += 1
                logger.warning(
                    f"insert_batch_transactions: row missing "
                    f"bank_reference_id AND line_position; using legacy "
                    f"content hash. bank={bank} file_id={file_id} "
                    f"idx={idx} desc={txn['description'][:50]!r}"
                )
        transactions.append({
            "reference": ref_id,
            "date": txn["date"],
            "description": txn["description"],
            "amount": txn["amount"],
            "processed_via": "CLAUDE_CODE",
            "bank_reference_id": raw_ref,
        })

    if transactions:
        logger.info(
            f"insert_batch_transactions ak-8l5 hash split: "
            f"primary={primary_hash_count} fallback={fallback_hash_count} "
            f"(bank={bank}, file_id={file_id})"
        )

    if not transactions:
        logger.info(f"insert_batch_transactions called with 0 transactions for {bank}")
        return {"result": "no_transactions", "inserted": 0}

    # Parse statement period dates if provided
    period_start = _parse_period_date(period_start_raw) if period_start_raw else None
    period_end = _parse_period_date(period_end_raw) if period_end_raw else None

    # Create/find file details BEFORE reconciliation so we have a file_id to exclude
    # from the overlap guard (prevents chunked PDFs from deleting their own earlier chunks)
    reused_file = False
    if source == "Statement":
        if file_id:
            # Preset file_id (chunked PDF) — check if fileDetails already exists
            existing = _find_existing_file_details(transaction_service, gmail_id) if gmail_id else None
            if existing and existing == file_id:
                reused_file = True
                logger.info(f"Reusing existing fileDetails {file_id} for gmail_id {gmail_id}")
            elif not existing:
                # First chunk — create the fileDetails record
                file_name = f"{bank}_statement.pdf"
                transaction_service.insertFileDetails(
                    file_id, file_name, len(transactions), bank, user_id,
                    "", gmail_id,
                )
            else:
                # Existing file has a different ID — reuse it
                file_id = existing
                reused_file = True
                logger.info(f"Reusing existing fileDetails {file_id} for gmail_id {gmail_id}")
        else:
            # No preset file_id — look up by gmail_id or generate one
            if gmail_id:
                existing_file = _find_existing_file_details(transaction_service, gmail_id)
                if existing_file:
                    file_id = existing_file
                    reused_file = True
                    logger.info(f"Reusing existing fileDetails {file_id} for gmail_id {gmail_id}")

            if file_id is None:
                from utils.DateTimeUtil import DateTimeUtil
                dt_util = DateTimeUtil()
                month = dt_util.getMonthYearRange(
                    transactions[0]["date"], transactions[-1]["date"], bank
                )
                file_id = f"mail_pipeline_{user_id}_{bank}_{month}"
                file_name = f"{bank}_{month}.pdf"
                transaction_service.insertFileDetails(
                    file_id, file_name, len(transactions), bank, user_id,
                    "", gmail_id,
                )

    # If this is a statement with period info, replace email transactions first
    # file_id is now set, so the overlap guard will exclude our own file's transactions
    reconciliation_result = None
    if (source == "Statement" and period_start and period_end
            and reconciliation_service is not None):
        reconciliation_result = reconciliation_service.replace_email_transactions_with_statement(
            user_id=user_id,
            bank=bank,
            period_start=period_start,
            period_end=period_end,
            file_id=file_id,
            gmail_message_id=gmail_id,
            transaction_count=len(transactions),
        )
        email_replaced = reconciliation_result.get('email_transactions_deleted', 0)
        stmt_replaced = reconciliation_result.get('statement_transactions_deleted', 0)
        logger.info(
            f"Reconciliation for {bank} ({period_start} to {period_end}): "
            f"replaced {email_replaced} email txns, {stmt_replaced} overlapping statement txns"
        )

    logger.info(f"insert_batch_transactions: inserting {len(transactions)} txns for {bank} (file_id={file_id})")
    # DEBUG: Log every transaction being inserted so we can trace hallucinations
    for idx, txn in enumerate(transactions):
        logger.info(
            f"  TXN[{idx}] date={txn['date']} amt={txn['amount']} "
            f"desc={txn['description'][:100]}"
        )
    source_emails = [{"message_id": gmail_id}] if gmail_id else None
    errors = transaction_service.insertTransactions(
        transactions, bank, user_id, [],
        source, fileId=file_id, source_emails=source_emails,
    )

    inserted = len(transactions) - errors
    logger.info(f"Batch inserted: {inserted} transactions, {errors} duplicates for {bank}")
    # Clean up file details if nothing was inserted AND we created it (not reused)
    if inserted == 0 and file_id and source == "Statement" and not reused_file:
        transaction_service.deleteFileDetails(file_id)
    elif file_id and source == "Statement" and reused_file and inserted > 0:
        # Chunked PDF: accumulate count from previous chunks
        current_count = _get_statement_count(transaction_service, file_id)
        transaction_service.updateStatementCount(file_id, current_count + inserted)
    elif errors > 0 and file_id and source == "Statement":
        transaction_service.updateStatementCount(file_id, inserted)

    result = {
        "result": "success",
        "total": len(transactions),
        "inserted": inserted,
        "duplicates": errors,
        "file_id": file_id,
    }

    if reconciliation_result:
        result["email_transactions_replaced"] = reconciliation_result.get("email_transactions_deleted", 0)
        result["statement_transactions_replaced"] = reconciliation_result.get("statement_transactions_deleted", 0)
        result["statement_period_recorded"] = reconciliation_result.get("statement_period_recorded", False)

    return result


def _parse_period_date(date_str):
    """Parse a date string in DD/MM/YYYY, DD-MM-YYYY, or YYYY-MM-DD format."""
    from datetime import datetime
    if not date_str:
        return None
    for fmt in ("%d/%m/%Y", "%d-%m-%Y", "%Y-%m-%d"):
        try:
            return datetime.strptime(date_str, fmt).date()
        except ValueError:
            continue
    logger.warning(f"Could not parse period date: {date_str}")
    return None


def _handle_insert_epf_deposit(args, user_id, investment_service):
    """Record EPF contribution data from email (no DB insert)."""
    return {
        "status": "recorded",
        "service_type": "EPF",
        "date": args["date"],
        "description": args.get("description", "EPF Contribution"),
        "employee_amount": args.get("employee_amount"),
        "employer_amount": args.get("employer_amount"),
        "message": "EPF data recorded for email linkage (not inserted into portfolio)"
    }


def _handle_insert_ppf_deposit(args, user_id, investment_service):
    """Record PPF deposit data from email (no DB insert)."""
    return {
        "status": "recorded",
        "service_type": "PPF",
        "date": args["date"],
        "description": args.get("description", "PPF Deposit"),
        "amount": args.get("amount"),
        "message": "PPF data recorded for email linkage (not inserted into portfolio)"
    }


def _handle_insert_gold_purchase(args, user_id, investment_service):
    """Record gold purchase data from email (no DB insert)."""
    return {
        "status": "recorded",
        "service_type": "Gold",
        "date": args["date"],
        "description": args.get("description", "Gold Purchase"),
        "amount": args.get("amount"),
        "quantity": args.get("quantity"),
        "gold_type": args.get("gold_type"),
        "message": "Gold data recorded for email linkage (not inserted into portfolio)"
    }


def _handle_insert_investment(args, user_id, investment_service):
    """Record stock/MF/NPS purchase data from email (no DB insert)."""
    return {
        "status": "recorded",
        "service_type": args.get("service_type"),
        "scheme_code": args.get("scheme_code"),
        "date": args.get("date"),
        "quantity": args.get("quantity"),
        "amount": args.get("amount"),
        "message": "Investment data recorded for email linkage (not inserted into portfolio)"
    }


def _handle_mark_invoice_paid(args, invoice_service):
    """Mark a freelance invoice as paid."""
    invoice_number = args["invoice_number"]
    update_data = {"status": "paid"}
    if args.get("payment_date") or args.get("amount_received") or args.get("payment_method"):
        update_data["payment"] = {}
        if args.get("payment_date"):
            update_data["payment"]["payment_date"] = args["payment_date"]
        if args.get("amount_received"):
            update_data["payment"]["amount_received"] = args["amount_received"]
        if args.get("payment_method"):
            update_data["payment"]["payment_method"] = args["payment_method"]

    return invoice_service.update_invoice(invoice_number, update_data)


def _parse_page_range(page_range_str, total_pages):
    """Parse a page_range string like '1-3', '4-6', or 'all' into a list of 1-indexed page numbers."""
    if page_range_str.strip().lower() == "all":
        return list(range(1, total_pages + 1))
    parts = page_range_str.split("-")
    if len(parts) == 2:
        start = max(1, int(parts[0].strip()))
        end = min(total_pages, int(parts[1].strip()))
        return list(range(start, end + 1))
    # Single page number
    num = int(page_range_str.strip())
    return [num] if 1 <= num <= total_pages else []


# Max accumulated base64 bytes before stopping — stay under 1MB SDK buffer.
# SDK's JSON message buffer is 1,048,576 bytes; JSON envelope adds ~50-100KB.
# With 1-page-per-chunk at fixed 100 DPI, a single page is typically 200-500KB.
_MAX_CONTENT_BYTES = 900_000
_RENDER_DPI = 100


def _handle_get_pdf_pages(args):
    """
    Render PDF pages to images using PyMuPDF and return as ImageContent.
    Supports fetching multiple pages per call via page_range (e.g. '1-3', 'all').
    Caps accumulated image size at _MAX_CONTENT_BYTES to stay under the 1MB SDK buffer.
    Falls back to single page_number for backward compatibility.
    """
    pdf_path = args["pdf_path"]
    password = args.get("password")
    page_range = args.get("page_range")
    page_number = args.get("page_number", 1)  # 1-indexed, fallback

    if not os.path.exists(pdf_path):
        return {"error": f"PDF file not found: {pdf_path}"}

    try:
        doc = fitz.open(pdf_path)

        if doc.needs_pass:
            if password:
                if not doc.authenticate(password):
                    doc.close()
                    return {"error": "Invalid PDF password"}
            else:
                doc.close()
                return {"error": "PDF is password-protected but no password provided"}

        total_pages = doc.page_count
        if total_pages == 0:
            doc.close()
            return {"error": "PDF has no pages"}

        # Determine which pages to render
        if page_range:
            requested_pages = _parse_page_range(page_range, total_pages)
        else:
            requested_pages = [page_number]

        if not requested_pages:
            doc.close()
            return {"error": f"No valid pages in range (total_pages={total_pages})"}

        # Validate bounds
        for pn in requested_pages:
            if pn < 1 or pn > total_pages:
                doc.close()
                return {"error": f"Page {pn} out of range (1-{total_pages})"}

        # Render pages, stopping if accumulated size exceeds buffer limit
        content = []
        accumulated_bytes = 0
        pages_rendered = []

        for pn in requested_pages:
            page = doc[pn - 1]

            # Fixed DPI rendering — never degrade quality
            pix = page.get_pixmap(dpi=_RENDER_DPI)
            img_data = pix.tobytes("png")
            b64_data = base64.standard_b64encode(img_data).decode("ascii")

            # Check if adding this page would exceed the buffer
            if accumulated_bytes > 0 and accumulated_bytes + len(b64_data) > _MAX_CONTENT_BYTES:
                logger.info(
                    f"Stopping at page {pn} — accumulated {accumulated_bytes} bytes "
                    f"(next page would exceed {_MAX_CONTENT_BYTES} limit)"
                )
                break

            content.append({
                "type": "image",
                "data": b64_data,
                "mimeType": "image/png",
            })
            accumulated_bytes += len(b64_data)
            pages_rendered.append(pn)

        doc.close()

        first_page = pages_rendered[0] if pages_rendered else page_number
        last_page = pages_rendered[-1] if pages_rendered else page_number

        return {
            "_image_content": True,
            "content": content,
            "page_number": first_page,
            "last_page_rendered": last_page,
            "pages_rendered": pages_rendered,
            "total_pages": total_pages,
        }

    except Exception as e:
        logger.error(f"PDF rendering error: {e}", exc_info=True)
        return {"error": f"Failed to render PDF: {str(e)}"}


def _handle_save_attachment(args, user_id):
    """Save a PDF attachment to local storage."""
    source_path = args["source_path"]
    category = args["category"]
    filename = args["filename"]

    if not os.path.exists(source_path):
        return {"error": f"Source file not found: {source_path}"}

    save_dir = os.path.join(os.getcwd(), "attachments", user_id, category)
    os.makedirs(save_dir, exist_ok=True)

    # Sanitize filename — strip path traversal and separators
    safe_filename = filename.replace("..", "").replace("/", "_").replace("\\", "_")
    dest_path = os.path.join(save_dir, safe_filename)

    shutil.copy2(source_path, dest_path)
    logger.info(f"Saved attachment to {dest_path}")

    return {"result": "success", "saved_path": dest_path}


def _handle_report_result(args, user_id, transaction_service):
    """Log the processing result for a single email and persist to processedEmails table."""
    from datetime import datetime
    from models.processedEmails import ProcessedEmails
    from sqlalchemy.exc import IntegrityError

    status = args.get("status", "unknown")
    category = args.get("category", "unknown")
    items = args.get("items_extracted", 0)
    message = args.get("message", "")
    gmail_id = args.get("gmail_message_id")

    logger.info(
        f"Mail processing result: status={status}, category={category}, "
        f"items={items}, gmail_id={gmail_id[:20] if gmail_id else 'N/A'}"
        f"{', msg=' + message if message else ''}"
    )

    # Persist to processedEmails table
    if not gmail_id or not user_id:
        return {"result": "logged", "status": status, "persisted": False}

    # Map tool status to DB status
    status_map = {"success": "processed", "skipped": "skipped", "error": "failed"}
    db_status = status_map.get(status, "processed")

    # Parse email_date if provided
    email_date = None
    raw_date = args.get("email_date")
    if raw_date:
        for fmt in ("%Y-%m-%d", "%d/%m/%Y", "%d-%m-%Y", "%Y-%m-%dT%H:%M:%S"):
            try:
                email_date = datetime.strptime(raw_date, fmt)
                break
            except ValueError:
                continue

    session = _get_db_session(transaction_service)
    if not session:
        return {"result": "logged", "status": status, "persisted": False}

    try:
        row = ProcessedEmails(
            gmail_id=gmail_id,
            user_id=user_id,
            sender=args.get("sender"),
            subject=args.get("subject"),
            email_date=email_date,
            category=category,
            processing_type=args.get("processing_type"),
            status=db_status,
            items_extracted=items or 0,
            extraction_summary=args.get("extraction_summary"),
            error_message=message if db_status == "failed" else None,
        )
        session.add(row)
        session.commit()

        # Auto-link to freelance customer if sender matches
        try:
            from services.customerEmailService import CustomerEmailService
            ce_service = CustomerEmailService()
            ce_service.auto_link_email(row, user_id)
        except Exception:
            pass  # Non-critical, don't break email processing

        return {"result": "logged", "status": status, "persisted": True}
    except IntegrityError:
        # Duplicate gmail_id+user_id — update the existing row
        session.rollback()
        try:
            existing = session.query(ProcessedEmails).filter_by(
                gmail_id=gmail_id, user_id=user_id
            ).first()
            if existing:
                existing.category = category
                existing.status = db_status
                existing.items_extracted = items or 0
                existing.extraction_summary = args.get("extraction_summary")
                existing.processing_type = args.get("processing_type")
                if db_status == "failed":
                    existing.error_message = message
                session.commit()

                # Auto-link on update too
                if existing:
                    try:
                        from services.customerEmailService import CustomerEmailService
                        ce_service = CustomerEmailService()
                        ce_service.auto_link_email(existing, user_id)
                    except Exception:
                        pass

            return {"result": "logged", "status": status, "persisted": True, "updated": True}
        except Exception as e:
            session.rollback()
            logger.error(f"Failed to update existing processedEmails row: {e}")
            return {"result": "logged", "status": status, "persisted": False}
    except Exception as e:
        session.rollback()
        logger.error(f"Failed to persist processedEmails row: {e}")
        return {"result": "logged", "status": status, "persisted": False}
