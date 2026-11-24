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
from utils.mail_row_validation import (
    MAX_BATCH_ROWS, check_amount, check_and_normalise_date, check_date,
    first_error,
)
from utils.mail_tool_paths import (
    ToolPathRejected, model_path_differs, resolve_pinned_pdf,
    safe_attachment_dest,
)

logger = Logger(__name__).get_logger()


def _record_hash_tier(dual_path_seen, txn, *, primary):
    """ak-8l5 M1 (review minor): record which hash tier saw this row's
    (normalized) content. Callers pass one of primary=True or primary=
    False per row. When both flags fire for the same key within a
    batch, the batch-summary log emits a M1 dual-tier warning.

    Normalization matches the fallback-hash contract so a chunk-boundary
    variant (case / whitespace / trailing punct) doesn't hide a real
    dual-tier hit.
    """
    from utils.reference_id import normalize_description_for_backstop
    try:
        amt = round(float(txn.get("amount", 0)), 2)
    except (TypeError, ValueError):
        amt = None
    key = (
        str(txn.get("date", "")),
        amt,
        normalize_description_for_backstop(txn.get("description", "")),
    )
    slot = dual_path_seen.setdefault(key, [False, False])
    if primary:
        slot[0] = True
    else:
        slot[1] = True


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


def _statement_file_ids(session, user_id, file_id, gmail_id):
    """The fileIDs of one statement: the resolved file_id plus every
    fileDetails row recorded for its gmail id (exact join) plus, review
    minor, the fileIDs on transactions with the anchored ingest shape
    mail_pipeline_<user>_<BANK>_<gmail_id> (only the bank segment is a
    wildcard; _ / % escaped), the same linkage the backfill uses."""
    from models.fileDetails import FileDetails
    from models.transactions import Transactions
    ids = {file_id} if file_id else set()
    if gmail_id:
        ids |= {r.fileID for r in session.query(FileDetails.fileID).filter(
            FileDetails.user == user_id, FileDetails.gmail_message_id == gmail_id).all()}

        def esc(v):
            return str(v).replace("\\", "\\\\").replace("%", "\\%").replace("_", "\\_")
        pattern = f"mail\\_pipeline\\_{esc(user_id)}\\_%\\_{esc(gmail_id)}"
        ids |= {r.fileID for r in session.query(Transactions.fileID).filter(
            Transactions.user == user_id,
            Transactions.fileID.like(pattern, escape="\\")).distinct().all()}
    return ids


def _drop_previous_run_copies(transaction_service, user_id, bank, file_id, gmail_id, transactions):
    """ak-zejx: drop incoming statement rows that an EARLIER ingest of the
    same statement already wrote, whatever bank id either run used.

    The referenceID hash folds the bank in, so re-ingesting a statement
    under a corrected bank (BOI -> BOB) mints new referenceIDs and the PK
    dedup misses: the ledger would double-count. Rule, per statement
    (same gmail id / file):
      * rows already written in THIS run (statement_ingest_outcome.
        STATEMENT_RUN_REFS) are not "previous": chunks of one run never
        dedup against each other, so legit identical rows on different
        pages stay distinct;
      * the previous-run rows form a MULTISET of (date, amount,
        normalised description); an incoming row whose referenceID is
        not already present consumes a matching slot and is skipped (a
        previous copy). Two identical previous rows absorb exactly two
        incoming ones, so legit same-day duplicates are preserved. A
        consumed slot stays consumed for the whole run (recorded in the
        run state), so a later chunk can't match the same earlier row;
      * an incoming row whose referenceID already exists keeps the
        normal PK path (and uses up its own slot).
    Only active inside a statement ingest (the ContextVar is set).
    Returns (rows_to_insert, skipped_count).
    """
    from collections import defaultdict
    from decimal import Decimal as _D
    from models.transactions import Transactions
    from utils.reference_id import normalize_description_for_backstop
    from utils.statement_ingest_outcome import (
        STATEMENT_RUN_REFS, consumed_prior_refs, mark_prior_consumed)

    run_refs = STATEMENT_RUN_REFS.get()
    session = _get_db_session(transaction_service)
    if run_refs is None or session is None or not transactions:
        return transactions, 0
    file_ids = _statement_file_ids(session, user_id, file_id, gmail_id)
    if not file_ids:
        return transactions, 0
    existing = session.query(Transactions).filter(
        Transactions.user == user_id, Transactions.fileID.in_(list(file_ids))).all()

    def key(date_value, amount, description):
        try:
            if isinstance(date_value, str):
                date_value = transaction_service.dateTimeUtil.convert_to_sql_datetime(date_value, bank)
            day = str(date_value)[:10]
            amt = _D(str(amount)).quantize(_D("0.01"))
        except Exception:
            return None
        return day, amt, normalize_description_for_backstop(description or "")

    existing_refs = {r.referenceID for r in existing}
    # Review MAJOR (2nd round): an earlier row a skipped incoming row
    # consumed stays consumed for the rest of the run (later chunks / calls
    # rebuild the slots from the DB, so it must be remembered in run state),
    # else it would absorb a genuine second identical row in a later chunk.
    consumed = consumed_prior_refs(run_refs)
    available = defaultdict(list)          # key -> earlier refs not yet consumed
    key_of_ref = {}
    for r in existing:
        if r.referenceID in run_refs or r.referenceID in consumed:
            continue
        k = key(r.date, r.amount, r.details)
        if k is not None:
            available[k].append(r.referenceID)
            key_of_ref[r.referenceID] = k
    keep, skipped, newly_consumed = [], 0, set()
    for txn in transactions:
        k = key(txn.get("date"), txn.get("amount"), txn.get("description"))
        ref = txn.get("reference")
        if ref in existing_refs:
            own = key_of_ref.get(ref)
            if own is not None and ref in available[own]:
                available[own].remove(ref)    # the PK path handles it; its own slot
                newly_consumed.add(ref)
            keep.append(txn)
        elif k is not None and available.get(k):
            newly_consumed.add(available[k].pop())
            skipped += 1
        else:
            keep.append(txn)
    run_refs.update(t.get("reference") for t in keep if t.get("reference"))
    mark_prior_consumed(run_refs, newly_consumed)
    if skipped:
        logger.info(
            f"[ak-zejx] {skipped} statement row(s) already ingested by an earlier run "
            f"(bank-agnostic match); skipped as duplicates"
        )
    return keep, skipped


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
                      invoice_service=None, reconciliation_service=None,
                      pinned_pdf_path=None):
    """
    Execute a mail processor tool call by routing to the appropriate service method.
    Returns a dict with tool result, or for get_pdf_pages returns image content.

    ak-jmbw: ``pinned_pdf_path`` is the run's own PDF, supplied by the
    SERVICE. get_pdf_pages / save_attachment act only on it (see
    utils.mail_tool_paths); model-supplied paths are ignored.
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
            return _handle_get_pdf_pages(tool_input, pinned_pdf_path, user_id)
        elif tool_name == "save_attachment":
            result = _handle_save_attachment(tool_input, user_id, pinned_pdf_path)
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

def _invalid_args(tool, reason, **extra):
    """ak-n0cg: uniform fail-fast result. Logs the tool + reason only
    (never the extracted values, which are personal financial data)."""
    logger.warning(f"[ak-n0cg] {tool} rejected: {reason}")
    out = {"result": "invalid_args", "error": reason, "inserted": 0}
    out.update(extra)
    return out


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
    # ak-n0cg / ak-v3mf m3: value sanity before any DB work. A Rs 0.00
    # line is legitimate; accepted bank date spellings are normalised to
    # a form the insert path parses.
    norm_date, date_err = check_and_normalise_date(args.get("date"))
    err = first_error(check_amount(args.get("amount"), allow_zero=True),
                      date_err)
    if err:
        return _invalid_args("insert_transaction", err)
    args["date"] = norm_date
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
        # statement PDF ingest of the same tx. ak-8l5 M3: amount is
        # folded in so shared-ref-across-amounts stays distinct.
        from utils.reference_id import generate_reference_v2
        ref_id = generate_reference_v2(
            bank, row["bank_reference_id"], args["amount"],
        )
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
        normalize_description_for_backstop,
    )

    generic = GenericUtil()
    bank = args.get("bank", "UNKNOWN")
    source = args.get("source", "Statement")
    file_id = args.get("file_id")
    gmail_id = args.get("gmail_message_id")
    period_start_raw = args.get("period_start")
    period_end_raw = args.get("period_end")

    # ak-csng Phase 1 (2026-11-23) — arg-validation fail-fast.
    # This tool receives args from an LLM that may have been
    # prompt-injected via an attacker email. Reject obviously-
    # malformed calls BEFORE any DB write. Fields that can be
    # silently omitted ("UNKNOWN" bank, missing gmail_message_id
    # on a statement source) were previously accepted — a prompt-
    # injection could craft a batch insert with no source/bank
    # binding and poison the ledger. Fail-closed on:
    #   * bank missing / equals "UNKNOWN" literal (sentinel for
    #     "LLM didn't supply") — a legit statement tool-call
    #     always carries the bank identifier.
    #   * source == "Statement" but no gmail_message_id — the
    #     dedup path + fileDetails creation both key on gmail
    #     id; missing it means we can't correlate the insert
    #     with the mail that produced it.
    if not bank or bank == "UNKNOWN":
        logger.warning(
            "[ak-csng] insert_batch_transactions rejected: bank "
            "missing or 'UNKNOWN' sentinel (prompt-injection "
            "class — LLM tool call must carry a bank identifier). "
            "user_id=%s source=%s",
            user_id, source,
        )
        return {
            "result": "invalid_args",
            "error": "bank is required (ak-csng validation)",
            "inserted": 0,
        }
    if source == "Statement" and not gmail_id and not file_id:
        logger.warning(
            "[ak-csng] insert_batch_transactions rejected: "
            "Statement source without gmail_message_id OR "
            "file_id (prompt-injection class — can't correlate "
            "insert with source mail). user_id=%s bank=%s",
            user_id, bank,
        )
        return {
            "result": "invalid_args",
            "error": (
                "Statement source requires gmail_message_id "
                "or file_id (ak-csng validation)"
            ),
            "inserted": 0,
        }

    # ak-n0cg / ak-v3mf m3: cap + per-row sanity BEFORE any write. The
    # whole call fails (the agent can retry with corrected rows):
    # silently dropping some statement rows would itself be an
    # accounting error.
    # An EMPTY list keeps its pre-n0cg meaning (a page with no
    # transactions → "no_transactions"), so it is not a rejection.
    rows_in = args.get("transactions")
    if rows_in is None:
        rows_in = []
    if not isinstance(rows_in, list):
        return _invalid_args("insert_batch_transactions",
                             "transactions must be a list")
    if len(rows_in) > MAX_BATCH_ROWS:
        return _invalid_args(
            "insert_batch_transactions",
            f"too many rows in one call (max {MAX_BATCH_ROWS})")
    bad_rows = []
    norm_rows = []
    for idx, row in enumerate(rows_in, start=1):
        if not isinstance(row, dict):
            bad_rows.append({"row": idx, "error": "row is not an object"})
            continue
        # Rs 0.00 statement lines are real (allow_zero); dates in an
        # accepted bank spelling are normalised, not rejected.
        norm_date, date_err = check_and_normalise_date(row.get("date"))
        err = first_error(check_amount(row.get("amount"), allow_zero=True),
                          date_err)
        if err:
            bad_rows.append({"row": idx, "error": err})
        else:
            norm_rows.append(dict(row, date=norm_date))
    if bad_rows:
        return _invalid_args(
            "insert_batch_transactions",
            f"{len(bad_rows)} row(s) failed validation",
            bad_rows=bad_rows[:10],
            bad_row_count=len(bad_rows),
        )
    args["transactions"] = norm_rows

    transactions = []
    # Track how each row was hashed so the batch summary log makes it
    # obvious when a chunk re-read collapses vs. a novel row lands.
    primary_hash_count = 0
    fallback_hash_count = 0
    # ak-8l5 M1 (review minor): dual-path visibility. If the SAME
    # normalized (date, amount, description) tuple appears in this
    # batch once via the primary path AND once via the fallback path,
    # that's an extraction pathology worth warning about (chunk
    # boundary saw a ref on one side and not the other, or the LLM
    # emitted a ref for the summary line but not the detail line).
    # We record { normkey: (via_primary, via_fallback) } per batch.
    _dual_path_seen: dict[tuple, list[bool]] = {}
    for idx, txn in enumerate(args.get("transactions", [])):
        # ak-8l5 primary — LLM extracted a per-tx bank ref. Chunk
        # re-reads should emit the SAME ref, so same PK, so the
        # PK-conflict path collapses.
        raw_ref = txn.get("bank_reference_id")
        if raw_ref:
            # ak-8l5 M3: pass amount into the primary hash.
            ref_id = generate_reference_v2(bank, raw_ref, txn["amount"])
            primary_hash_count += 1
            _record_hash_tier(_dual_path_seen, txn, primary=True)
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
                _record_hash_tier(_dual_path_seen, txn, primary=False)
            else:
                # ak-tik (2026-11-23): the extractor didn't give us
                # a positional anchor. This is the LAST-RESORT path,
                # triggered ONLY when the LLM missed both
                # bank_reference_id AND line_position (prompt drift
                # or chunk-boundary partial read).
                #
                # Pre-ak-tik the hash included batch-local `idx`,
                # which made chunk re-reads of the SAME ref-less row
                # produce DIFFERENT hashes → different refs → no PK
                # collision → backstop never fires → both rows land
                # as duplicates on the user's Transactions page.
                # This WAS the "chunk-overlap re-extraction bug
                # bypasses dedup" shape surfaced by the Overseer.
                #
                # Fix: drop `idx` from the hash. Use (date, amount,
                # bank, file_id, normalized_desc) so two reads of
                # the SAME row in different chunks produce the SAME
                # ref → PK collision → backstop sees content-equal
                # (via the SAME normalize_description_for_backstop
                # the hash uses) → silent-drop path (correct
                # chunk-overlap dedup).
                #
                # Accepted tradeoff: two LEGITIMATELY-DIFFERENT
                # ref-less rows with the same (date, amount,
                # normalized-desc) now collapse via the backstop's
                # content-equal path. That's the ak-tik-era
                # regression the ak-8l5 refactor ADDRESSED by
                # requiring line_position. The regression only
                # bites in a double-degradation state (missing
                # bank_reference_id AND missing line_position) that
                # SHOULD be rare post-ak-8l5; the WARN below is
                # the operator's trigger to fix the extractor
                # prompt.
                #
                # Net effect: user-visible duplicate-row bug
                # collapses correctly; rare legitimate-duplicate
                # case silently collapses, logged.
                norm_desc = normalize_description_for_backstop(
                    txn["description"],
                )
                # file_id defaults to a scope sentinel so non-
                # statement callers (email-alert path, if ever
                # routed here) still produce a stable hash within
                # their own scope.
                scope = file_id or "no_file"
                ref_id = generic.generate_reference_id(
                    txn["date"],
                    f"{bank}|{scope}|{norm_desc}|no_pos",
                    txn["amount"],
                )
                fallback_hash_count += 1
                _record_hash_tier(_dual_path_seen, txn, primary=False)
                logger.warning(
                    f"insert_batch_transactions ak-tik: row missing "
                    f"bank_reference_id AND line_position; using "
                    f"file-scoped content hash (NOT idx-scoped — see "
                    f"docstring). bank={bank} file_id={file_id} "
                    f"idx={idx} desc={txn['description'][:50]!r} — "
                    f"fix extractor prompt to emit line_position for "
                    f"ref-less rows so this path stops firing."
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
        # ak-8l5 M1: emit the dual-path warning if the same normalized
        # tuple crossed both tiers in this batch. Non-blocking — this
        # is monitoring visibility; the primary/fallback split alone
        # doesn't imply loss, but persistent dual-path signals prompt
        # drift and a follow-up sweep should look.
        _dual = [k for k, v in _dual_path_seen.items() if v[0] and v[1]]
        if _dual:
            logger.warning(
                f"insert_batch_transactions ak-8l5 M1 dual-tier: "
                f"{len(_dual)} tuple(s) inserted via BOTH primary and "
                f"fallback in the same batch — possible extractor "
                f"pathology. bank={bank} file_id={file_id} "
                f"first_sample={_dual[0]!r}"
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

    # ak-zejx: bank-agnostic re-ingest dedup (statement ingest only).
    prior_copies = 0
    if source == "Statement":
        transactions, prior_copies = _drop_previous_run_copies(
            transaction_service, user_id, bank, file_id, gmail_id, transactions)

    logger.info(f"insert_batch_transactions: inserting {len(transactions)} txns for {bank} (file_id={file_id})")
    # DEBUG: Log every transaction being inserted so we can trace hallucinations
    for idx, txn in enumerate(transactions):
        logger.info(
            f"  TXN[{idx}] date={txn['date']} amt={txn['amount']} "
            f"desc={txn['description'][:100]}"
        )
    source_emails = [{"message_id": gmail_id}] if gmail_id else None
    try:
        errors = transaction_service.insertTransactions(
            transactions, bank, user_id, [],
            source, fileId=file_id, source_emails=source_emails,
        )
    except Exception:
        # ak-iu8i: nothing was written. Drop the fileDetails row this
        # call created (same cleanup as the 0-inserted case below) and
        # let the error propagate: the tool reports it, the chunk fails
        # and the email is retried, never "inserted N".
        if file_id and source == "Statement" and not reused_file:
            try:
                transaction_service.deleteFileDetails(file_id)
            except Exception as cleanup_exc:
                logger.warning(
                    f"[ak-iu8i] fileDetails cleanup failed after a failed insert "
                    f"({type(cleanup_exc).__name__}); an orphan fileDetails row may remain"
                )
        raise

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

    # ak-tik Overseer Option 1 mitigation (2026-11-23): surface the
    # count of rows that fell into the fallback hash path so the FE
    # ingest-complete screen can show a banner when the dedup was
    # degraded. The hash-fix itself (c500560) collapses chunk re-reads
    # correctly but ACCEPTS a rare regression where two legitimately-
    # different ref-less rows with identical (date, amount, normalized-
    # desc) collapse via the backstop. This count is the operator /
    # user signal that the accepted-tradeoff path fired this batch.
    #
    # Contract for FE / agent-tool consumers:
    #   * degraded_dedup_rows  — int ≥ 0. Count of rows this batch
    #     that hashed via the fallback tier (no bank_reference_id,
    #     OR no line_position + no file_id → legacy content hash).
    #     Any value > 0 means at least one row was subject to the
    #     accepted-tradeoff path; users can review Transactions for
    #     that batch's date range.
    #   * dual_tier_tuples    — int ≥ 0. Count of normalized
    #     (date, amount, desc) tuples that crossed BOTH the primary
    #     AND fallback tiers in the same batch (ak-8l5 M1 signal;
    #     extractor-prompt-drift canary). Any value > 0 warrants
    #     operator attention.
    #
    # FE banner rule: if degraded_dedup_rows > 0 → show a non-blocking
    # yellow banner naming the file + count ("N transactions in this
    # file were deduped without a bank reference — review on
    # Transactions"). If dual_tier_tuples > 0 → additional WARN
    # already fires in the batch log (line 425-430); banner can
    # optionally link to a "why" explainer.
    _dual = [k for k, v in _dual_path_seen.items() if v[0] and v[1]]
    result = {
        "result": "success",
        "total": len(transactions),
        "inserted": inserted,
        "duplicates": errors + prior_copies,
        "file_id": file_id,
        # ak-tik mitigation surface — see comment above.
        "degraded_dedup_rows": fallback_hash_count,
        "dual_tier_tuples": len(_dual),
    }

    if reconciliation_result:
        result["email_transactions_replaced"] = reconciliation_result.get("email_transactions_deleted", 0)
        result["statement_transactions_replaced"] = reconciliation_result.get("statement_transactions_deleted", 0)
        result["statement_period_recorded"] = reconciliation_result.get("statement_period_recorded", False)

    # ak-qic6 (2026-11-23): wire the ak-qpkm statement-reconciliation
    # alarm into the post-ingest path. Phase 2b alarm infra landed
    # in ak-qpkm (f8072cb) but stayed INERT — audit_statement_file
    # was never invoked on real imports. This wiring activates it.
    #
    # Design:
    #   * advisory, NOT gating — wrap in try/except so an alarm
    #     failure (unlikely; it's a pure read-aggregate) can never
    #     fail a successful ingest.
    #   * expected_row_count = `inserted` — the count of tx rows
    #     this batch landed. audit_statement_file() then counts
    #     the TOTAL for (file_id, user_id, source='Statement') and
    #     flags drift. For single-chunk statements this is the
    #     exact expected number; for multi-chunk statements, later
    #     batches extend the total and the audit's expected is
    #     the CURRENT-BATCH contribution — which is what we want
    #     the per-batch alarm to pin.
    #   * severity mapping:
    #       - clean          → no log
    #       - info           → logger.warning (ROW_COUNT_DRIFT /
    #                          SUM_DRIFT / NEW_IMPORT not alarming)
    #       - critical       → logger.error (REGRESSED / empty
    #                          import when expected non-trivial)
    #   * audit summary exposed on the result dict so FE
    #     companions (ak-dngn-sibling) can surface a
    #     "RECONCILIATION DRIFT: review" badge alongside the
    #     ak-tik degraded_dedup_rows banner.
    #
    # §23 #4 PII guard: audit result carries only (file_id, bank,
    # user_id, counts, sums, flags, severity) — NEVER tx
    # descriptions / amounts-per-row. Confirmed by the ak-qpkm
    # test `test_result_dict_excludes_tx_descriptions`.
    #
    # Only runs on successful Statement imports — the common case.
    # Email-alert ingests don't carry file_id → audit is skipped
    # (the function would raise ValueError on empty file_id; the
    # wiring pre-checks instead of catching).
    if source == "Statement" and file_id and user_id and inserted > 0:
        try:
            from services.statement_reconciliation_alarm import (
                FLAG_REGRESSED,
                audit_statement_file,
            )
            session = _get_db_session(transaction_service)
            audit_result = audit_statement_file(
                session=session,
                file_id=file_id,
                bank=bank,
                user_id=user_id,
                expected_row_count=inserted,
            )
            # Expose on the result dict so FE can surface an
            # import-quality banner. Keep the field name explicit
            # so a future refactor doesn't quietly drop it.
            result["reconciliation_audit"] = {
                "severity": audit_result.get("severity"),
                "flags": audit_result.get("flags", []),
                "actual_row_count": audit_result.get("actual_row_count"),
                "expected_row_count": audit_result.get("expected_row_count"),
                "row_count_delta": audit_result.get("row_count_delta"),
            }
            severity = audit_result.get("severity")
            if severity == "critical":
                logger.error(
                    f"statementReconciliationAlarm REGRESSED: "
                    f"file={file_id} bank={bank} user={user_id} "
                    f"flags={audit_result.get('flags')} "
                    f"actual_rows={audit_result.get('actual_row_count')} "
                    f"expected_rows={audit_result.get('expected_row_count')} "
                    f"delta={audit_result.get('row_count_delta')}"
                )
            elif severity == "info":
                logger.warning(
                    f"statementReconciliationAlarm drift: "
                    f"file={file_id} bank={bank} user={user_id} "
                    f"flags={audit_result.get('flags')} "
                    f"actual_rows={audit_result.get('actual_row_count')} "
                    f"expected_rows={audit_result.get('expected_row_count')} "
                    f"delta={audit_result.get('row_count_delta')}"
                )
            # severity == "clean" → no log (quiet-success path)
        except Exception as exc:
            # Alarm is advisory — must not block a successful ingest.
            # Log class-only (no stack, no row content) so §23 #4 PII
            # guard holds even when the alarm itself malfunctions.
            logger.warning(
                f"statementReconciliationAlarm wiring failed "
                f"(advisory; ingest unaffected): "
                f"{type(exc).__name__} file={file_id} bank={bank}"
            )

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
    err = first_error(
        check_date(args.get("date")),
        check_amount(args.get("employee_amount"), field="employee_amount",
                     allow_zero=True, allow_negative=False),
        check_amount(args.get("employer_amount"), field="employer_amount",
                     allow_zero=True, allow_negative=False),
    )
    if not err and not (float(args["employee_amount"]) or float(args["employer_amount"])):
        err = "employee_amount and employer_amount are both zero"
    if err:
        return _invalid_args("insert_epf_deposit", err)
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
    err = first_error(check_date(args.get("date")),
                      check_amount(args.get("amount"), allow_negative=False))
    if err:
        return _invalid_args("insert_ppf_deposit", err)
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
    err = first_error(
        check_date(args.get("date")),
        check_amount(args.get("amount"), allow_negative=False),
        check_amount(args.get("quantity"), field="quantity", allow_negative=False),
    )
    if err:
        return _invalid_args("insert_gold_purchase", err)
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
    err = first_error(
        check_date(args.get("date")),
        check_amount(args.get("amount")),
        (check_amount(args.get("quantity"), field="quantity", allow_zero=True,
                      allow_negative=False)
         if args.get("quantity") is not None else None),
    )
    if err:
        return _invalid_args("insert_investment", err)
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
    """ak-lvu A.7 + A.8 + MP-3: FILE A CLAIM, do NOT flip status.

    Pre-ak-lvu semantics: this tool set `status="paid"` and wrote a
    payment row on email say-so alone (super-review MP-3 CRITICAL). Also
    the payment payload used snake_case field names but `_replace_payment`
    read camelCase, causing a silent KeyError that partial-masked the
    payment while leaving the status flip in place (AC-7).

    New semantics (ak-lvu A.8):
      * Do NOT touch invoice.status.
      * Do NOT write an InvoicePayment row.
      * DO write a `PendingPaymentClaim` row with the LLM's extracted
        amount / currency / method / date.
      * Return {status: "pending", claim_id: <uuid>} so the LLM surfaces
        an accurate "payment claim recorded, pending reconciliation"
        response to Overseer rather than falsely reporting "invoice
        marked paid".

    Confirmation flow (separate follow-up beads):
      * FE bead — claims-review UI + per-claim confirm button
      * BE bead — bank-transaction ingest hook to auto-match on
        (amount + N-day window)

    Only after confirmation does the actual payment row + status flip
    happen — closes MP-3.
    """
    from flask import g as _flask_g
    from services.Base_Service import BaseService
    from models.freelance_management import PendingPaymentClaim, Invoice

    invoice_number = args["invoice_number"]
    # ak-n0cg: provenance. The SERVICE attaches the batch email this
    # claim came from (verified against its own batch); the model can't
    # supply it (any model-sent value is stripped upstream). No source
    # email → refuse: an unattributable claim could come from anywhere.
    source_email = args.get("_source_email")
    if not isinstance(source_email, dict) or not source_email.get("gmail_id"):
        logger.warning(
            "[ak-n0cg] mark_invoice_paid refused: no server-verified "
            "source email for this claim"
        )
        return {
            "status": "error",
            "error": "mark_invoice_paid refused: the claim must come from an email in this batch",
        }
    user_id = _flask_g.get('firebase_id') if _flask_g else None
    if not user_id:
        # Mail executor is always called inside a request context via
        # agentEP so g.firebase_id should be set. If not, surface loudly.
        raise RuntimeError(
            "mark_invoice_paid: no firebase_id on request context"
        )

    # Resolve invoice → id. Use invoice_service.db.session so the same
    # SQLAlchemy session serves both lookups.
    session = invoice_service.db.session
    invoice = session.query(Invoice).filter_by(
        invoice_number=invoice_number, user_id=user_id,
    ).first()
    if not invoice:
        return {
            "status": "error",
            "error": f"Invoice {invoice_number!r} not found",
        }

    # Extract claim metadata from LLM tool args (snake_case per the tool
    # schema — see mailProcessorTools.py:280-320).
    claimed_amount_raw = args.get("amount_received")
    claimed_currency_supplied = bool(args.get("currency"))
    claimed_currency = args.get("currency") or (
        invoice.currency.value if hasattr(invoice.currency, 'value')
        else str(invoice.currency)
    )
    payment_method = args.get("payment_method")
    payment_date = args.get("payment_date")
    # ak-n0cg: the server-verified gmail id, never the model's free text.
    source_email_id = source_email["gmail_id"]

    # Parse claimed_amount tolerantly (LLM may pass float / str / omit).
    from services.money_utils import money, q2, MoneyError
    claimed_amount = None
    if claimed_amount_raw is not None:
        try:
            claimed_amount = q2(money(claimed_amount_raw))
        except MoneyError:
            logger.warning(
                f"ak-lvu A.8: mark_invoice_paid: non-numeric amount "
                f"{claimed_amount_raw!r} for invoice {invoice_number}; "
                f"filing claim without amount"
            )

    # ak-n0cg: server-side evidence binding the claim to its email.
    from utils.payment_claim_evidence import evaluate_claim_evidence
    customer = getattr(invoice, "customer", None)
    evidence = evaluate_claim_evidence(
        sender=source_email.get("sender"),
        subject=source_email.get("subject"),
        body=source_email.get("body"),
        invoice_number=invoice_number,
        invoice_total=invoice.total,
        invoice_currency=invoice.currency,
        claimed_amount=claimed_amount,
        claimed_currency=claimed_currency if claimed_currency_supplied else None,
        customer_emails=[getattr(customer, "email", None), invoice.to_email],
    )
    requires_user_confirmation = not evidence["all_match"]

    # Write the claim row.
    claim = PendingPaymentClaim(
        user_id=user_id,
        invoice_id=invoice.id,
        source_email_id=source_email_id,
        claimed_amount=claimed_amount,
        claimed_currency=str(claimed_currency).upper() if claimed_currency else None,
        claim_metadata={
            "payment_method": payment_method,
            "payment_date": payment_date,
            "invoice_number": invoice_number,
            # ak-n0cg: evidence flags + sender domain only (no content).
            "evidence": evidence,
            "requires_user_confirmation": requires_user_confirmation,
        },
        resolution='pending',
    )
    try:
        session.add(claim)
        session.commit()
    except Exception as exc:
        session.rollback()
        logger.error(
            f"ak-lvu A.8: failed to persist PendingPaymentClaim for "
            f"invoice {invoice_number}: {exc}"
        )
        return {
            "status": "error",
            "error": f"Failed to record payment claim: {exc}",
        }

    logger.info(
        f"ak-lvu A.8: payment claim {claim.id} filed for invoice "
        f"{invoice_number} (amount={claimed_amount} {claimed_currency}, "
        f"method={payment_method}, date={payment_date}); status NOT flipped"
    )

    return {
        "status": "pending",
        "claim_id": claim.id,
        "invoice_number": invoice_number,
        # ak-n0cg: True unless sender + amount + reference all matched.
        "requires_user_confirmation": requires_user_confirmation,
        "message": (
            "Payment claim recorded from email — pending reconciliation. "
            "Invoice status not changed until either (a) bank credit "
            "matches the claimed amount or (b) Overseer manually confirms "
            "the claim from the claims-review UI."
        ),
    }


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


def _handle_get_pdf_pages(args, pinned_pdf_path=None, user_id=None):
    """
    Render PDF pages to images using PyMuPDF and return as ImageContent.
    Supports fetching multiple pages per call via page_range (e.g. '1-3', 'all').
    Caps accumulated image size at _MAX_CONTENT_BYTES to stay under the 1MB SDK buffer.
    Falls back to single page_number for backward compatibility.
    """
    # ak-jmbw: NEVER open the model-supplied path. Use the run's own PDF
    # (pinned by the service) after a realpath containment check.
    try:
        pdf_path = resolve_pinned_pdf(pinned_pdf_path, user_id)
    except ToolPathRejected as exc:
        logger.warning(f"get_pdf_pages refused: {exc}")
        return {"error": f"get_pdf_pages refused: {exc}"}
    if model_path_differs(args.get("pdf_path"), pdf_path):
        logger.warning(
            "get_pdf_pages: model asked for a different file than the "
            "run's PDF; ignored (serving the run's PDF)"
        )
    password = args.get("password")
    page_range = args.get("page_range")
    page_number = args.get("page_number", 1)  # 1-indexed, fallback

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


def _handle_save_attachment(args, user_id, pinned_pdf_path=None):
    """Save the run's PDF attachment to local storage.

    ak-jmbw: the SOURCE is always the run's own PDF (pinned by the
    service, realpath-contained); the model's `source_path` is ignored.
    `category` (a directory name) is validated against the enum
    server-side and the destination is confined to
    attachments/<uid>/<category>/.
    """
    try:
        source_path = resolve_pinned_pdf(pinned_pdf_path, user_id)
        dest_path = safe_attachment_dest(
            user_id, args.get("category"), args.get("filename"),
        )
    except ToolPathRejected as exc:
        logger.warning(f"save_attachment refused: {exc}")
        return {"error": f"save_attachment refused: {exc}"}
    if model_path_differs(args.get("source_path"), source_path):
        logger.warning(
            "save_attachment: model asked for a different file than the "
            "run's PDF; ignored (saving the run's PDF)"
        )

    os.makedirs(os.path.dirname(dest_path), exist_ok=True)
    shutil.copy2(source_path, dest_path)
    logger.info("Saved run PDF to attachments dir")

    return {"result": "success", "saved_path": dest_path}


def _handle_report_result(args, user_id, transaction_service):
    """Log the processing result for a single email and persist to processedEmails table.

    ak-bwe: the actual upsert now lives in
    utils.processed_emails_upsert.upsert_processed_email so the
    LLM tool-call path (this function) and the service-layer
    stamping path (MailProcessorService._stamp_processed_email)
    share one implementation. Behavior is identical whichever
    caller signals completion.
    """
    from models.processedEmails import ProcessedEmails
    from utils.processed_emails_upsert import upsert_processed_email

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

    if not gmail_id or not user_id:
        return {"result": "logged", "status": status, "persisted": False}

    session = _get_db_session(transaction_service)
    if not session:
        return {"result": "logged", "status": status, "persisted": False}

    result = upsert_processed_email(
        session,
        ProcessedEmails,
        gmail_id=gmail_id,
        user_id=user_id,
        sender=args.get("sender"),
        subject=args.get("subject"),
        email_date=args.get("email_date"),
        category=category,
        processing_type=args.get("processing_type"),
        status=status,
        items_extracted=items or 0,
        extraction_summary=args.get("extraction_summary"),
        error_message=message,
    )

    if result.get("status") in ("inserted", "updated"):
        # Auto-link to freelance customer if sender matches. Only run
        # when we know a row exists; needs the row instance so we
        # re-query.
        try:
            row = session.query(ProcessedEmails).filter_by(
                gmail_id=gmail_id, user_id=user_id,
            ).first()
            if row is not None:
                from services.customerEmailService import CustomerEmailService
                ce_service = CustomerEmailService()
                ce_service.auto_link_email(row, user_id)
        except Exception:
            pass  # Non-critical, don't break email processing
        return {
            "result": "logged",
            "status": status,
            "persisted": True,
            "updated": result["status"] == "updated",
        }

    # Helper returned "failed" or an unexpected shape — log and
    # bubble up. The helper has already rolled back the session and
    # logged the specific error internally.
    logger.error(
        f"ak-bwe: upsert_processed_email returned "
        f"{result.get('status')!r} for gmail_id={gmail_id!r}: "
        f"{result.get('reason', '(no reason)')}"
    )
    return {
        "result": "logged",
        "status": status,
        "persisted": False,
    }
