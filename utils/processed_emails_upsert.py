"""ak-bwe: processedEmails upsert helper — idempotency for text-mode
statement processing.

BUG (dispatch hq-wisp-bk6efn, promoted to P1):

  ak-5oi's backfill script (boi_backfill_2026.py and any similar
  driver) invokes mailProcessorService.process_emails, which
  processes PDF statements via _process_single_pdf_email →
  _run_pdf_analysis → _run_all_chunks_async. Transactions land
  correctly, PDFs get persisted, BUT no processedEmails row is
  inserted for the ingested email.

  _update_processed_email_pdf runs after persistence but is an
  UPDATE-only path — it logs "No processedEmails row found for
  {gmail_id} to update pdf_filename" and moves on. The row never
  materializes.

  On the next re-run (backfill re-fire, cron re-processing, ak-32o
  HDFC re-parse), _filter_already_processed doesn't see the
  gmail_id in processedEmails → the file gets re-ingested →
  duplicate transactions accumulate (some absorbed by ak-8l5's
  storage-layer dedup, some slipping past the ak-8l5-residual-B
  edge case where suffixed rows land as -dupN vs -dup(N+1)).

FIX:

  Provide a single upsert function that either callers of
  _handle_report_result (LLM tool path) OR the text-mode
  processing path can use to stamp processedEmails after a
  successful ingestion. Matches the shape of the existing
  _handle_report_result upsert but decoupled from the tool-call
  interface, so the service layer can call it directly with
  strongly-typed args.

  Pure-Python. Callers own the SQLAlchemy session; helper handles
  the insert / integrity-error / update dance. Returns a dict with
  the outcome so callers can log/return meaningful status.

Related: ak-8l5 residual B (find_disambiguated_ref allocates a new
-dupN on every re-parse instead of reusing when content matches a
previously-suffixed row) is a separate downstream bug — this fix
narrows the re-parse rate but doesn't fix the suffix-explosion
edge case. That's tracked separately.
"""

from __future__ import annotations

from datetime import datetime
from typing import Optional


# Tool-status → DB-status mapping (mirrors _handle_report_result so
# behavior is identical whether the LLM path or the service-layer
# path calls the upsert).
_STATUS_MAP = {
    "success": "processed",
    "skipped": "skipped",
    "error": "failed",
}


def _parse_email_date(raw_date) -> Optional[datetime]:
    """Best-effort parse of a raw email_date string. Returns None if
    none of the tolerated formats match (caller should degrade
    gracefully — email_date is nullable in the schema)."""
    if not raw_date:
        return None
    if isinstance(raw_date, datetime):
        return raw_date
    if not isinstance(raw_date, str):
        return None
    for fmt in (
        "%Y-%m-%d",
        "%d/%m/%Y",
        "%d-%m-%Y",
        "%Y-%m-%dT%H:%M:%S",
    ):
        try:
            return datetime.strptime(raw_date, fmt)
        except ValueError:
            continue
    return None


def map_tool_status_to_db(tool_status: Optional[str]) -> str:
    """Public tool-status → DB-status map. Unknown / missing → 'processed'
    (matches pre-ak-bwe fallback in _handle_report_result)."""
    if not tool_status:
        return "processed"
    return _STATUS_MAP.get(tool_status, "processed")


def upsert_processed_email(
    session,
    processed_emails_model,
    *,
    gmail_id: str,
    user_id: str,
    sender: Optional[str] = None,
    subject: Optional[str] = None,
    email_date=None,
    category: Optional[str] = "unknown",
    processing_type: Optional[str] = None,
    status: Optional[str] = "success",
    items_extracted: int = 0,
    extraction_summary=None,
    error_message: Optional[str] = None,
    integrity_error_class=None,
) -> dict:
    """ak-bwe: upsert a processedEmails row.

    - If no row exists for (gmail_id, user_id): INSERT.
    - If a row exists (IntegrityError on the unique constraint):
      UPDATE the mutable fields on the existing row.

    Callers own the session — this helper commits after the write
    and rolls back on failure. Returns:
      {
        "status": "inserted" | "updated" | "no_session" | "failed",
        "gmail_id": <id>,
        "db_status": <mapped from `status`>,
      }

    integrity_error_class defaults to sqlalchemy.exc.IntegrityError;
    injected for tests so they can raise a stand-in without pulling
    SQLAlchemy in.

    Contract:
      - Never raises. Always returns a dict with a "status" field.
      - When the row already exists and the incoming status is
        'processed' (or maps to it), the previous row's status is
        overwritten. That's intentional — a successful re-parse
        should upgrade a prior 'failed'/'skipped' row to 'processed'.
    """
    if not gmail_id or not user_id:
        return {"status": "failed", "gmail_id": gmail_id, "reason": "missing key"}

    if session is None:
        return {"status": "no_session", "gmail_id": gmail_id}

    if integrity_error_class is None:
        try:
            from sqlalchemy.exc import IntegrityError as _IE
            integrity_error_class = _IE
        except Exception:  # pragma: no cover — defensive
            integrity_error_class = Exception

    db_status = map_tool_status_to_db(status)
    parsed_email_date = _parse_email_date(email_date)

    try:
        row = processed_emails_model(
            gmail_id=gmail_id,
            user_id=user_id,
            sender=sender,
            subject=subject,
            email_date=parsed_email_date,
            category=category,
            processing_type=processing_type,
            status=db_status,
            items_extracted=items_extracted or 0,
            extraction_summary=extraction_summary,
            error_message=error_message if db_status == "failed" else None,
        )
        session.add(row)
        session.commit()
        return {
            "status": "inserted",
            "gmail_id": gmail_id,
            "db_status": db_status,
        }
    except integrity_error_class:
        session.rollback()
        try:
            existing = session.query(processed_emails_model).filter_by(
                gmail_id=gmail_id, user_id=user_id,
            ).first()
            if existing is None:
                # Rare race — the row disappeared between the
                # IntegrityError and the query. Log and give up
                # (idempotency isn't threatened; something else
                # will re-stamp on next run).
                return {
                    "status": "failed",
                    "gmail_id": gmail_id,
                    "reason": "row vanished after IntegrityError",
                }
            existing.category = category
            existing.status = db_status
            existing.items_extracted = items_extracted or 0
            existing.extraction_summary = extraction_summary
            existing.processing_type = processing_type
            if db_status == "failed":
                existing.error_message = error_message
            session.commit()
            return {
                "status": "updated",
                "gmail_id": gmail_id,
                "db_status": db_status,
            }
        except Exception as e:
            try:
                session.rollback()
            except Exception:
                pass
            return {
                "status": "failed",
                "gmail_id": gmail_id,
                "reason": f"update failed: {e}",
            }
    except Exception as e:
        try:
            session.rollback()
        except Exception:
            pass
        return {
            "status": "failed",
            "gmail_id": gmail_id,
            "reason": f"insert failed: {e}",
        }
