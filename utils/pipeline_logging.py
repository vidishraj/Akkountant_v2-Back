"""ak-n44: structured pipeline observability.

BUG (per Lead's Batch 2 dispatch hq-wisp-ekveei):

  Chunk-level errors happen silently. When we grep logs post-hoc
  to diagnose failures, most of the info is missing. Today's
  ak-32o diag took hours to grep because we had to correlate
  fileIDs, chunk indices, and error text spread across free-form
  log lines. Would have been ~15 minutes with structured logging.

FIX:

  Emit a single-line JSON payload per pipeline event with a fixed
  field schema so a single `grep '"event":"chunk_error"' | jq`
  produces a per-file per-chunk error report.

  Mandatory fields (present on every payload):
    - event         — canonical event name (chunk_error,
                       chunk_retry, chunk_ok, file_start,
                       file_complete, reconciliation_divergent, …)
    - timestamp     — ISO-8601 UTC. Human + machine readable.

  Optional fields (populated per event):
    - file_id, gmail_id, user_id
    - chunk_index, chunks_total, page_range
    - error_type    — canonical label from
                       utils.reprocess_status._classify_reason
                       (mysql_1040, sdk_message_reader,
                        pdf_password, pdf_open, extraction_empty,
                        stream, rate_limit, timeout,
                        analysis_exception,
                        unknown_chunk_result_shape, unknown).
    - message       — free-form (typically str(exception))
    - traceback     — best-effort exception trace when the caller
                       hands us the BaseException object
    - Any additional caller-supplied `extra` fields spread at top
      level so a per-event schema addition is one dict key away.

Fires through the standard logger tree — no new sink or handler
required. Grep on the JSON substring; the existing per-service
formatter wraps the JSON in the standard preamble.

Pure-Python (no framework deps) so it's testable without booting
flask / SQLAlchemy. Depends on utils.reprocess_status for
_classify_reason so ak-1rh's canonical labels are the single
source of truth.
"""

from __future__ import annotations

import json
import logging
import traceback as _tb
from datetime import datetime, timezone
from typing import Any, Optional

# ak-n44 reuses ak-1rh's canonical error_type labels so
# reprocess_pdf's summary and every chunk-error log line agree
# byte-for-byte on the label taxonomy.
from utils.reprocess_status import _classify_reason


# Canonical event names. Not enforced; documented so a future
# reviewer can see the schema at a glance.
EVENT_CHUNK_ERROR = "chunk_error"
EVENT_CHUNK_RETRY = "chunk_retry"
EVENT_CHUNK_OK = "chunk_ok"
EVENT_FILE_START = "file_start"
EVENT_FILE_COMPLETE = "file_complete"
EVENT_RECONCILIATION_DIVERGENT = "reconciliation_divergent"
EVENT_STAMP_SKIPPED = "stamp_skipped"


def _iso_utc_now() -> str:
    """ISO-8601 UTC with a trailing 'Z' so grep and log-shipper
    parsers agree on the shape."""
    return datetime.now(timezone.utc).strftime("%Y-%m-%dT%H:%M:%S.%fZ")


def _serialize_exception(exc: Optional[BaseException]) -> tuple[Optional[str], Optional[str]]:
    """Return (message, traceback_str) for a caught exception.
    Both None if `exc` is None. Falls back to str(exc) if a
    formatter fails."""
    if exc is None:
        return None, None
    try:
        message = str(exc)
    except Exception:
        message = repr(exc)
    try:
        tb = "".join(
            _tb.format_exception(type(exc), exc, exc.__traceback__)
        )
    except Exception:
        tb = None
    return message, tb


def build_pipeline_event(
    event: str,
    *,
    file_id: Optional[str] = None,
    gmail_id: Optional[str] = None,
    user_id: Optional[str] = None,
    chunk_index: Optional[int] = None,
    chunks_total: Optional[int] = None,
    page_range: Optional[list] = None,
    error_type: Optional[str] = None,
    message: Optional[str] = None,
    exception: Optional[BaseException] = None,
    extra: Optional[dict] = None,
) -> dict:
    """Pure-Python payload builder. The emit_* wrappers call this
    then hand the dict to the logger; tests can call it directly
    to assert schema.

    Rules:
      - `event` is required and stamped verbatim (no case
        normalization — this is the grep key).
      - `timestamp` is stamped automatically (UTC ISO-8601).
      - `error_type` is auto-classified from `message` or
        `str(exception)` if the caller didn't supply one and either
        is present. Falls back to 'unknown'.
      - `message` is populated from `exception` if the caller
        didn't supply one.
      - `exception` (if provided) adds a `traceback` field.
      - `extra` values are spread at TOP LEVEL to keep the JSON
        flat — a caller adding {"retry_attempt": 2} produces
        `"retry_attempt": 2` next to the mandatory fields, not
        nested under "extra".
    """
    payload: dict = {
        "event": event,
        "timestamp": _iso_utc_now(),
    }
    if file_id is not None:
        payload["file_id"] = file_id
    if gmail_id is not None:
        payload["gmail_id"] = gmail_id
    if user_id is not None:
        payload["user_id"] = user_id
    if chunk_index is not None:
        payload["chunk_index"] = chunk_index
    if chunks_total is not None:
        payload["chunks_total"] = chunks_total
    if page_range is not None:
        payload["page_range"] = list(page_range)

    exc_message, exc_tb = _serialize_exception(exception)
    resolved_message = message if message is not None else exc_message
    if resolved_message is not None:
        payload["message"] = resolved_message

    if error_type is None:
        # Auto-classify from the message the caller supplied OR
        # the exception's str. Falls back to 'unknown' via
        # _classify_reason's own default.
        error_type = _classify_reason(resolved_message or "")
    payload["error_type"] = error_type

    if exc_tb is not None:
        payload["traceback"] = exc_tb

    if extra:
        for k, v in extra.items():
            # Never let extra silently overwrite a mandatory field.
            if k in ("event", "timestamp"):
                continue
            payload[k] = v

    return payload


def emit_pipeline_event(
    logger: logging.Logger,
    event: str,
    *,
    level: int = logging.INFO,
    **kwargs,
) -> dict:
    """ak-n44 general-purpose emitter. Builds the payload, encodes
    as a single-line JSON string, and dispatches through the given
    logger at `level`. Returns the payload dict so callers can
    inspect / assert.

    Prefer the event-specific wrappers below (emit_chunk_error
    etc.) — they document the expected fields per event. This
    function is the escape hatch for one-off events.
    """
    payload = build_pipeline_event(event, **kwargs)
    try:
        line = json.dumps(payload, default=str, sort_keys=True)
    except Exception:
        # Absolute last resort: never let the logger error itself
        # take down the pipeline.
        line = f'{{"event": {event!r}, "message": "json_encode_failed"}}'
    logger.log(level, line)
    return payload


def emit_chunk_error(
    logger: logging.Logger,
    *,
    file_id: Optional[str] = None,
    chunk_index: Optional[int] = None,
    chunks_total: Optional[int] = None,
    page_range: Optional[list] = None,
    error_type: Optional[str] = None,
    message: Optional[str] = None,
    exception: Optional[BaseException] = None,
    gmail_id: Optional[str] = None,
    user_id: Optional[str] = None,
    extra: Optional[dict] = None,
) -> dict:
    """ak-n44 canonical: emit a structured chunk-error log line.

    Level defaults to ERROR because a chunk-error is a failure
    the operator should see. Callers can override via
    emit_pipeline_event(...) with a lower level if the chunk error
    was a retry recoverable via ak-wty backoff.

    Any exception passed via `exception` contributes both
    `message` (str) and `traceback` fields. If no error_type is
    supplied, auto-classified from the message/exception.
    """
    return emit_pipeline_event(
        logger, EVENT_CHUNK_ERROR,
        level=logging.ERROR,
        file_id=file_id, chunk_index=chunk_index,
        chunks_total=chunks_total, page_range=page_range,
        error_type=error_type, message=message,
        exception=exception, gmail_id=gmail_id, user_id=user_id,
        extra=extra,
    )


def emit_chunk_retry(
    logger: logging.Logger,
    *,
    file_id: Optional[str] = None,
    chunk_index: Optional[int] = None,
    chunks_total: Optional[int] = None,
    page_range: Optional[list] = None,
    attempt: Optional[int] = None,
    max_attempts: Optional[int] = None,
    error_type: Optional[str] = None,
    message: Optional[str] = None,
    extra: Optional[dict] = None,
) -> dict:
    """ak-n44 canonical: emit a structured chunk-retry log line
    (WARNING level). Used by ak-wty's backoff loop. attempt +
    max_attempts land in the JSON as top-level fields."""
    kwargs_extra = dict(extra or {})
    if attempt is not None:
        kwargs_extra["attempt"] = attempt
    if max_attempts is not None:
        kwargs_extra["max_attempts"] = max_attempts
    return emit_pipeline_event(
        logger, EVENT_CHUNK_RETRY,
        level=logging.WARNING,
        file_id=file_id, chunk_index=chunk_index,
        chunks_total=chunks_total, page_range=page_range,
        error_type=error_type, message=message,
        extra=kwargs_extra,
    )


def emit_reconciliation_divergent(
    logger: logging.Logger,
    *,
    file_id: Optional[str] = None,
    bank: Optional[str] = None,
    extracted_debits: Optional[float] = None,
    extracted_credits: Optional[float] = None,
    stated_debits: Optional[float] = None,
    stated_credits: Optional[float] = None,
    checked_fields: Optional[list] = None,
    reason: Optional[str] = None,
    extra: Optional[dict] = None,
) -> dict:
    """ak-n44 canonical: emit a structured
    reconciliation-divergent log line (WARNING level). Used by
    ak-ifc v3's file-level reconciliation fallback path."""
    kwargs_extra = dict(extra or {})
    if bank is not None:
        kwargs_extra["bank"] = bank
    if extracted_debits is not None:
        kwargs_extra["extracted_debits"] = extracted_debits
    if extracted_credits is not None:
        kwargs_extra["extracted_credits"] = extracted_credits
    if stated_debits is not None:
        kwargs_extra["stated_debits"] = stated_debits
    if stated_credits is not None:
        kwargs_extra["stated_credits"] = stated_credits
    if checked_fields is not None:
        kwargs_extra["checked_fields"] = list(checked_fields)
    return emit_pipeline_event(
        logger, EVENT_RECONCILIATION_DIVERGENT,
        level=logging.WARNING,
        file_id=file_id,
        message=reason,
        error_type="reconciliation_divergent",
        extra=kwargs_extra,
    )
