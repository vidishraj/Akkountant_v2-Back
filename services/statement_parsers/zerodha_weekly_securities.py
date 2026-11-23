"""ak-7y6o (ak-8kp4 Phase 3a, 2026-11-23) — parse weekly-
securities-statement PDFs (Zerodha / HA6023 broker ID) into
per-trade dicts suitable for `InvestmentTrade` rows.

Source document: the broker publishes a weekly PDF listing
every settled trade for the holding period. The relevant
columns (confirmed per infra probe 2026-11-23):

    Transaction Date, Execution Date, Clearing Corp, Segment,
    UCC, Client PAN, Member Demat Acct, Counterparty Demat
    Acct, Settlement No., ISIN Code, Scrip Name,
    Quantity Delivered, Quantity Received, Balance (Qty),
    Transaction Type, Purpose

Key semantic rules (from the broker's convention):

  * `Quantity Delivered` is positive when the client SOLD
    shares out of their demat.
  * `Quantity Received` is positive when the client BOUGHT
    shares into their demat.
  * Exactly one of the two is non-zero per row.
  * `Transaction Type` is often a redundant label ("Buy" /
    "Sell") — we honor BOTH the label AND the Qty-column
    inference; they must agree, else the row is REJECTED
    (with a logged warning) rather than silently misclassified.

Zero-activity weeks: the PDF emits rows where every Qty column
reads "NA" / empty. The parser skips these with no error.

Multi-page statement periods: broker continues the trade
table across pages with no per-page header repeat; the parser
concatenates all pages' text before line-based extraction so
a row split across a page boundary stays intact.

Price: NOT present in these PDFs. The parser emits `price=None`
on every row; Phase 3b (ak-TBD) UPDATEs from the Kite Console
tradebook CSV.

Public API:
    parse_weekly_securities_pdf(pdf_path: str) -> List[dict]

Output dict shape (matches `InvestmentTrade` column names):
    {
        'trade_date':       date,
        'settlement_date':  date | None,
        'isin':             str,
        'symbol':           str,
        'quantity':         Decimal (positive),
        'transaction_type': 'BUY' | 'SELL',
        'segment':          str,
        'settlement_no':    str | None,
        'price':            None,
        'price_source':     None,
        'source':           'zerodha_weekly_securities_pdf',
        'source_file':      str  (pdf_path basename),
    }

Testability: the pdf-text-extraction stage is factored out of the
row-parsing stage so unit tests can inject synthetic text
without touching fitz / a real PDF.
"""
from __future__ import annotations

import logging
import os
import re
from datetime import date, datetime
from decimal import Decimal, InvalidOperation
from typing import List, Optional

try:  # pragma: no cover - fitz guard mirrors other repo parsers
    import fitz  # PyMuPDF
except ImportError:  # pragma: no cover
    fitz = None


_LOG = logging.getLogger(__name__)


_SOURCE_TAG = "zerodha_weekly_securities_pdf"


# Field tokens that indicate a header line (either on the first
# page or on a continuation). Skip lines containing any of these
# markers so page-concat doesn't produce header-as-data rows.
_HEADER_MARKERS = (
    "Transaction Date",
    "Execution Date",
    "Scrip Name",
    "Settlement No.",
    "Quantity Delivered",
    "Quantity Received",
)


def _parse_date(raw: str) -> Optional[date]:
    """Broker emits dates as `DD-MM-YYYY` on these statements.
    Tolerant of surrounding whitespace / stray quotes. Returns
    None on empty / malformed input (caller decides to skip row)."""
    if raw is None:
        return None
    raw = raw.strip().strip('"').strip("'")
    if not raw or raw.upper() in ("NA", "N/A", "-"):
        return None
    for fmt in ("%d-%m-%Y", "%Y-%m-%d", "%d/%m/%Y"):
        try:
            return datetime.strptime(raw, fmt).date()
        except ValueError:
            continue
    return None


def _parse_qty(raw: str) -> Decimal:
    """Parse a quantity cell. 'NA' / empty / '-' → Decimal(0)
    (lets the row pass through the zero-activity filter).
    Caller gets Decimal; validate_row rejects on >14-digit
    overflow before persistence."""
    if raw is None:
        return Decimal("0")
    r = raw.strip().strip('"').strip("'").replace(",", "")
    if not r or r.upper() in ("NA", "N/A", "-"):
        return Decimal("0")
    try:
        return Decimal(r)
    except InvalidOperation:
        return Decimal("0")


def _is_header_or_blank(line: str) -> bool:
    if not line or not line.strip():
        return True
    stripped = line.strip()
    for mk in _HEADER_MARKERS:
        if mk in stripped:
            return True
    return False


def extract_rows_from_text(
    full_text: str, source_file: str,
) -> tuple:
    """Parse an already-extracted full-document text string into
    trade dicts. Factored out of PDF loading so unit tests can
    pass synthetic text.

    Row-split heuristic: broker emits each trade as a run of
    whitespace-separated columns on a single line (after fitz
    text extraction with preserve_whitespace). The parser
    tolerates variable column spacing + attempts tab-first
    then multi-space split; rejects rows that don't resolve to
    the expected minimum column count.

    Returns:
        (rows, rejections) tuple:
        * rows: list[dict] of emitted trade rows (never
          contains a row with settlement_no=None; see below)
        * rejections: dict[str, int] of per-reason reject
          counters. Keys:
            - 'qty_ambiguous'   : qty_delivered + qty_received
              both non-zero OR both zero for a data row (bad
              shape)
            - 'label_mismatch'  : Transaction Type label
              disagrees with Qty-column inference
            - 'missing_settlement'  : settlement_no absent /
              empty (ak-7y6o reviewer CRITICAL — SQL UNIQUE
              treats NULL ≠ NULL so NULL-settlement rows
              would double-insert without IntegrityError;
              hard-reject at parse time + document in the
              ingest-script report)

    ak-7y6o reviewer CRITICAL (2026-11-23): rows WITHOUT a
    parseable settlement_no are REJECTED, not emitted with
    settlement_no=None. Two reasons:
      1. UniqueConstraint on
         (user_id, isin, trade_date, settlement_no,
         transaction_type) doesn't dedup NULL-settlement rows
         (SQL semantics: NULL ≠ NULL). Phase 3c aggregator
         would then double-count those trades → wrong
         portfolio totals.
      2. Phase 3b price-fill `UPDATE … WHERE settlement_no = ?`
         never matches NULL — the tail would silently
         remain price-NULL forever.
    """
    rows: List[dict] = []
    rejections: dict = {
        "qty_ambiguous": 0,
        "label_mismatch": 0,
        "missing_settlement": 0,
    }
    line_no = 0

    for raw_line in full_text.splitlines():
        line_no += 1
        if _is_header_or_blank(raw_line):
            continue

        # Column split — broker uses either tab-separated OR
        # multi-space-separated. Try tab first.
        if "\t" in raw_line:
            cols = [c.strip() for c in raw_line.split("\t") if c.strip()]
        else:
            cols = [c for c in re.split(r"\s{2,}", raw_line.strip()) if c]

        # Expected minimum: Transaction Date + Execution Date +
        # ... + Settlement No. + ISIN + Scrip + Qty Delivered +
        # Qty Received + Balance + Txn Type. In practice some
        # PDFs collapse Clearing Corp / UCC / PAN / Demat
        # columns onto a prior line; robust fallback below.
        # We REQUIRE at minimum: 2 dates + ISIN + scrip +
        # 2 qty cols + txn type = 7 columns.
        if len(cols) < 7:
            continue

        # Best-effort column extraction — scan for the first two
        # DD-MM-YYYY tokens (txn_date, settlement_date), the
        # first ISIN-shaped token (INE or IN followed by 10 alnums),
        # and the first two numeric-or-NA tokens AFTER the ISIN
        # (qty_delivered, qty_received).
        txn_date = None
        settlement_date = None
        isin = None
        symbol = None
        qty_delivered = None
        qty_received = None
        segment = None
        settlement_no = None
        txn_type_label = None

        # Date scan
        date_tokens = []
        for c in cols:
            if re.match(r"^\d{2}-\d{2}-\d{4}$", c):
                date_tokens.append(c)
            if len(date_tokens) == 2:
                break
        if len(date_tokens) < 2:
            continue
        txn_date = _parse_date(date_tokens[0])
        settlement_date = _parse_date(date_tokens[1])
        if txn_date is None:
            continue

        # ISIN scan — Indian ISINs are `INE` or `IN` + 10 alnums.
        for i, c in enumerate(cols):
            if re.match(r"^IN[A-Z0-9]{10}$", c):
                isin = c
                # Scrip name follows ISIN (may contain spaces,
                # may be multi-token). Walk forward until we hit
                # a numeric / NA / segment token.
                scrip_tokens = []
                j = i + 1
                while j < len(cols):
                    nxt = cols[j]
                    if (re.match(r"^-?[0-9][0-9,]*\.?[0-9]*$", nxt)
                            or nxt.upper() in ("NA", "N/A", "-")
                            or nxt in ("EQ", "F&O", "CDS", "MCX", "BSE", "NSE")):
                        break
                    scrip_tokens.append(nxt)
                    j += 1
                symbol = " ".join(scrip_tokens).strip() or None
                break

        if not isin or not symbol:
            continue

        # Qty columns — first two numeric-or-NA tokens AFTER
        # the scrip tokens.
        qty_tokens: List[str] = []
        past_scrip = False
        for c in cols:
            if c == isin:
                past_scrip = True
                continue
            if not past_scrip:
                continue
            # Skip the scrip-name tokens (they came between ISIN
            # and the first qty token).
            if (re.match(r"^-?[0-9][0-9,]*\.?[0-9]*$", c)
                    or c.upper() in ("NA", "N/A", "-")):
                qty_tokens.append(c)
                if len(qty_tokens) == 2:
                    break
        if len(qty_tokens) < 2:
            continue
        qty_delivered = _parse_qty(qty_tokens[0])
        qty_received = _parse_qty(qty_tokens[1])

        # Zero-activity row — skip silently (bead §scope).
        if qty_delivered == 0 and qty_received == 0:
            continue

        # Transaction type inference + label cross-check.
        if qty_delivered > 0 and qty_received == 0:
            inferred_type = "SELL"
            quantity = qty_delivered
        elif qty_received > 0 and qty_delivered == 0:
            inferred_type = "BUY"
            quantity = qty_received
        else:
            # Ambiguous shape — either BOTH non-zero or BOTH
            # zero. Zero-activity rows are filtered above
            # (qty_delivered == 0 and qty_received == 0 →
            # silent skip). Reaching here means the data row
            # is malformed; log + count as rejected.
            # ak-7y6o reviewer m1: this branch was previously
            # logged as "both non-zero" only; message now
            # covers both pathological cases.
            _LOG.warning(
                "[ak-7y6o] REJECT row: qty_delivered + "
                "qty_received both zero or both non-zero "
                "(malformed row) at %s:line %d (ISIN=%s, "
                "qty_delivered=%s, qty_received=%s)",
                source_file, line_no, isin,
                qty_delivered, qty_received,
            )
            rejections["qty_ambiguous"] += 1
            continue

        # Label cross-check — scan remaining cols for Buy/Sell
        # label; if present and disagrees with inferred_type,
        # REJECT the row (tightest discipline).
        for c in cols:
            cu = c.upper().strip()
            if cu in ("BUY", "SELL", "PURCHASE", "SALE"):
                txn_type_label = "BUY" if cu in ("BUY", "PURCHASE") else "SELL"
                break
        if txn_type_label is not None and txn_type_label != inferred_type:
            _LOG.warning(
                "[ak-7y6o] REJECT row: Transaction Type label "
                "(%s) disagrees with Qty-column inference (%s) "
                "at %s:line %d (ISIN=%s)",
                txn_type_label, inferred_type, source_file,
                line_no, isin,
            )
            rejections["label_mismatch"] += 1
            continue

        # Segment scan — presence of a known segment token.
        for c in cols:
            if c in ("EQ", "F&O", "CDS", "MCX"):
                segment = c
                break
        # Default to EQ for weekly-securities-statement (equity
        # cash market); fall-through to keep rows even when the
        # broker omits the segment column on this layout.
        if not segment:
            segment = "EQ"

        # Settlement number — the broker's column order places
        # Settlement_No immediately BEFORE the ISIN. Pick the
        # token at that position; fall back to None if the
        # column is missing or shaped unexpectedly.
        isin_idx = None
        for idx, c in enumerate(cols):
            if c == isin:
                isin_idx = idx
                break
        if isin_idx is not None and isin_idx > 0:
            candidate = cols[isin_idx - 1]
            # Accept short alphanumeric strings. Reject obvious
            # non-settlement shapes (dates / segment labels /
            # clearing corps / PAN).
            if (re.match(r"^[0-9A-Z]{4,12}$", candidate)
                    and not re.match(r"^\d{2}-\d{2}-\d{4}$", candidate)
                    and candidate not in (
                        "EQ", "F&O", "CDS", "MCX",
                        "BSE", "NSE", "NSCCL", "ICCL",
                    )
                    # Not a 10-char PAN (AAAAA0000A shape:
                    # 5 letters + 4 digits + 1 letter).
                    and not re.match(r"^[A-Z]{5}[0-9]{4}[A-Z]$",
                                     candidate)):
                settlement_no = candidate

        # ak-7y6o reviewer CRITICAL (2026-11-23): REJECT rows
        # without a parseable settlement_no. Two
        # correctness dependencies:
        #   1. Dedup UniqueConstraint relies on settlement_no
        #      being NOT NULL (SQL UNIQUE treats NULL ≠ NULL;
        #      Phase 3c would double-count the NULL-settlement
        #      tail).
        #   2. Phase 3b price-fill UPDATE joins on
        #      settlement_no; NULL rows never get prices filled
        #      and silently remain price-NULL forever.
        # Operator surfaces these in dry-run output + decides
        # if the underlying PDF is anomalous (bead §Option-a
        # tight-discipline).
        if settlement_no is None or not settlement_no.strip():
            _LOG.warning(
                "[ak-7y6o] REJECT row: missing settlement_no "
                "at %s:line %d (ISIN=%s, symbol=%s, "
                "trade_date=%s, qty=%s) — dedup + Phase 3b "
                "both require a non-NULL settlement number",
                source_file, line_no, isin, symbol,
                txn_date, quantity,
            )
            rejections["missing_settlement"] += 1
            continue

        rows.append({
            "trade_date": txn_date,
            "settlement_date": settlement_date,
            "isin": isin,
            "symbol": symbol,
            "quantity": quantity,
            "transaction_type": inferred_type,
            "segment": segment,
            "settlement_no": settlement_no,
            "price": None,
            "price_source": None,
            "source": _SOURCE_TAG,
            "source_file": os.path.basename(source_file),
        })

    total_rejected = sum(rejections.values())
    if total_rejected > 0:
        _LOG.info(
            "[ak-7y6o] extract_rows_from_text(%s): %d rows "
            "parsed, %d rows rejected (qty_ambiguous=%d, "
            "label_mismatch=%d, missing_settlement=%d) — see "
            "WARN logs above",
            source_file, len(rows), total_rejected,
            rejections["qty_ambiguous"],
            rejections["label_mismatch"],
            rejections["missing_settlement"],
        )
    return rows, rejections


def parse_weekly_securities_pdf(pdf_path: str) -> tuple:
    """Load the PDF, concatenate all pages' text, extract trade
    rows. Returns ([], {empty rejections}) on empty / zero-
    activity statement or on PyMuPDF load failure (with WARN
    log — caller decides whether to abort the ingest run).

    Returns:
        (rows, rejections) — same shape as extract_rows_from_
        text. See its docstring for rejections keys.
    """
    _empty_rejections = {
        "qty_ambiguous": 0,
        "label_mismatch": 0,
        "missing_settlement": 0,
    }
    if fitz is None:  # pragma: no cover
        raise RuntimeError(
            "PyMuPDF (fitz) not installed — required for "
            "ak-7y6o Phase 3a PDF parsing"
        )
    try:
        doc = fitz.open(pdf_path)
    except Exception as exc:
        _LOG.warning(
            "[ak-7y6o] parse_weekly_securities_pdf(%s): fitz "
            "load failed (%s); returning empty row list",
            pdf_path, type(exc).__name__,
        )
        return [], dict(_empty_rejections)

    parts = []
    try:
        for page in doc:
            try:
                parts.append(page.get_text("text"))
            except Exception as exc:
                _LOG.warning(
                    "[ak-7y6o] page.get_text failure on %s: %s",
                    pdf_path, type(exc).__name__,
                )
                continue
    finally:
        doc.close()
    full_text = "\n".join(parts)
    return extract_rows_from_text(full_text, pdf_path)


__all__ = [
    "parse_weekly_securities_pdf",
    "extract_rows_from_text",
]
