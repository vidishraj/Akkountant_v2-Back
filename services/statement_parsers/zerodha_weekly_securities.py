"""ak-7y6o / ak-4t7f (ak-8kp4 Phase 3a) — parse weekly-
securities-statement PDFs (Zerodha / HA6023 broker ID) into
per-trade dicts suitable for `InvestmentTrade` rows.

## Real layout (measured 2026-11-23 over the Overseer's statements)

Each page carries two ruled tables, each under a section title:

  1. "Statement of Accounts of Securities/Commodities for the
     period from YYYY-MM-DD to YYYY-MM-DD ...": SETTLED demat
     movements. These are the rows we ingest.
  2. "Pending Obligations as on YYYY-MM-DD ...": obligations not
     yet settled. They are NOT ingested. Each one reappears as a
     SETTLED row in a later week's statement, so ingesting both
     would double-count. Pending rows also use a broker-side
     direction convention: a client BUY shows as purpose=payin
     with Quantity Delivered > 0, which is the opposite of the
     settled convention below. They are counted
     (`stats['pending_skipped']`) for reconciliation only.

Both tables have the same 21 physical columns. The header labels
(the demat-account columns span two physical sub-columns each)
are:

    Transaction Date | Execution Date |
    Clearing Corporation/Clearing Member | Segment Type |
    Unique Client Code (UCC) | Back Office Client Code |
    Client Name | Client PAN | Member Demat Account No. | <sub> |
    Counterparty Demat Account No. | <sub> | Settlement No. |
    ISIN Code | Scrip Name | Quantity Delivered (Qty.) |
    Quantity Received (Qty.) | Balance (Qty.) | Trf. Ref. No. |
    Transaction Type | Purpose

PyMuPDF `get_text("text")` emits these one cell per line, which is
why the original line-splitting parser yielded 0 rows. We use
`page.find_tables()` (ruled-table detection). It returns one cell
per column with wrapped text joined by '\\n'. Columns are mapped
by HEADER LABEL, not position, so a column reorder or insertion
cannot silently shift values.

Cell conventions: dates are YYYY-MM-DD (DD-MM-YYYY tolerated);
quantities are decimals such as "10.000"; empty or zero-activity
cells read "NA"; zero-activity weeks have a single all-"NA" row.

## Semantics (settled section)

  * Quantity Received > 0  → BUY  (purpose "payout")
  * Quantity Delivered > 0 → SELL (purpose "payin", e.g. Early
    Pay-in)
  * Exactly one must be > 0. Both or neither → REJECT
    (qty_ambiguous).
  * Purpose / Transaction-Type label must agree with the qty
    inference, else REJECT (label_mismatch).
  * trade_date = "Transaction Date". NOTE: on settled rows this is
    the DEMAT MOVEMENT date, which is T+1 or later relative to the
    exchange trade date (measured: a pending row's date is 3-5
    days earlier than its settled twin). Phase 3b (tradebook)
    is the source of the true exchange trade date.
  * settlement_date = "Execution Date" (equal to Transaction Date
    on every settled row measured).

## Multi-page

The table can continue on the next page. A continuation table
with NO header row and no section title inherits the previous
table's column map and section. A table that has its own header
row but no section title above it is NOT treated as a
continuation; its rows are rejected (fail closed). If the first data
row of a new page has neither a date nor an ISIN, it is the tail
of a row split by the page break, and its cells are merged into
the previous page's last row.

## Rejections (never silent)

`qty_ambiguous`, `label_mismatch`, `missing_settlement` (a
NULL settlement_no would defeat the UNIQUE dedup and the Phase 3b
price-fill join), and `malformed` (bad date / ISIN checksum /
quantity / over-length field / row in an unmapped table).

## Secrecy

Logs carry only the file basename, the row index, and the reason.
They never carry ISIN, scrip, quantity, date, PAN, name or demat
number.

Public API:
    parse_weekly_securities_pdf(pdf_path) -> (rows, rejections, stats)
    extract_rows_from_tables(tables, source_file)
        -> (rows, rejections, stats)
        Pure (no fitz), for unit tests. `tables` is a list of
        {'section': 'settled'|'pending'|None, 'page': int,
         'rows': list[list[str|None]]} in document order, where
        'rows' is exactly what `Table.extract()` returns.

Output row dict (matches `InvestmentTrade` column names):
    trade_date (date), settlement_date (date|None), isin, symbol,
    quantity (Decimal > 0), transaction_type ('BUY'|'SELL'),
    segment, settlement_no, price=None, price_source=None,
    source='zerodha_weekly_securities_pdf', source_file (basename)
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

SECTION_SETTLED = "settled"
SECTION_PENDING = "pending"

_SECTION_TITLES = (
    # (regex on whitespace-normalised block text, section)
    (re.compile(r"^Statement of Accounts of Securities", re.I),
     SECTION_SETTLED),
    (re.compile(r"^Pending Obligations", re.I), SECTION_PENDING),
)

# field -> predicate on the normalised (lower-case, single-spaced)
# header label. Matched by label so a column shift can't move data.
_HEADER_FIELDS = {
    "trade_date": lambda h: h == "transaction date",
    "execution_date": lambda h: h == "execution date",
    "segment": lambda h: h.startswith("segment"),
    "settlement_no": lambda h: h.startswith("settlement no"),
    "isin": lambda h: h.startswith("isin"),
    "symbol": lambda h: h.startswith("scrip name"),
    "qty_delivered": lambda h: h.startswith("quantity delivered"),
    "qty_received": lambda h: h.startswith("quantity received"),
    "txn_type": lambda h: h == "transaction type",
    "purpose": lambda h: h == "purpose",
}
_REQUIRED_FIELDS = (
    "trade_date", "settlement_no", "isin", "symbol",
    "qty_delivered", "qty_received",
)

_NA = ("", "NA", "N/A", "-")

_ISIN_RE = re.compile(r"^IN[A-Z0-9]{9}[0-9]$")
# Text-side ISIN count for reconciliation. Indian issuer-type
# prefixes only (E company, F fund, 0-9 government). This excludes
# the broker's "SEBI Reg.No.: INZ…" header token, which happens to
# pass the ISIN checksum.
_TEXT_ISIN_RE = re.compile(r"\bIN[EF0-9][A-Z0-9]{8}[0-9]\b")
_PAN_RE = re.compile(r"^[A-Z]{5}[0-9]{4}[A-Z]$")
_SETTLEMENT_RE = re.compile(r"^[0-9A-Z]{4,12}$")
_QTY_RE = re.compile(r"^-?\d+(\.\d+)?$")

# Model column limits (models/investmentTrade.py).
_MAX_SYMBOL = 50
_MAX_SEGMENT = 10
_MAX_SETTLEMENT = 20
_MAX_QTY = Decimal("9999999999")  # Numeric(14, 4) integer part


def _empty_rejections() -> dict:
    return {
        "qty_ambiguous": 0,
        "label_mismatch": 0,
        "missing_settlement": 0,
        "malformed": 0,
    }


def _empty_stats() -> dict:
    return {
        # Data rows whose ISIN cell holds a checksum-valid ISIN,
        # in ANY section. Equals rows + rejected-with-valid-ISIN
        # + pending_skipped.
        "isin_rows": 0,
        "pending_skipped": 0,
        "zero_activity": 0,
        "tables": 0,
    }


def _norm(cell) -> str:
    """Collapse whitespace (incl. the '\\n' that find_tables puts
    in wrapped cells) to single spaces."""
    if cell is None:
        return ""
    return " ".join(str(cell).split())


def _is_na(cell) -> bool:
    return _norm(cell).upper() in _NA


def isin_checksum_ok(isin: str) -> bool:
    """ISO 6166 check digit (Luhn over letters-expanded-to-digits)."""
    if not _ISIN_RE.match(isin or ""):
        return False
    digits = "".join(str(int(c, 36)) for c in isin[:-1])
    total = 0
    for i, ch in enumerate(reversed(digits)):
        n = int(ch)
        if i % 2 == 0:
            n *= 2
            if n > 9:
                n -= 9
        total += n
    return (10 - total % 10) % 10 == int(isin[-1])


def _parse_date(raw) -> Optional[date]:
    raw = _norm(raw).strip('"').strip("'")
    if raw.upper() in _NA:
        return None
    for fmt in ("%Y-%m-%d", "%d-%m-%Y", "%d/%m/%Y"):
        try:
            return datetime.strptime(raw, fmt).date()
        except ValueError:
            continue
    return None


def _parse_qty(raw) -> Optional[Decimal]:
    """'NA' / empty → 0. Unparseable → None (caller rejects as
    malformed rather than silently treating it as zero)."""
    r = _norm(raw).replace(",", "")
    if r.upper() in _NA:
        return Decimal("0")
    if not _QTY_RE.match(r):
        return None
    try:
        return Decimal(r)
    except InvalidOperation:  # pragma: no cover - regex-guarded
        return None


def _header_map(row) -> Optional[dict]:
    """If `row` is the table's header row, return field -> column
    index; else None."""
    labels = [_norm(c).lower() for c in row]
    if not any(_HEADER_FIELDS["isin"](h) for h in labels if h):
        return None
    cmap = {}
    for idx, h in enumerate(labels):
        if not h:
            continue
        for field, pred in _HEADER_FIELDS.items():
            if field not in cmap and pred(h):
                cmap[field] = idx
    return cmap


def _label_type(purpose: str, txn_type: str) -> Optional[str]:
    """Direction implied by the row's labels, or None if the labels
    carry no direction. Purpose wins; 'payin'/'payout' are the
    broker's settled-section vocabulary."""
    p = purpose.lower().replace(" ", "").replace("-", "")
    if p == "payout":
        return "BUY"
    if p == "payin":
        return "SELL"
    words = set(re.split(r"[^a-z]+", txn_type.lower()))
    if words & {"buy", "purchase"}:
        return "BUY"
    if words & {"sell", "sale"}:
        return "SELL"
    return None


def _merge_continuation(prev: list, frag: list) -> None:
    for i, cell in enumerate(frag):
        if i >= len(prev):
            prev.append(cell)
            continue
        c = _norm(cell)
        if c:
            prev[i] = f"{_norm(prev[i])} {c}".strip()


def extract_rows_from_tables(tables: list, source_file: str) -> tuple:
    """Pure stage: table matrices → (rows, rejections, stats).
    See module docstring for the input shape."""
    base = os.path.basename(source_file)
    rejections = _empty_rejections()
    stats = _empty_stats()

    # Pass 1: flatten to (section, cmap, cells) records, merging
    # page-break continuation fragments into the previous record.
    records: List[list] = []  # [section, cmap, cells]
    cmap = None
    section = None
    last_page = None
    def at(row, field, cm):
        i = cm.get(field) if cm else None
        return row[i] if i is not None and i < len(row) else None

    for tab in tables:
        stats["tables"] += 1
        tab_rows = [list(r) for r in (tab.get("rows") or [])]
        has_header = any(_header_map(r) is not None for r in tab_rows)
        if tab.get("section") is not None:
            section = tab["section"]
        elif has_header:
            # A fresh table (own header row) with no section title
            # above it is NOT a continuation: fail closed rather
            # than inheriting the previous section (its rows are
            # rejected as malformed in pass 2).
            section = None
        new_page = tab.get("page") != last_page
        last_page = tab.get("page")
        first_data_row = True
        for row in tab_rows:
            hm = _header_map(row)
            if hm is not None:
                cmap = hm
                continue
            if not any(_norm(c) for c in row):
                continue  # header sub-row / spacer
            if (cmap is not None and new_page and first_data_row
                    and records and section is not None
                    and records[-1][0] == section
                    and not _norm(at(row, "trade_date", cmap))
                    and not _norm(at(row, "isin", cmap))):
                _merge_continuation(records[-1][2], row)
                first_data_row = False
                continue
            first_data_row = False
            records.append([section, cmap, row])

    # Pass 2: validate + emit.
    rows: List[dict] = []

    def reject(reason: str, idx: int, why: str) -> None:
        rejections[reason] += 1
        _LOG.warning(
            "[ak-4t7f] REJECT %s (%s) at %s row #%d",
            reason, why, base, idx,
        )

    for idx, (sec, cm, cells) in enumerate(records, start=1):
        if cm is None or any(f not in cm for f in _REQUIRED_FIELDS):
            reject("malformed", idx, "table has no recognisable header")
            continue

        def cell(field):
            i = cm.get(field)
            return cells[i] if i is not None and i < len(cells) else None

        isin_raw = _norm(cell("isin")).replace(" ", "")
        qd = _parse_qty(cell("qty_delivered"))
        qr = _parse_qty(cell("qty_received"))

        if _is_na(isin_raw):
            if qd == 0 and qr == 0:
                if isin_raw:  # literal "NA" row, not a blank sub-row
                    stats["zero_activity"] += 1
                continue
            reject("malformed", idx, "quantity without ISIN")
            continue
        if not isin_checksum_ok(isin_raw):
            reject("malformed", idx, "ISIN fails shape/checksum")
            continue
        stats["isin_rows"] += 1

        if sec == SECTION_PENDING:
            stats["pending_skipped"] += 1
            continue
        if sec != SECTION_SETTLED:
            reject("malformed", idx, "row outside a known section")
            continue

        trade_date = _parse_date(cell("trade_date"))
        if trade_date is None:
            reject("malformed", idx, "unparseable transaction date")
            continue
        settlement_date = _parse_date(cell("execution_date"))

        if qd is None or qr is None or qd < 0 or qr < 0:
            reject("malformed", idx, "unparseable/negative quantity")
            continue
        if qd > 0 and qr == 0:
            inferred, quantity = "SELL", qd
        elif qr > 0 and qd == 0:
            inferred, quantity = "BUY", qr
        else:
            reject(
                "qty_ambiguous", idx,
                "qty columns both zero or both non-zero",
            )
            continue
        if quantity > _MAX_QTY:
            reject("malformed", idx, "quantity exceeds column")
            continue

        label = _label_type(_norm(cell("purpose")), _norm(cell("txn_type")))
        if label is not None and label != inferred:
            reject("label_mismatch", idx, "label disagrees with qty")
            continue

        settlement_no = _norm(cell("settlement_no")).replace(" ", "")
        if (not settlement_no or _is_na(settlement_no)
                or not _SETTLEMENT_RE.match(settlement_no)
                or _PAN_RE.match(settlement_no)
                or len(settlement_no) > _MAX_SETTLEMENT):
            # NULL settlement defeats the UNIQUE dedup (NULL ≠ NULL)
            # and the Phase 3b price-fill join; hard-reject.
            reject("missing_settlement", idx, "no usable settlement no")
            continue

        symbol = _norm(cell("symbol"))
        if not symbol or _is_na(symbol) or len(symbol) > _MAX_SYMBOL:
            reject("malformed", idx, "scrip name empty/over-length")
            continue

        segment = _norm(cell("segment"))
        if not segment or _is_na(segment):
            segment = "EQ"
        if len(segment) > _MAX_SEGMENT:
            reject("malformed", idx, "segment over-length")
            continue

        rows.append({
            "trade_date": trade_date,
            "settlement_date": settlement_date,
            "isin": isin_raw,
            "symbol": symbol,
            "quantity": quantity,
            "transaction_type": inferred,
            "segment": segment,
            "settlement_no": settlement_no,
            "price": None,
            "price_source": None,
            "source": _SOURCE_TAG,
            "source_file": base,
        })

    return rows, rejections, stats


def _section_for_table(page, table_y0: float) -> Optional[str]:
    """Nearest section title above the table on this page, or None
    (continuation page → caller inherits)."""
    best = None
    best_y = None
    for b in page.get_text("blocks"):
        x0, y0, x1, y1, text = b[:5]
        if y1 > table_y0 + 1:
            continue
        t = _norm(text)
        for rx, sec in _SECTION_TITLES:
            if rx.search(t) and (best_y is None or y0 > best_y):
                best, best_y = sec, y0
    return best


def count_text_isins(text: str) -> int:
    """Checksum-valid Indian ISIN tokens in raw page text, as an
    independent cross-check on the table extraction."""
    return sum(
        1 for t in _TEXT_ISIN_RE.findall(text or "")
        if isin_checksum_ok(t)
    )


def parse_weekly_securities_pdf(pdf_path: str) -> tuple:
    """Load the PDF, extract its tables, and parse rows.

    Returns (rows, rejections, stats). Besides the
    extract_rows_from_tables keys, `stats` carries:
      * text_isins: checksum-valid ISIN tokens in the page text
      * unaccounted_isins: max(0, text_isins - isin_rows), i.e.
        ISINs that are present in the PDF but never reached a row,
        a rejection, or a pending skip. Any non-zero value is
        logged as a WARN.
    A fitz load failure returns ([], zero rejections, stats with
    load_failed=1) and logs a WARN.
    """
    base = os.path.basename(pdf_path)
    if fitz is None:  # pragma: no cover
        raise RuntimeError(
            "PyMuPDF (fitz) not installed — required for "
            "ak-8kp4 Phase 3a PDF parsing"
        )
    try:
        doc = fitz.open(pdf_path)
    except Exception as exc:
        _LOG.warning(
            "[ak-4t7f] %s: fitz load failed (%s); no rows",
            base, type(exc).__name__,
        )
        stats = _empty_stats()
        stats.update(text_isins=0, unaccounted_isins=0, load_failed=1)
        return [], _empty_rejections(), stats

    tables = []
    text_isins = 0
    try:
        for pno, page in enumerate(doc):
            text_isins += count_text_isins(page.get_text("text"))
            if not hasattr(page, "find_tables"):  # pragma: no cover
                raise RuntimeError(
                    "PyMuPDF >= 1.23 required (Page.find_tables)"
                )
            for t in page.find_tables().tables:
                tables.append({
                    "section": _section_for_table(page, t.bbox[1]),
                    "page": pno,
                    "rows": t.extract(),
                })
    finally:
        doc.close()

    rows, rejections, stats = extract_rows_from_tables(tables, pdf_path)
    stats["text_isins"] = text_isins
    stats["unaccounted_isins"] = max(0, text_isins - stats["isin_rows"])
    stats["load_failed"] = 0
    if stats["unaccounted_isins"]:
        _LOG.warning(
            "[ak-4t7f] %s: %d ISIN(s) in the PDF text did not reach "
            "a row, rejection or pending skip (tables=%d); the "
            "layout may have changed",
            base, stats["unaccounted_isins"], stats["tables"],
        )
    return rows, rejections, stats


__all__ = [
    "parse_weekly_securities_pdf",
    "extract_rows_from_tables",
    "count_text_isins",
    "isin_checksum_ok",
    "SECTION_SETTLED",
    "SECTION_PENDING",
]
