"""ak-8l5: reference-aware dedup for statement transactions.

Supersedes ak-tik's content-hash approach (which merged legitimate
same-tuple tx and drew a reviewer MAJOR: silent transaction loss).

The design goal from Overseer: "no transactions whatsoever lost".
Both directions must hold at the same time —
  (1) chunk-overlap dups collapse (the ak-tik / ak-7dz problem)
  (2) legitimate same-(bank, date, amount, description) transactions
      stay distinct (the ak-tik regression that got rejected)

Achieved via a per-transaction bank-native identifier
(`bank_reference_id`) that the extractor pulls out of the narration
row — UPI ref, IMPS ref, NEFT UTR, MBSF number, cheque number, etc.
The dedup primary key uses (bank, bank_reference_id) when the ref is
present; when it's null (some tx rows genuinely have no per-tx
identifier — cash deposits, interest posts, bank fees), we fall back
to a positional hash keyed by (bank, statement_file_id, line_position,
date, amount, normalized description). Position pins the identity to
a specific row in a specific file, so a chunk re-read of the same row
collapses while two null-ref rows on different lines of the same
file stay distinct.

Both hashes are SHA-256 truncated to 64 hex chars to fit the existing
Transactions.referenceID VARCHAR(64) primary-key column. Same input →
same output; pure-Python, no framework deps.

Two entry points:

  generate_reference_v2(bank, bank_reference_id)
      Primary path. Caller must pre-check `bank_reference_id is not
      None` — this function will raise ValueError on a null ref so a
      caller can't accidentally collapse ref-less rows.

  generate_reference_v2_fallback(bank, file_id, line_position,
                                  date_iso, amount, description)
      Positional fallback for ref-less rows. `line_position` is a
      1-indexed offset within the RAW file (NOT the chunk) — the
      extractor must add the chunk's base offset before emitting.

Legacy `generate_stable_reference_id` is retained solely for
backward-compat imports; new callers should NOT use it. The
ak-tik-era hash is documented as deprecated in-place.
"""

import hashlib
import re


# Description normalization — collapse chunk-boundary variance the
# same way the ak-tik fallback did. Used by the fallback path only;
# the primary (ref-based) path doesn't touch description because the
# ref alone is authoritative.
_WS_RE = re.compile(r"\s+")
_TRAILING_PUNCT = ".,:;"

# Ref normalization — banks pad refs with surrounding whitespace or
# trailing dots in some chunk reads. Normalize to a stable form so
# "UPI-1234" and " UPI-1234 " and "UPI-1234." collapse.
_REF_STRIP = " \t\n\r.,:;"


def _normalize_description(description):
    """Same shape ak-tik used — kept intentionally identical to that
    module so a bench-audit query can compare pre/post hashes if
    needed. See ak-tik test suite for the guarantee list."""
    desc = (description or "").strip()
    desc = _WS_RE.sub(" ", desc)
    desc = desc.upper()
    desc = desc.rstrip(_TRAILING_PUNCT)
    return desc


def _normalize_bank(bank):
    """UNKNOWN sentinel + upper. Also the shape callers of the
    ak-tik-era hash used."""
    return (bank or "UNKNOWN").upper()


def _normalize_ref(ref):
    """Ref stripping — kept SEPARATE from _normalize_description so a
    ref like "UPI/415712345678" doesn't accidentally uppercase-eat a
    slash-preserving parse. We ONLY trim surrounding whitespace and
    trailing punctuation; case is preserved so refs whose alphabet
    includes case-sensitive alphanumerics (some UTRs) don't collide.
    """
    return (ref or "").strip(_REF_STRIP)


def _hash(components):
    """SHA-256 → 64 hex chars, truncated to fit Transactions.referenceID's
    VARCHAR(64). We deliberately use sha256 (not md5 like the ak-tik
    era) to future-proof against any collision-driven security concern
    — the ref is user-controlled through the LLM's read of the PDF,
    so a stronger hash is cheap insurance."""
    joined = "|".join(str(c) for c in components)
    digest = hashlib.sha256(joined.encode("utf-8")).hexdigest()
    return digest[:64]


def generate_reference_v2(bank, bank_reference_id):
    """Primary dedup hash for tx with an extracted bank-native ref.

    Contract:
      - bank_reference_id MUST be a non-empty string. A null / empty
        ref means the extractor didn't find one; caller must use
        `generate_reference_v2_fallback` for those rows. Raising
        makes accidental "collapse-all-null-refs" impossible.
      - bank is folded in as a scope prefix — same UPI ref number
        under HDFC vs BOI won't collide (paranoid but cheap).

    Example collisions:
      HDFC + "UPI-1234"        → collapses re-read of same UPI tx
      BOI + "MBSF/443710..."   → collapses re-read of same MBSF tx

    Example distinctness:
      HDFC + "UPI-1234"        distinct from HDFC + "UPI-5678"
                                (two different UPI tx same day, same
                                 amount, same merchant)
      HDFC + "UPI-1234"        distinct from BOI + "UPI-1234"
                                (cross-bank collision guard)
    """
    ref = _normalize_ref(bank_reference_id)
    if not ref:
        raise ValueError(
            "generate_reference_v2: bank_reference_id is empty. Use "
            "generate_reference_v2_fallback for tx rows without an "
            "extracted reference."
        )
    return _hash([_normalize_bank(bank), ref])


def generate_reference_v2_fallback(
    bank, file_id, line_position, date_iso, amount, description,
):
    """Positional-fallback hash for tx rows without an extracted ref.

    line_position is 1-indexed within the RAW file. If chunking is
    used, the extractor MUST add the chunk's base line offset before
    emitting so the position is stable across chunks — the whole
    point of this fallback is to make a chunk re-read of the SAME
    row on line L collapse to the same key.

    file_id scopes to a single statement file; two null-ref rows on
    the same line-position in DIFFERENT files stay distinct
    (statement re-parse of month M produces different file_id than
    the original ingest, so those rows deliberately don't collapse —
    the caller is responsible for wiping old rows before re-parse).

    (date, amount, description) are folded in as belt-and-suspenders
    defense against a positional-only regression: a schema change
    that shifted line numbers by one would otherwise silently
    collapse different rows.

    Description is normalized (whitespace-collapse + upper + trailing-
    punct-strip) to swallow LLM chunk-boundary variance in the
    narration text, matching ak-tik's shape.
    """
    if not file_id:
        # Positional fallback requires file scope. A tx without a
        # file_id came from the email-alert path — that path doesn't
        # need positional fallback (there's exactly one row per email
        # and behavioral dedup at the caller handles duplicates).
        raise ValueError(
            "generate_reference_v2_fallback: file_id is required. The "
            "email-alert path should use the email dedup at the caller "
            "before calling this."
        )
    try:
        line_int = int(line_position)
    except (TypeError, ValueError):
        raise ValueError(
            f"generate_reference_v2_fallback: line_position must be "
            f"convertible to int, got {line_position!r}"
        )
    if line_int < 1:
        raise ValueError(
            f"generate_reference_v2_fallback: line_position must be "
            f"1-indexed and positive, got {line_int}"
        )
    amount_str = f"{float(amount):.2f}"
    return _hash([
        _normalize_bank(bank),
        file_id,
        str(line_int),
        str(date_iso or ""),
        amount_str,
        _normalize_description(description),
    ])


def generate_reference_from_row(
    bank, row, file_id=None, line_position=None,
):
    """Convenience: pick the right hash based on whether `row` carries
    an extracted `bank_reference_id`. Kept as a single entry point so
    callers don't accidentally forget the "if ref present → primary,
    else fallback" branch.

    `row` is a dict with (at minimum): date, amount, description, and
    optionally bank_reference_id.

    Returns the referenceID string. Raises ValueError on illegal
    combos (fallback path needs file_id + line_position).
    """
    ref = _normalize_ref(row.get("bank_reference_id"))
    if ref:
        return generate_reference_v2(bank, ref)
    return generate_reference_v2_fallback(
        bank,
        file_id,
        line_position,
        row.get("date"),
        row.get("amount"),
        row.get("description"),
    )


# ── Deprecated (ak-tik era) ──────────────────────────────────────────
#
# generate_stable_reference_id is retained solely for import
# backward-compat during the ak-8l5 rollout window. New callers
# should use generate_reference_v2 / generate_reference_v2_fallback.
# The ak-tik hash silently merges legitimate same-tuple tx and is
# the reason ak-8l5 exists at all.
#
# DO NOT USE FOR NEW WORK.


def generate_stable_reference_id(bank, datetime_str, description, amount):
    """[DEPRECATED — ak-tik / superseded by ak-8l5] Merge-risky hash.

    Kept only so pre-ak-8l5 imports don't break during the transition
    window. Prefer generate_reference_v2 (primary path) or
    generate_reference_v2_fallback (positional fallback).

    See docstring header for the full rationale.
    """
    combined = f"{_normalize_bank(bank)}|{datetime_str}|" \
               f"{_normalize_description(description)}|" \
               f"{float(amount):.2f}"
    return hashlib.md5(combined.encode("utf-8")).hexdigest()
