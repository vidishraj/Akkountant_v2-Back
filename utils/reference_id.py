"""ak-tik: stable content-hash for statement transactions.

Pure-Python (no flask / no SQL) so it can be imported + tested from any
context without pulling in the whole app stack.

Wrapped by `GenericUtil.generate_stable_reference_id` for callers that
already have a GenericUtil instance; standalone tests import this
module directly.
"""

import hashlib
import re


# Description normalization: collapse runs of whitespace to a single
# space so a wrapped narration in chunk N and a single-line narration
# in chunk N+1 collapse to the same canonical form.
_WS_RE = re.compile(r"\s+")

# Trailing punctuation strip: banks pad narrations with trailing
# dots/commas/colons/semicolons that vary by chunk visibility
# ("... UPI/rent" vs "... UPI/rent.").
_TRAILING_PUNCT = ".,:;"


def generate_stable_reference_id(bank, datetime_str, description, amount):
    """Bank-scoped, chunk-position-stable content hash for statement tx.

    ak-tik fix (hq-wisp-1trny5): the original `generate_reference_id`
    composed `date | description | amount` — good when the extractor
    emits identical `description` text for the same tx across re-reads.
    The LLM PDF extractor processes statement PDFs page-by-page: a
    transaction that visually straddles a page boundary gets emitted
    TWICE, with slightly different `description` text or with a
    `reference_number` extractable from one chunk but not the other →
    different MD5 → different `referenceID` PK → both rows land.
    Footprint of the pre-fix bug: 836 excess BOI rows (ak-7dz cluster).

    This helper stabilizes the hash by:

      1. Normalizing description before hashing:
         - trim surrounding whitespace
         - collapse internal whitespace runs to single space
         - uppercase (case-only chunk variance collapses)
         - strip trailing punctuation
      2. Including `bank` in the hash — defense-in-depth against
         cross-bank collisions on same-date-same-amount tuples.
      3. NOT including `reference_number` — that field is often
         extractable from one chunk but not the other, so including it
         would REINTRODUCE the exact chunk-position variance we're
         defeating. `reference_number` remains a persisted column for
         user-visible dedup lookups; it just doesn't participate in
         the identity hash.

    Amount canonicalization matches the legacy `generate_reference_id`
    scheme (float → f"{v:.2f}") so historic-input hashes can be
    round-tripped for comparison if audit ever needs it.
    """
    # Description normalization order matters — strip → collapse → uppercase.
    desc = (description or "").strip()
    desc = _WS_RE.sub(" ", desc)
    desc = desc.upper()
    desc = desc.rstrip(_TRAILING_PUNCT)

    amount_val = float(amount)
    amount_str = f"{amount_val:.2f}"

    bank_str = (bank or "UNKNOWN").upper()

    # Pipe delimiter matches the historic scheme so audit queries can
    # inspect components easily.
    combined = f"{bank_str}|{datetime_str}|{desc}|{amount_str}"
    return hashlib.md5(combined.encode()).hexdigest()
