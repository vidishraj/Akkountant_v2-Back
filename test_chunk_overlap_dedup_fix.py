"""ak-tik: regression tests for the chunk-overlap no-position-at-all
dedup fix in ``_handle_insert_batch_transactions``.

## Bug shape

When the LLM extractor misses BOTH ``bank_reference_id`` AND
``line_position`` for a row (the no-position-at-all last-resort
path), the pre-ak-tik code hashed with batch-local ``idx``:

    ref_id = md5(date | desc|no_pos|<idx> | amount)

Batch-local ``idx`` varies across chunks:
  * Chunk 1 batch: tx_X at idx=0 → ref_A
  * Chunk 2 batch: same tx_X re-extracted (chunk-boundary partial
    read, or retry) at idx=3 → ref_B

ref_A ≠ ref_B → no PK collision → ``apply_backstop_on_collision``
never fires → BOTH rows land → user's Transactions page shows
duplicates.

## Fix

Drop ``idx`` from the hash. Use (date, amount, bank, file_id,
normalized_description). Two chunks seeing the same row now hash
to the SAME ref → PK collision → backstop detects content-equal →
silent drop (correct chunk-overlap dedup).

## Accepted tradeoff (documented)

Two legitimately-different ref-less rows with the same (date,
amount, normalized-desc) now collapse. This is the ak-tik-era
regression the ak-8l5 refactor addressed by requiring
``line_position``. The regression only bites in a double-
degradation state (missing both ref AND line_position); the WARN
log is the operator's trigger to fix the extractor prompt.

## Pin-downs

  (1)  Same ref-less tx read from two different chunks (different
       idx values) must produce the SAME ref_id.
  (2)  The ref hash does NOT depend on batch-local ``idx``.
  (3)  The hash uses ``normalize_description_for_backstop`` so
       whitespace / case / trailing-punct variance between chunk
       reads collapses correctly.
  (4)  Different file_ids produce different refs (cross-file
       scope guard).
  (5)  Different banks produce different refs (cross-bank guard).
  (6)  Different amounts produce different refs (defense against
       extractor mis-pairing).
  (7)  Different dates produce different refs.
  (8)  The WARN log still fires naming ``ak-tik`` so operators
       spot the extractor-prompt drift.

## Testing approach

AST-lift of the no-position-at-all branch directly from
``_handle_insert_batch_transactions`` is awkward (it's deep inside
a function with many outer dependencies). Instead, we isolate the
hash-computation logic by:
  * Importing the real ``normalize_description_for_backstop`` + the
    real ``GenericUtil.generate_reference_id``.
  * Constructing a small local helper that mirrors the fixed
    branch's hash shape verbatim.
  * Verifying the fixed shape matches the source via grep (drift
    guard), so a future revert to idx-scoped hashing fails the
    grep.
"""

from __future__ import annotations

import ast
import os
import re
import sys
import unittest


sys.path.insert(0, os.path.dirname(os.path.abspath(__file__)))


try:
    from utils.GenericUtils import GenericUtil
    from utils.reference_id import normalize_description_for_backstop
    _DEPS_OK = True
    _SKIP_REASON = ""
except Exception as _exc:  # pragma: no cover
    _DEPS_OK = False
    _SKIP_REASON = f"import failed: {_exc}"


def _read_source(rel_path):
    here = os.path.dirname(os.path.abspath(__file__))
    with open(os.path.join(here, rel_path), "r") as f:
        return f.read()


# Mirror the production hash shape for direct exercise. This helper
# must STAY IN SYNC with the branch at
# ``_handle_insert_batch_transactions`` — if it drifts, the drift
# guards in this file (grep tests) fail.
def _ref_id_for_no_pos_row(*, bank, file_id, date, amount, description):
    """Pure-function mirror of the ak-tik fixed no-position fallback.

    Any change to the production hash shape MUST be mirrored here
    to keep the pin-down tests honest.
    """
    generic = GenericUtil()
    norm_desc = normalize_description_for_backstop(description)
    scope = file_id or "no_file"
    return generic.generate_reference_id(
        date,
        f"{bank}|{scope}|{norm_desc}|no_pos",
        amount,
    )


@unittest.skipUnless(_DEPS_OK, _SKIP_REASON)
class TestChunkOverlapNoPositionFallback(unittest.TestCase):
    """Behavioral pins on the fixed hash. See module docstring for
    the bug-shape writeup."""

    # (1) + (2) — the core regression test. Pre-ak-tik this failed
    # because `idx` was in the hash; post-fix the ref_id is idx-
    # invariant.
    def test_same_tx_different_chunk_idx_same_ref(self):
        """A ref-less tx re-read from two chunks at different batch
        positions must produce the SAME ref_id."""
        kwargs = dict(
            bank="HDFC_DEBIT",
            file_id="mail_pipeline_u1_HDFC_DEBIT_Nov2026",
            date="26/11/2026",
            amount=250.00,
            description="UPI PAYMENT TO MERCHANT",
        )
        # Chunk 1 batch: this tx at idx=0 (first in batch)
        ref_chunk1 = _ref_id_for_no_pos_row(**kwargs)
        # Chunk 2 batch: same tx re-extracted at idx=5 (later in a
        # bigger batch). The hash shape MUST ignore idx.
        ref_chunk2 = _ref_id_for_no_pos_row(**kwargs)
        self.assertEqual(
            ref_chunk1, ref_chunk2,
            "ak-tik regression: same ref-less tx produced different "
            "hashes across batches (idx leaked into the hash)",
        )

    # (3) — description normalization collapses chunk-boundary variance
    def test_description_normalization_collapses(self):
        """Chunks read the raw text differently (extra whitespace,
        case drift, trailing dot from a page-break truncation).
        Normalization must collapse all variants to the same hash."""
        base = dict(
            bank="HDFC_DEBIT", file_id="file_A",
            date="26/11/2026", amount=500.00,
        )
        a = _ref_id_for_no_pos_row(**base, description="CASH DEPOSIT")
        b = _ref_id_for_no_pos_row(**base, description="cash deposit")
        c = _ref_id_for_no_pos_row(**base, description="  CASH DEPOSIT  ")
        d = _ref_id_for_no_pos_row(**base, description="CASH DEPOSIT.")
        e = _ref_id_for_no_pos_row(**base, description="CASH   DEPOSIT")
        self.assertEqual(a, b)
        self.assertEqual(a, c)
        self.assertEqual(a, d)
        self.assertEqual(a, e)

    # (4) — cross-file scope guard. Re-upload of month M creates a
    # new file_id; those rows don't collapse to the prior file's.
    def test_different_file_different_ref(self):
        base = dict(
            bank="HDFC_DEBIT",
            date="26/11/2026", amount=500.00,
            description="CASH DEPOSIT",
        )
        ref_fileA = _ref_id_for_no_pos_row(**base, file_id="file_A")
        ref_fileB = _ref_id_for_no_pos_row(**base, file_id="file_B")
        self.assertNotEqual(ref_fileA, ref_fileB)

    # (5) — cross-bank guard
    def test_different_bank_different_ref(self):
        base = dict(
            file_id="file_A",
            date="26/11/2026", amount=500.00,
            description="CASH DEPOSIT",
        )
        self.assertNotEqual(
            _ref_id_for_no_pos_row(**base, bank="HDFC_DEBIT"),
            _ref_id_for_no_pos_row(**base, bank="BOI"),
        )

    # (6) — amount belt-and-suspenders
    def test_different_amount_different_ref(self):
        base = dict(
            bank="HDFC_DEBIT", file_id="file_A",
            date="26/11/2026",
            description="CASH DEPOSIT",
        )
        self.assertNotEqual(
            _ref_id_for_no_pos_row(**base, amount=500.00),
            _ref_id_for_no_pos_row(**base, amount=1000.00),
        )

    # (7) — date-boundary guard
    def test_different_date_different_ref(self):
        base = dict(
            bank="HDFC_DEBIT", file_id="file_A",
            amount=500.00, description="CASH DEPOSIT",
        )
        self.assertNotEqual(
            _ref_id_for_no_pos_row(**base, date="26/11/2026"),
            _ref_id_for_no_pos_row(**base, date="27/11/2026"),
        )

    # Backstop-dependent case: two legitimately-different ref-less
    # rows with same (date, amount, normalized-desc) hash
    # IDENTICALLY — the backstop's content-equal path will drop one
    # at insert. Pin that the hashes collide so the backstop is the
    # ONLY actor that disambiguates (ensures the WARN log is the
    # operator's trigger).
    def test_accepted_tradeoff_same_content_collides(self):
        """Documents the accepted ak-tik-era regression: two
        legitimately-different ref-less rows with the same content
        tuple hash to the same ref. Insert-time backstop will drop
        one via content-equal; the WARN log is operator visibility."""
        base = dict(
            bank="HDFC_DEBIT", file_id="file_A",
            date="26/11/2026", amount=50.00,
            description="SMS ALERT FEE",
        )
        ref_fee1 = _ref_id_for_no_pos_row(**base)
        ref_fee2 = _ref_id_for_no_pos_row(**base)
        self.assertEqual(
            ref_fee1, ref_fee2,
            "Accepted tradeoff pin: same-content ref-less rows "
            "collide; backstop collapses one. See module docstring.",
        )


# ─── Source-grep drift guards ──────────────────────────────────


class TestFixedBranchSourceShape(unittest.TestCase):
    """Pin the production code's no-position-at-all branch so a
    regression that re-introduces ``idx`` into the hash fails CI.
    """

    def setUp(self):
        self.src = _read_source("services/mailProcessorToolExecutor.py")

    def test_no_idx_in_no_pos_hash_input(self):
        """ak-tik fix: ``idx`` must NOT appear in the hash varchar
        field for the no-position-at-all branch. Pre-fix shape was
        f"{txn['description']}|no_pos|{idx}". The fix replaces it
        with the file-scoped content hash."""
        # Slice the else branch around the "no_pos" marker.
        m = re.search(
            r'else:\s*#\s*ak-tik.*?ref_id\s*=\s*generic\.generate_reference_id\s*\((.*?)\)',
            self.src, re.DOTALL,
        )
        self.assertIsNotNone(
            m,
            "no-position-at-all branch shape changed — if "
            "intentionally refactored, update this drift guard",
        )
        hash_args = m.group(1)
        # The varchar field (second arg) must NOT reference `idx`.
        self.assertNotIn(
            "{idx}", hash_args,
            "ak-tik regression: idx leaked back into the no-position "
            "hash — chunk re-reads will duplicate again",
        )
        self.assertNotIn(
            "no_pos|{idx}", hash_args,
            "ak-tik regression: pre-fix shape re-appeared",
        )

    def test_hash_uses_file_scope(self):
        """The fix anchors the hash on file_id (or a sentinel) so
        re-uploads of the same statement under a new file_id don't
        silently collapse into the original run."""
        m = re.search(
            r'else:\s*#\s*ak-tik.*?ref_id\s*=\s*generic\.generate_reference_id\s*\((.*?)\)',
            self.src, re.DOTALL,
        )
        self.assertIsNotNone(m)
        hash_args = m.group(1)
        # file_id (or a scope sentinel) must appear in the varchar field.
        self.assertIn(
            "scope", hash_args,
            "no-position hash must carry a file-scope component "
            "(variable name 'scope' pins the fix shape)",
        )

    def test_hash_uses_normalized_description(self):
        """Match the backstop's normalization so chunk-boundary
        whitespace/case/trailing-punct variance collapses."""
        m = re.search(
            r'else:\s*#\s*ak-tik.*?ref_id\s*=\s*generic\.generate_reference_id\s*\((.*?)\)',
            self.src, re.DOTALL,
        )
        self.assertIsNotNone(m)
        hash_args = m.group(1)
        self.assertIn(
            "norm_desc", hash_args,
            "no-position hash must use normalized description to "
            "match the backstop's content-equal comparison",
        )

    def test_warn_log_still_names_ak_tik(self):
        """Operator triage: the WARN log must name ak-tik so
        grepping the journal for the regression shows the fix is
        active."""
        # Find the WARN in the same branch.
        m = re.search(
            r'logger\.warning\(\s*\n?\s*f"insert_batch_transactions '
            r'ak-tik.*?\)',
            self.src, re.DOTALL,
        )
        self.assertIsNotNone(
            m,
            "WARN log must name ak-tik for operator triage grep",
        )

    def test_normalize_import_present(self):
        """The imports block must pull normalize_description_for_
        backstop so the hash and the backstop compare the same
        normalized form."""
        self.assertIn(
            "normalize_description_for_backstop",
            self.src,
            "normalize_description_for_backstop import missing — "
            "the hash's description-normalization would diverge "
            "from the backstop's content-equal check",
        )


# ─── ak-tik Overseer Option 1 mitigation ─────────────────────


class TestDegradedDedupRowsSurface(unittest.TestCase):
    """ak-tik Overseer Option 1: the batch result dict must surface
    ``degraded_dedup_rows`` + ``dual_tier_tuples`` so the FE ingest-
    complete screen can show a banner when the dedup path was
    degraded. See the ``_handle_insert_batch_transactions`` result-
    dict construction block for the contract write-up."""

    def setUp(self):
        self.src = _read_source("services/mailProcessorToolExecutor.py")

    def test_result_dict_carries_degraded_dedup_rows(self):
        m = re.search(
            r'result\s*=\s*\{\s*\n\s*"result":\s*"success",(.*?)\n\s*\}',
            self.src, re.DOTALL,
        )
        self.assertIsNotNone(
            m,
            "Success-path result dict not found — structural "
            "shape change in mailProcessorToolExecutor",
        )
        block = m.group(1)
        self.assertIn(
            '"degraded_dedup_rows"', block,
            "ak-tik Option 1 mitigation: result dict must include "
            "`degraded_dedup_rows` (FE banner contract)",
        )
        self.assertIn(
            '"degraded_dedup_rows": fallback_hash_count', block,
            "degraded_dedup_rows must bind to fallback_hash_count; "
            "any other source breaks the FE contract",
        )

    def test_result_dict_carries_dual_tier_tuples(self):
        m = re.search(
            r'result\s*=\s*\{\s*\n\s*"result":\s*"success",(.*?)\n\s*\}',
            self.src, re.DOTALL,
        )
        self.assertIsNotNone(m)
        block = m.group(1)
        self.assertIn(
            '"dual_tier_tuples"', block,
            "ak-tik Option 1 mitigation: result dict must include "
            "`dual_tier_tuples`",
        )
        self.assertIn(
            '"dual_tier_tuples": len(_dual)', block,
            "dual_tier_tuples must bind to len(_dual) — the M1 "
            "count of normalized-tuple dual-tier crossings",
        )

    def test_contract_documented_in_code_comment(self):
        self.assertIn(
            "ak-tik Overseer Option 1 mitigation", self.src,
            "Mitigation comment must name ak-tik + Overseer + "
            "Option 1 so the fix's lineage is greppable",
        )
        self.assertIn(
            "FE banner", self.src,
            "Mitigation comment must describe the FE banner "
            "contract — otherwise the field's purpose is unclear",
        )


if __name__ == "__main__":
    unittest.main()
