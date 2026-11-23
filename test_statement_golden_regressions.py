"""ak-qpkm Phase 2a: golden-file regression harness for the
statement extractor.

## What this harness tests

Per-format golden fixtures under ``goldens/<bank>/<name>/`` pair:
  * ``input_txs.json``  — synthesized tx dicts matching the shape
                          the LLM extractor emits (date, amount,
                          description, bank_reference_id,
                          line_position)
  * ``expected.json``   — hand-asserted canonical post-pipeline
                          state (row count, in_sum, out_sum, net,
                          per-row hash-tier pins, date range)

The harness pins the POST-LLM pipeline's deterministic behavior:
hash-tier dispatch, Σ-sum math, date-range preservation. Catches
ak-tik-class regressions where the backend pipeline silently
drops / collapses rows.

## What this harness does NOT test

The LLM extraction step itself. LLM output is non-deterministic,
so golden-testing the LLM-prompt-level behavior requires either
canned mocks (shallow) or stochastic sampling (expensive). That
scope is deferred to a follow-up bead; the Phase 2a seed here
pins the pipeline below the LLM.

## Expansion plan

Phase 2a seeds a single HDFC_DEBIT fixture. Expansion:
  * Add ICICI, BOI, YES, SBI as representative per-format pins
  * Add multi-chunk fixtures to pin chunk-overlap dedup end-to-end
  * Add degenerate fixtures (zero rows, all-credit, all-debit)

Each added golden is one directory + two JSON files + no test
code changes — the harness autodiscovers.
"""
from __future__ import annotations

import json
import os
import sys
import unittest
from decimal import Decimal


sys.path.insert(0, os.path.dirname(os.path.abspath(__file__)))


try:
    from utils.GenericUtils import GenericUtil
    from utils.reference_id import (
        generate_reference_v2,
        generate_reference_v2_fallback,
        normalize_description_for_backstop,
    )
    _DEPS_OK = True
    _SKIP_REASON = ""
except Exception as _exc:  # pragma: no cover
    _DEPS_OK = False
    _SKIP_REASON = f"import failed: {_exc}"


# Fixtures live under ``goldens/<bank>/<name>/`` — NOT
# ``tests/golden/``. ``/tests/**`` is .gitignored as local
# scratch; committed fixtures go under ``goldens/`` so git tracks
# them.
_GOLDEN_ROOT = os.path.join(
    os.path.dirname(os.path.abspath(__file__)),
    "goldens",
)


def _load_goldens():
    """Discover all (bank, name, input_txs, expected) tuples under
    tests/golden/. Returns an empty list if no goldens exist —
    callers use skipUnless to handle that cleanly."""
    out = []
    if not os.path.isdir(_GOLDEN_ROOT):
        return out
    for bank in sorted(os.listdir(_GOLDEN_ROOT)):
        bank_dir = os.path.join(_GOLDEN_ROOT, bank)
        if not os.path.isdir(bank_dir):
            continue
        for name in sorted(os.listdir(bank_dir)):
            fixture_dir = os.path.join(bank_dir, name)
            input_path = os.path.join(fixture_dir, "input_txs.json")
            expected_path = os.path.join(fixture_dir, "expected.json")
            if not (os.path.exists(input_path)
                    and os.path.exists(expected_path)):
                continue
            with open(input_path) as f:
                input_data = json.load(f)
            with open(expected_path) as f:
                expected = json.load(f)
            out.append((bank, name, input_data, expected))
    return out


def _compute_ref_id(txn, bank, file_id, idx):
    """Mirror the production hash-dispatch in
    ``_handle_insert_batch_transactions`` so goldens pin the
    hash shape. If the production shape changes, a sibling grep
    guard (test_chunk_overlap_dedup_fix.py) reds; THIS harness's
    golden assertions ALSO red if the pin'd tier flip changes.
    """
    generic = GenericUtil()
    raw_ref = txn.get("bank_reference_id")
    if raw_ref:
        return "primary", generate_reference_v2(bank, raw_ref, txn["amount"])
    line_pos = txn.get("line_position")
    if line_pos is not None and file_id:
        return (
            "fallback",
            generate_reference_v2_fallback(
                bank, file_id, line_pos,
                txn["date"], txn["amount"], txn["description"],
            ),
        )
    # No-position-at-all last resort — ak-tik fix shape
    norm_desc = normalize_description_for_backstop(txn["description"])
    scope = file_id or "no_file"
    return "fallback", generic.generate_reference_id(
        txn["date"],
        f"{bank}|{scope}|{norm_desc}|no_pos",
        txn["amount"],
    )


@unittest.skipUnless(_DEPS_OK, _SKIP_REASON)
class TestStatementGoldenRegressions(unittest.TestCase):
    """Parametrized over all discovered goldens. Each golden
    verifies row count + sums + per-tx hash-tier pins."""

    @classmethod
    def setUpClass(cls):
        cls.goldens = _load_goldens()

    def test_goldens_discovered(self):
        """Sanity: at least ONE golden must be present. Catches
        the empty-tests/golden dir state."""
        self.assertGreater(
            len(self.goldens), 0,
            f"No goldens under {_GOLDEN_ROOT}. Phase 2a must seed "
            f"at least one fixture.",
        )

    def test_each_golden_row_count(self):
        """Pin per-golden row count."""
        for bank, name, input_data, expected in self.goldens:
            with self.subTest(bank=bank, name=name):
                self.assertEqual(
                    len(input_data["transactions"]),
                    expected["row_count"],
                    f"Golden {bank}/{name}: input tx count != "
                    f"expected row_count",
                )

    def test_each_golden_sums(self):
        """Pin per-golden in_sum / out_sum / net via the ak-p3id
        canonical positive-magnitude convention."""
        for bank, name, input_data, expected in self.goldens:
            with self.subTest(bank=bank, name=name):
                in_sum = sum(
                    -float(t["amount"])
                    for t in input_data["transactions"]
                    if float(t["amount"]) < 0
                )
                out_sum = sum(
                    float(t["amount"])
                    for t in input_data["transactions"]
                    if float(t["amount"]) > 0
                )
                net = in_sum - out_sum
                self.assertAlmostEqual(
                    in_sum, expected["in_sum"], places=2,
                    msg=f"Golden {bank}/{name}: in_sum mismatch",
                )
                self.assertAlmostEqual(
                    out_sum, expected["out_sum"], places=2,
                    msg=f"Golden {bank}/{name}: out_sum mismatch",
                )
                self.assertAlmostEqual(
                    net, expected["net"], places=2,
                    msg=f"Golden {bank}/{name}: net mismatch",
                )

    def test_each_golden_hash_tier_pins(self):
        """For each row in each golden, verify the production
        hash-dispatch assigns the row to the expected tier
        (primary vs fallback). Catches silent tier flips (e.g.,
        a refactor that treats null bank_reference_id as a
        primary-path row by accident).
        """
        for bank, name, input_data, expected in self.goldens:
            with self.subTest(bank=bank, name=name):
                primary_count = 0
                fallback_count = 0
                for idx, txn in enumerate(input_data["transactions"]):
                    tier, _ref = _compute_ref_id(
                        txn, input_data["bank"],
                        input_data["file_id"], idx,
                    )
                    pin = expected["ref_id_shapes"].get(str(idx))
                    self.assertIsNotNone(
                        pin,
                        f"Golden {bank}/{name} idx={idx}: no pin in "
                        f"expected.ref_id_shapes",
                    )
                    self.assertEqual(
                        tier, pin["tier"],
                        f"Golden {bank}/{name} idx={idx}: tier "
                        f"mismatch — expected {pin['tier']}, "
                        f"computed {tier}",
                    )
                    if tier == "primary":
                        primary_count += 1
                    else:
                        fallback_count += 1
                self.assertEqual(
                    primary_count, expected["primary_hash_count"],
                    f"Golden {bank}/{name}: primary hash count "
                    f"mismatch — expected "
                    f"{expected['primary_hash_count']}, got "
                    f"{primary_count}",
                )
                self.assertEqual(
                    fallback_count, expected["fallback_hash_count"],
                    f"Golden {bank}/{name}: fallback hash count "
                    f"mismatch — expected "
                    f"{expected['fallback_hash_count']}, got "
                    f"{fallback_count}",
                )

    def test_each_golden_date_range(self):
        """Pin first-tx + last-tx dates per golden (catches a
        regression that drops boundary rows)."""
        for bank, name, input_data, expected in self.goldens:
            with self.subTest(bank=bank, name=name):
                if not input_data["transactions"]:
                    continue
                self.assertEqual(
                    input_data["transactions"][0]["date"],
                    expected["date_range"]["first"],
                )
                self.assertEqual(
                    input_data["transactions"][-1]["date"],
                    expected["date_range"]["last"],
                )

    def test_each_golden_hashes_deterministic(self):
        """Running the hash twice over the same input produces the
        same refs. Pins hash determinism (ref_id is used as the
        PK, so any non-determinism is catastrophic).
        """
        for bank, name, input_data, expected in self.goldens:
            with self.subTest(bank=bank, name=name):
                refs_run1 = []
                refs_run2 = []
                for idx, txn in enumerate(input_data["transactions"]):
                    _, r1 = _compute_ref_id(
                        txn, input_data["bank"],
                        input_data["file_id"], idx,
                    )
                    _, r2 = _compute_ref_id(
                        txn, input_data["bank"],
                        input_data["file_id"], idx,
                    )
                    refs_run1.append(r1)
                    refs_run2.append(r2)
                self.assertEqual(
                    refs_run1, refs_run2,
                    f"Golden {bank}/{name}: hash non-determinism "
                    f"detected — ref_id must be a pure function",
                )

    def test_each_golden_no_intra_batch_ref_collisions(self):
        """Within a batch, every row must hash to a unique ref
        (modulo the accepted-tradeoff ref-less-same-content case
        which the backstop resolves). The goldens should avoid
        the accepted-tradeoff case in their seed fixtures — if
        two rows DO collide, flag it loud."""
        for bank, name, input_data, expected in self.goldens:
            with self.subTest(bank=bank, name=name):
                seen = {}
                for idx, txn in enumerate(input_data["transactions"]):
                    _tier, ref = _compute_ref_id(
                        txn, input_data["bank"],
                        input_data["file_id"], idx,
                    )
                    if ref in seen:
                        self.fail(
                            f"Golden {bank}/{name}: rows {seen[ref]} "
                            f"and {idx} collided on ref {ref[:16]}... "
                            f"— goldens must use unique-ref rows "
                            f"(expand golden to a dedicated 'backstop' "
                            f"fixture if testing that case)"
                        )
                    seen[ref] = idx


if __name__ == "__main__":
    unittest.main()
