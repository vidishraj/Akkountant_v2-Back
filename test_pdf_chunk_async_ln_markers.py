"""ak-u949 (ak-tik Option 4 structural cure): regression tests for
the `_run_pdf_chunk_async` per-call prompt restating the ak-8l5
dedup-field schema in BOTH the text-embedding branch (defensive;
unreachable today) AND the image-mode branches (the active
degraded_dedup_rows generator).

## Bug shape addressed by the parent fix

Pre-ak-u949 the `_run_pdf_chunk_async` per-call prompts (text
embedding + image single + image chunked) did NOT mention
`bank_reference_id` or `line_position` in their schema
instructions. PDF_SYSTEM_PROMPT does carry the schema, but a
belt-and-suspenders per-call restatement ensures a future
system-prompt refactor doesn't quietly regress the structural
cure.

## Tests

Shape (source-grep) drift guards:
  (1)  The ak-8l5 schema instruction block is referenced in the
       text-embedding branch (`if processing_mode == "text":`)
  (2)  The ak-8l5 schema instruction block is referenced in the
       image-mode single-chunk branch
  (3)  The ak-8l5 schema instruction block is referenced in the
       image-mode chunked branch
  (4)  The schema instruction block itself names both
       `bank_reference_id` and `line_position`
  (5)  The text-embedding branch adds `[Ln]` annotations
       (parallel to `_run_text_chunk`)
  (6)  The text-embedding branch computes `page_start_line` for
       file-level offset stability across chunks
  (7)  _run_text_chunk (the already-fixed reference impl) also
       references bank_reference_id + line_position — used as
       parity pin to catch divergence between the two paths
  (8)  Transaction-format instruction in the text branch
       explicitly includes bank_reference_id + line_position
       in the shape it tells the LLM to emit
  (9)  Pure-image / OCR PDFs are documented as an accepted-
       limitation in-code (comments pinning the ak-tik Option
       1 banner as the user-facing signal)
"""

from __future__ import annotations

import ast
import os
import re
import sys
import unittest


sys.path.insert(0, os.path.dirname(os.path.abspath(__file__)))


def _read_source(rel_path):
    here = os.path.dirname(os.path.abspath(__file__))
    with open(os.path.join(here, rel_path), "r") as f:
        return f.read()


def _extract_balanced_paren_block(source, after_marker):
    """Given a source string and a marker like `varname = (`, return
    the paren-balanced block content (without the surrounding parens).

    The schema instruction block contains `(` / `)` INSIDE its
    f-strings — a naive non-greedy regex terminates at the first
    inner close-paren. This helper counts depth to find the TRUE
    closing paren.
    """
    start = source.find(after_marker)
    if start < 0:
        return None
    # Position of the open-paren itself
    open_pos = start + len(after_marker) - 1
    depth = 1
    i = open_pos + 1
    while i < len(source):
        c = source[i]
        if c == "(":
            depth += 1
        elif c == ")":
            depth -= 1
            if depth == 0:
                return source[open_pos + 1:i]
        i += 1
    return None


class TestPdfChunkAsyncSchemaRestatement(unittest.TestCase):
    """Source-grep pins on `_run_pdf_chunk_async`'s per-call
    prompt shape. Catches a regression that drops the ak-8l5
    schema instructions from any of the three branches."""

    def setUp(self):
        self.src = _read_source("services/mailProcessorService.py")
        # Slice the full method body
        m = re.search(
            r"async def _run_pdf_chunk_async\(.*?(?=\n    async def |\n    def [a-zA-Z_])",
            self.src, re.DOTALL,
        )
        self.assertIsNotNone(m, "_run_pdf_chunk_async body not found")
        self.body = m.group(0)

    # (1) Text-embedding branch references the schema instructions
    def test_text_branch_includes_schema_instructions(self):
        # The schema instruction block constant name must appear in
        # the text-mode branch (the `if processing_mode == "text":`
        # block of _run_pdf_chunk_async).
        text_branch = re.search(
            r'if processing_mode == "text":(.*?)elif page_start == 1',
            self.body, re.DOTALL,
        )
        self.assertIsNotNone(text_branch, "text-mode branch not found")
        self.assertIn(
            "_ak8l5_schema_instructions",
            text_branch.group(1),
            "ak-u949: text-mode branch must reference "
            "_ak8l5_schema_instructions so the LLM sees the "
            "dedup-field schema in-prompt",
        )

    # (2) Image-mode single-chunk branch references the schema
    def test_image_single_branch_includes_schema_instructions(self):
        single = re.search(
            r'elif page_start == 1 and page_end == total_pages:(.*?)else:',
            self.body, re.DOTALL,
        )
        self.assertIsNotNone(single, "image single-chunk branch not found")
        self.assertIn(
            "_ak8l5_schema_instructions",
            single.group(1),
            "ak-u949: image-mode single-chunk branch must "
            "reference _ak8l5_schema_instructions",
        )

    # (3) Image-mode chunked branch references the schema
    def test_image_chunked_branch_includes_schema_instructions(self):
        # Slice from the last `else:` of the mode dispatch forward
        chunked = re.search(
            r'else:\s*#\s*ak-u949 image-mode chunked(.*?)(?=\n        # Health probe|\n        _sp_img)',
            self.body, re.DOTALL,
        )
        self.assertIsNotNone(
            chunked,
            "image-mode chunked branch (annotated with ak-u949 "
            "comment) not found — likely a refactor renamed the "
            "branch; update this test's regex if intentional",
        )
        self.assertIn(
            "_ak8l5_schema_instructions",
            chunked.group(1),
            "ak-u949: image-mode chunked branch must reference "
            "_ak8l5_schema_instructions",
        )

    # (4) The schema instruction block itself names both fields.
    # Use a paren-balanced slice because the block itself contains
    # parentheses inside f-strings; a naive non-greedy regex
    # terminates at the first inner `)`.
    def test_schema_block_names_both_fields(self):
        block = _extract_balanced_paren_block(
            self.body, "_ak8l5_schema_instructions = (",
        )
        self.assertIsNotNone(
            block, "_ak8l5_schema_instructions block not found",
        )
        self.assertIn(
            "bank_reference_id", block,
            "Schema block must name bank_reference_id",
        )
        self.assertIn(
            "line_position", block,
            "Schema block must name line_position",
        )

    # (5) Text-embedding branch adds `[Ln]` annotations
    def test_text_branch_adds_ln_annotations(self):
        text_branch = re.search(
            r'if processing_mode == "text":(.*?)elif page_start == 1',
            self.body, re.DOTALL,
        )
        self.assertIsNotNone(text_branch)
        body = text_branch.group(1)
        # The annotation line must match the text-chunk shape
        self.assertTrue(
            re.search(r"\[L\{file_line\}\]", body),
            "ak-u949: text-embedding branch must annotate lines "
            "with [L<file_line>] markers (parallel to "
            "_run_text_chunk L2418)",
        )

    # (6) page_start_line computation present
    def test_text_branch_computes_page_start_line(self):
        text_branch = re.search(
            r'if processing_mode == "text":(.*?)elif page_start == 1',
            self.body, re.DOTALL,
        )
        self.assertIsNotNone(text_branch)
        body = text_branch.group(1)
        self.assertIn(
            "page_start_line", body,
            "File-level offset computation missing — line_position "
            "won't be stable across chunks without this",
        )
        # The +1 for PyMuPDF trailing newline — matches _run_text_chunk
        self.assertTrue(
            re.search(r"count\(.\\n.\)\s*\+\s*1", body),
            "page_start_line increment must use count('\\n') + 1 "
            "(matches _run_text_chunk at L2299)",
        )

    # (7) Parity: _run_text_chunk (the reference impl) still
    # references both fields — catches a regression there.
    def test_run_text_chunk_still_references_both_fields(self):
        text_chunk_body = re.search(
            r"async def _run_text_chunk\(.*?(?=\n    async def |\n    def [a-zA-Z_])",
            self.src, re.DOTALL,
        )
        self.assertIsNotNone(text_chunk_body)
        body = text_chunk_body.group(0)
        self.assertIn(
            "bank_reference_id", body,
            "Parity regression: _run_text_chunk must still "
            "reference bank_reference_id in its prompt",
        )
        self.assertIn(
            "line_position", body,
            "Parity regression: _run_text_chunk must still "
            "reference line_position in its prompt",
        )

    # (8) Transaction-format schema example in text branch carries
    # the two fields — not just the instruction block.
    def test_text_branch_tx_format_includes_dedup_fields(self):
        text_branch = re.search(
            r'if processing_mode == "text":(.*?)elif page_start == 1',
            self.body, re.DOTALL,
        )
        self.assertIsNotNone(text_branch)
        body = text_branch.group(1)
        # The "Format each transaction as {...}" example must show
        # the fields explicitly so the LLM has an unambiguous pattern.
        format_example = re.search(
            r'Format each transaction as\s*\{(.+?)\}',
            body, re.DOTALL,
        )
        self.assertIsNotNone(
            format_example,
            "text-branch tx-format example not found",
        )
        fmt = format_example.group(1)
        self.assertIn(
            "bank_reference_id", fmt,
            "Transaction-format example must include "
            "bank_reference_id as a field so LLM emits it",
        )
        self.assertIn(
            "line_position", fmt,
            "Transaction-format example must include "
            "line_position as a field",
        )

    # (9) Pure-image / OCR accepted-limitation documented in-code
    def test_pure_image_accepted_limitation_documented(self):
        """Operator triage: a code comment must explain that pure-
        image PDFs fall to Option 1 banner. If that comment goes
        missing, a future maintainer could misinterpret the
        degraded_dedup_rows metric as a bug rather than an
        accepted-limitation signal.
        """
        # The comment should name both the accepted-limitation
        # shape AND the Option 1 banner reference.
        self.assertIn(
            "pure-image", self.body.lower(),
            "Accepted-limitation comment must name 'pure-image' "
            "PDFs so future maintainers find the rationale",
        )
        self.assertIn(
            "Option 1", self.body,
            "Accepted-limitation comment must name the ak-tik "
            "Option 1 mitigation as the user-facing signal",
        )


# ─── Shape-level drift guards on the shared instruction block ──


class TestSchemaInstructionBlockShape(unittest.TestCase):
    """Pins on the `_ak8l5_schema_instructions` constant's content
    so a future edit that drops required guidance is caught."""

    def setUp(self):
        self.src = _read_source("services/mailProcessorService.py")
        self.block = _extract_balanced_paren_block(
            self.src, "_ak8l5_schema_instructions = (",
        )
        self.assertIsNotNone(self.block)

    def test_names_null_sentinel(self):
        """The block must tell the LLM to return null for ref-less
        rows (not guess) — catches a prompt-tightening regression
        that would push the LLM to invent refs."""
        self.assertIn(
            "null",
            self.block.lower(),
            "Schema block must tell the LLM to return null for "
            "ref-less rows",
        )

    def test_names_chunk_boundary_stability(self):
        """Position must be stable across chunks — the block must
        say so."""
        self.assertIn(
            "chunk",
            self.block.lower(),
            "Schema block must explain chunk-boundary stability",
        )

    def test_names_both_hash_paths(self):
        """The block should explain that BOTH hash paths (primary
        when ref present, positional when ref null) depend on
        correct extraction — gives the LLM context for WHY it
        matters."""
        self.assertIn(
            "positional", self.block.lower(),
            "Schema block should name the positional-fallback hash "
            "path so the LLM understands the stakes",
        )


if __name__ == "__main__":
    unittest.main()
