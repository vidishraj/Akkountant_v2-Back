"""ak-4tc regression tests: _content_block_for returns the MCP shape
that claude_agent_sdk's MCP bridge actually reads.

Root-cause recap (Overseer's 2026-08-03 repro): ak-1x4 wired the
read_attachment MCP tool to return Anthropic-API-native content
shapes — `{type: 'image', source: {type: 'base64', media_type, data}}`
for images and `{type: 'document', ...}` for PDFs. But the SDK's
call_tool bridge (claude_agent_sdk/__init__.py::create_sdk_mcp_server,
around lines 301-313) only handles two shapes:

    {type: 'text',  text: <str>}                     -> TextContent
    {type: 'image', data: <b64>, mimeType: <mime>}   -> ImageContent

Anything else — including the Anthropic-native shapes above — is
silently dropped from the tool result content list. The LLM then
receives an empty content block and confabulates from thin air. Live
symptom: user uploaded a BOI Consumption Fund screenshot; agent
'extracted' a fully fabricated HDFC Flexi Cap Fund purchase (every
field wrong). Bug was live ~4 weeks from ak-1x4 landing.

Fix (this commit): _content_block_for now returns the MCP shape the
SDK reads:
  * image/*         -> {type: 'image', data: <b64>, mimeType: <mime>}
  * application/pdf -> {type: 'text',  text: <fitz-extracted text>}
                       (MVP fallback; SDK bridge has no 'document' branch)

Invariants asserted below:
  1. Image branch returns MCP shape (data + mimeType at top level).
  2. Image branch returns NO 'source' nesting (regression on the ak-4tc
     root cause — Anthropic-API-native shape was the bug).
  3. Image branch preserves the MIME (image/png, image/jpeg, etc.).
  4. PDF branch returns {type: 'text', text: <str>}; NOT 'document'.
  5. PDF branch includes an unambiguous header so the LLM knows it's
     looking at PDF text extract (not the user's typed message).
  6. Unsupported MIME raises _UnsupportedAttachmentMime (defense-in-depth
     wall — unreachable via legit uploads that go through save_upload).

The image-shape assertions run without any heavy deps. The PDF branch
needs PyMuPDF (fitz) — we skip cleanly when it's not installed rather
than failing the whole suite.

Run:
    python3 -m unittest test_agent_attachment_shape
    python3 -m pytest test_agent_attachment_shape.py
"""

import base64
import os
import sys
import unittest


sys.path.insert(0, os.path.dirname(os.path.abspath(__file__)))


# services.agentService imports flask etc. Guard the import so a truly
# bare box skips cleanly instead of failing at collection time. When
# flask IS available, we bind the two symbols we exercise directly —
# the top-level module import is the only heavy step; the function
# itself is pure Python.
try:
    from services.agentService import (
        _content_block_for,
        _UnsupportedAttachmentMime,
    )
    _AGENT_SERVICE_AVAILABLE = True
    _SKIP_REASON = ""
except Exception as _exc:  # pragma: no cover — env-dep skip
    _AGENT_SERVICE_AVAILABLE = False
    _SKIP_REASON = f"services.agentService unavailable: {_exc}"


# Minimal valid image bytes for shape assertions. We do NOT need actual
# decoded pixels — _content_block_for base64-encodes whatever bytes it
# receives; the SDK bridge shape check is on the dict, not the payload.
_TINY_PNG_BYTES = (
    b"\x89PNG\r\n\x1a\n"                      # PNG magic
    b"\x00\x00\x00\rIHDR"                    # IHDR chunk header
    b"\x00\x00\x00\x01\x00\x00\x00\x01"      # 1x1
    b"\x08\x06\x00\x00\x00\x1f\x15\xc4\x89"  # bit depth, color type, CRC
    b"\x00\x00\x00\nIDATx\x9cc\x00\x01\x00\x00\x05\x00\x01"
    b"\x0d\n-\xb4"                           # CRC
    b"\x00\x00\x00\x00IEND\xaeB`\x82"        # IEND
)


def _tiny_pdf_bytes():
    """Build a minimal valid single-page PDF with a small text run.

    We construct this in-memory (via fitz) rather than shipping a
    hex blob because that keeps the test hermetic — no external
    fixture files to keep in sync. Skips if fitz isn't installed.
    """
    import fitz  # PyMuPDF
    doc = fitz.open()
    page = doc.new_page()
    # Insert some text so page.get_text() has something to return.
    page.insert_text((72, 72), "AK-4TC-TEST-MARKER Line 1")
    page.insert_text((72, 96), "AK-4TC-TEST-MARKER Line 2")
    pdf_bytes = doc.tobytes()
    doc.close()
    return pdf_bytes


@unittest.skipUnless(_AGENT_SERVICE_AVAILABLE, _SKIP_REASON)
class TestImageBranchShape(unittest.TestCase):
    """ak-4tc P0: image content-block MUST be the MCP shape (top-level
    data + mimeType, no source nesting) so the SDK's ImageContent
    constructor can read it."""

    def test_image_returns_mcp_shape(self):
        print("\n[ak-4tc — image branch returns MCP-shape dict]")
        block = _content_block_for("image/png", _TINY_PNG_BYTES)
        self.assertIsInstance(block, dict)
        self.assertEqual(block.get("type"), "image")
        # MCP shape: TOP-LEVEL 'data' + 'mimeType'.
        self.assertIn("data", block)
        self.assertIn("mimeType", block)
        # Regression on the exact ak-4tc root cause — Anthropic-API-native
        # nested source shape was what the SDK was silently dropping.
        self.assertNotIn(
            "source", block,
            "image block must NOT nest under 'source' — the SDK's MCP "
            "bridge does not read source-nested shapes (see ak-4tc).",
        )
        # media_type is the API-native key; MCP uses mimeType. Both being
        # present would be a smell, and only the MCP one is read.
        self.assertNotIn(
            "media_type", block,
            "image block should use 'mimeType' (MCP), not 'media_type' "
            "(Anthropic-native) at the top level.",
        )
        print("  ✓ shape={type: image, data, mimeType} — no source, no media_type")

    def test_image_data_is_base64_of_input_bytes(self):
        print("\n[ak-4tc — image data is base64(input bytes)]")
        block = _content_block_for("image/png", _TINY_PNG_BYTES)
        decoded = base64.b64decode(block["data"])
        self.assertEqual(decoded, _TINY_PNG_BYTES)
        self.assertEqual(block["mimeType"], "image/png")
        print("  ✓ data round-trips; mimeType preserved")

    def test_image_mime_variants_pass_through(self):
        print("\n[ak-4tc — image/* MIMEs (jpeg, webp, gif) all take the image branch]")
        # We reuse the PNG bytes as opaque payloads — _content_block_for
        # doesn't inspect the payload against the declared MIME (that's
        # save_upload's magic-byte job). Every image/* MIME should hit
        # the same branch and preserve the declared type in mimeType.
        for mime in ("image/jpeg", "image/webp", "image/gif"):
            block = _content_block_for(mime, _TINY_PNG_BYTES)
            self.assertEqual(block["type"], "image")
            self.assertEqual(block["mimeType"], mime)
            self.assertNotIn("source", block)
        print("  ✓ image/jpeg + image/webp + image/gif all → MCP-shape")

    def test_image_block_keys_are_exactly_three(self):
        """Nothing else — no stray 'source', no 'media_type', no
        Anthropic-API leftovers. Locking key-set prevents a partial
        regression that adds `source` back alongside the correct keys."""
        print("\n[ak-4tc — image block key-set is exactly {type, data, mimeType}]")
        block = _content_block_for("image/png", _TINY_PNG_BYTES)
        self.assertEqual(
            set(block.keys()), {"type", "data", "mimeType"},
            f"image block key-set drifted: {set(block.keys())!r}",
        )
        print("  ✓ key-set locked at {type, data, mimeType}")


@unittest.skipUnless(_AGENT_SERVICE_AVAILABLE, _SKIP_REASON)
class TestPdfBranchShape(unittest.TestCase):
    """ak-4tc MVP fallback: PDF → {type: 'text', text: <fitz extract>}.
    The SDK bridge has no 'document' branch, so text-extract is the
    reliable shape today. Vision-native per-page render is a follow-up."""

    @classmethod
    def setUpClass(cls):
        try:
            _tiny_pdf_bytes()  # smoke — will raise ImportError if fitz missing
        except ImportError as e:
            raise unittest.SkipTest(f"PyMuPDF (fitz) required for PDF tests: {e}")
        except Exception as e:  # pragma: no cover — env-dep skip
            raise unittest.SkipTest(f"fitz smoke failed: {e}")

    def test_pdf_returns_text_content_block(self):
        print("\n[ak-4tc — PDF returns {type: text, text: <extract>}]")
        pdf = _tiny_pdf_bytes()
        block = _content_block_for("application/pdf", pdf)
        # Type is 'text' — NOT 'document' (the Anthropic-native shape
        # the SDK's MCP bridge drops on the floor).
        self.assertEqual(block.get("type"), "text")
        self.assertNotEqual(block.get("type"), "document")
        # Text field is present + non-empty.
        self.assertIn("text", block)
        self.assertIsInstance(block["text"], str)
        self.assertGreater(len(block["text"]), 0)
        # No source nesting.
        self.assertNotIn("source", block)
        self.assertNotIn("data", block)
        self.assertNotIn("mimeType", block)
        print("  ✓ shape={type: text, text: <str>} — no document, no source, no data")

    def test_pdf_text_includes_page_marker_and_extracted_text(self):
        print("\n[ak-4tc — PDF extract contains page marker + verbatim text]")
        pdf = _tiny_pdf_bytes()
        block = _content_block_for(
            "application/pdf", pdf, filename="test-marker.pdf",
        )
        text = block["text"]
        # Header prefix so the LLM knows this is PDF text extract, not the
        # user's typed message. Filename is threaded through for debug.
        self.assertIn("[PDF text extract", text)
        self.assertIn("test-marker.pdf", text)
        # Page marker AND the actual extracted text run.
        self.assertIn("--- Page 1 ---", text)
        self.assertIn("AK-4TC-TEST-MARKER", text)
        print("  ✓ header + page marker + extracted text all present")

    def test_pdf_block_key_set_is_exactly_two(self):
        print("\n[ak-4tc — PDF block key-set is exactly {type, text}]")
        pdf = _tiny_pdf_bytes()
        block = _content_block_for("application/pdf", pdf)
        self.assertEqual(
            set(block.keys()), {"type", "text"},
            f"PDF block key-set drifted: {set(block.keys())!r}",
        )
        print("  ✓ key-set locked at {type, text}")


@unittest.skipUnless(_AGENT_SERVICE_AVAILABLE, _SKIP_REASON)
class TestUnsupportedMimeStillRaises(unittest.TestCase):
    """Defense-in-depth: _content_block_for still fails closed on any
    MIME outside the image/* + application/pdf allowlist. Unreachable
    via legit uploads (save_upload magic-byte sniffs + sidecar-persists
    the validated type), but the raise remains as a wall."""

    def test_text_plain_raises(self):
        print("\n[ak-4tc — text/plain still raises _UnsupportedAttachmentMime]")
        with self.assertRaises(_UnsupportedAttachmentMime):
            _content_block_for("text/plain", b"hello")
        print("  ✓ text/plain rejected")

    def test_octet_stream_raises(self):
        print("\n[ak-4tc — application/octet-stream still raises]")
        with self.assertRaises(_UnsupportedAttachmentMime):
            _content_block_for("application/octet-stream", b"\x00\x01\x02")
        print("  ✓ octet-stream rejected (closes the pre-ak-1x4-pass-3 injection channel)")

    def test_empty_content_type_raises(self):
        print("\n[ak-4tc — empty content_type raises]")
        with self.assertRaises(_UnsupportedAttachmentMime):
            _content_block_for("", b"anything")
        with self.assertRaises(_UnsupportedAttachmentMime):
            _content_block_for(None, b"anything")
        print("  ✓ empty/None content_type rejected")


# ── SDK-shape source-of-truth pin ────────────────────────────────────────


class TestSdkImageContentShapeSourceOfTruth(unittest.TestCase):
    """Belt-and-braces: pin the exact SDK code that dictates the shape
    we return. If the SDK ever adds a 'document' branch OR renames the
    keys, this test catches it and forces us to revisit the fallback.

    The SDK ships pinned in requirements.txt; a version bump that
    changes this contract would land as a diff to the vendored code
    and this test would flip red.
    """

    def test_sdk_call_tool_still_reads_data_and_mimeType(self):
        try:
            import claude_agent_sdk  # noqa: F401
        except ImportError:
            self.skipTest("claude_agent_sdk not installed in this env")
        import inspect
        try:
            source = inspect.getsource(
                __import__("claude_agent_sdk", fromlist=["create_sdk_mcp_server"])
                .create_sdk_mcp_server
            )
        except (TypeError, OSError):
            # C-implemented or source unavailable — skip rather than fail.
            self.skipTest("claude_agent_sdk source not introspectable")
        # These are the ~4-line SDK contract we depend on. If any of these
        # substrings goes missing, our shape guarantees are stale.
        for marker in (
            'item.get("type") == "image"',
            'item["data"]',
            'item["mimeType"]',
        ):
            self.assertIn(
                marker, source,
                f"SDK call_tool no longer contains {marker!r} — "
                f"_content_block_for image shape may be stale. "
                f"Re-verify against the current SDK version.",
            )
        # If a 'document' branch appears in the SDK we can revisit the
        # PDF text-extract fallback (return image-per-page or native
        # document instead). Log it as info, don't fail — the fallback
        # keeps working either way.
        if 'item.get("type") == "document"' in source:
            print(
                "\n  ℹ SDK now has a 'document' branch — revisit "
                "_content_block_for PDF fallback for a vision-native path."
            )
        print("\n  ✓ SDK still enforces MCP-shape {data, mimeType} for images")


# ── Runner ───────────────────────────────────────────────────────────────


if __name__ == "__main__":
    print("ak-4tc _content_block_for MCP-shape regression tests")
    print("=" * 70)
    unittest.main(verbosity=0, exit=False)
    print("=" * 70)
    print("Done.")
