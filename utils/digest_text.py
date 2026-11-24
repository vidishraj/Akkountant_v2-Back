"""ak-b7vn (2026-11-24): keep agent preamble out of the Wealth Digest body.

The digest `text` must START with the mandatory personal-use header. The
model sometimes writes a lead-in first ("I'll research the holdings…"),
and the task used to PREPEND the header whenever the text didn't start
with it, so the lead-in survived between two headers and showed up as
the summary on Home and /wealth-digest.

The header is guaranteed content, so it is the anchor. A SHORT
single-paragraph lead-in before the model's own header is dropped, as is
the pre-fix "header, lead-in, header" sandwich the old prepend produced.
Anything else (no header, a header repeated later in a disclaimer or a
quote, a long prefix) is returned UNCHANGED: this also runs at read time
on stored digests, so it must never drop real content.

Leaf module (no app imports): used by the write side
(services/tasks/WealthDigestTask.py) and the read side
(services/wealthDigestService.parse_digest_content, which also cleans
digests already stored with a preamble).
"""
from __future__ import annotations

DIGEST_HEADER = "[Personal use — not investment advice]"

# A lead-in is short prose: at most this many chars, one paragraph,
# and never the bulk of the text. Measured prod lead-in: 152 chars,
# one line (msg 285).
MAX_LEAD_IN_CHARS = 400


def _is_lead_in(span: str, total_len: int) -> bool:
    core = span.strip()
    return (
        len(span) <= MAX_LEAD_IN_CHARS
        and "\n\n" not in core          # single paragraph
        and len(span) * 2 < total_len    # not the bulk of the text
    )


def _anchor(text) -> int:
    """Index where the digest really starts: 0 means unchanged, -1 means
    no header at all.

    Only the two KNOWN leak shapes move the anchor, and only when the
    span being dropped looks like a lead-in (see _is_lead_in):
      * `<lead-in> HEADER <body>`: the header is preceded by a short
        single-paragraph prefix (prod msg 285: 152 chars, one line);
      * `HEADER <lead-in> HEADER <body>`: the pre-fix sandwich (the old
        code prepended a header when the text didn't start with one).
    Anything else (a header repeated in a trailing disclaimer, a quoted
    header, a long prefix) leaves the text UNCHANGED. This runs at READ
    time too, so it must fail safe: never drop real content.
    """
    i = text.find(DIGEST_HEADER)
    if i < 0:
        return -1
    if i > 0:
        return i if _is_lead_in(text[:i], len(text)) else 0
    j = text.find(DIGEST_HEADER, len(DIGEST_HEADER))
    if j > 0 and _is_lead_in(text[len(DIGEST_HEADER):j], len(text)):
        return j
    return 0


def strip_digest_preamble(text):
    """Return `text` starting at the model's own DIGEST_HEADER (see
    `_anchor`). Unchanged if the header is absent or `text` is
    empty/not a string."""
    if not isinstance(text, str) or not text:
        return text
    i = _anchor(text)
    if i <= 0:
        return text
    return text[i:]


def preamble_len(text) -> int:
    """Characters strip_digest_preamble would drop (0 if none). For
    logging a count only, never content."""
    if not isinstance(text, str) or not text:
        return 0
    i = _anchor(text)
    return i if i > 0 else 0
