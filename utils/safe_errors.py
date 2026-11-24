"""ak-q9c8: the ONE error-text policy (extends ak-67q4 / ak-ouu2).

An exception's message can carry personal financial data: SQLAlchemy
errors embed bound params (passwords, PAN, amounts), and parse / agent
errors can quote email or statement text. So:

  * client-facing text: a fixed message + `error_type(e)`
  * persisted reasons and logs: `error_tag(e)` (= Type[closed-set label],
    classified server-side) + `error_frames(e)` (frames only)

Never format the exception itself (`{e}`, `str(e)`, `exc_info=True`).
A message scrubber was rejected: it can strip a known shape such as
`[parameters: ...]`, but can't prove that an arbitrary message is free
of receipt or email content.
"""
from __future__ import annotations

import traceback


def error_type(e: BaseException) -> str:
    return type(e).__name__


def _chain(e: BaseException):
    """e, then its __cause__ / __context__ chain (cycle-safe, bounded)."""
    seen, cur = set(), e
    while cur is not None and id(cur) not in seen and len(seen) < 10:
        seen.add(id(cur))
        yield cur
        cur = cur.__cause__ or (None if cur.__suppress_context__ else cur.__context__)


def error_frames(e: BaseException) -> str:
    """Traceback frames (file, line, function, source line) without any
    exception message, for e and its cause / context chain (ak-q9c8
    review minor: each link is headed by its TYPE only)."""
    parts = []
    for i, link in enumerate(_chain(e)):
        if i:
            parts.append(f"-- caused by / during {type(link).__name__}:\n")
        parts.append("".join(traceback.format_tb(link.__traceback__)))
    return "".join(parts)


def error_label(e: BaseException) -> str:
    """ak-q9c8 review MAJOR: the closed-set incident label
    (utils.reprocess_status canonical error_type: mysql_1040,
    sdk_message_reader, pdf_password, stream, rate_limit, ...), classified
    SERVER-SIDE from the type + message of e and its chain. Only the label
    leaves this function; the message is never stored or logged."""
    from utils.reprocess_status import _classify_reason
    for link in _chain(e):
        try:
            text = f"{type(link).__name__} {link}"
        except Exception:
            text = type(link).__name__
        label = _classify_reason(text)
        if label != "unknown":
            return label
    return "unknown"


def error_tag(e: BaseException) -> str:
    """`Type[label]`, e.g. `OperationalError[mysql_1040]`: what logs and
    persisted reasons carry instead of the message."""
    return f"{error_type(e)}[{error_label(e)}]"


__all__ = ["error_type", "error_frames", "error_label", "error_tag"]
