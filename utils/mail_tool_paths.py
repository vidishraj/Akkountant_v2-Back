"""ak-jmbw (2026-11-24): pin mail-processor tool file paths server-side.

The mail-processor MCP tools `get_pdf_pages` (fitz.open) and
`save_attachment` (shutil.copy2) used to act on ANY path the model put in
its tool arguments. Those agents read attacker-controllable email; the
ak-m0f6 review measured fitz opening a .txt and returning its text. With
ak-m0f6 there is no egress, but file contents could still land in DB rows
or the attachments dir, one tool away from exfiltration.

Rule: the model never chooses a file. The SERVICE passes the run's own
PDF (the one it downloaded / unlocked / is re-processing) as
`pinned_pdf_path`; the tools act only on that file, and only if its
realpath (symlinks resolved) is a regular `.pdf` file under:
  * <cwd>/tmp/                      (downloaded + unlocked statement PDFs)
  * <cwd>/claude_statements/<uid>/  (persisted statements, re-processing)
Anything else is refused. Leaf module: no app imports beyond the uid
shape validator, so it is unit-testable in isolation.
"""
from __future__ import annotations

import os

# Mirrors mailProcessorTools.save_attachment's `category` enum. Validated
# SERVER-side because `category` becomes a directory name.
SAVE_ATTACHMENT_CATEGORIES = frozenset({
    "bank_statement", "epf_passbook", "investment_confirmation",
    "gold_receipt", "freelance_payment", "freelance_contract",
    "freelance_correspondence",
})


class ToolPathRejected(Exception):
    """A tool file path failed the server-side pin. The message is
    generic on purpose (no path echoed back to the model)."""


def _within(path: str, root: str) -> bool:
    return path == root or path.startswith(root + os.sep)


def allowed_pdf_roots(user_id: str, cwd: str | None = None) -> list:
    """Realpaths of the directories a run's PDF may live in."""
    from utils.firebase_id_validator import assert_valid_firebase_id_for_path
    uid = assert_valid_firebase_id_for_path(user_id)
    base = cwd or os.getcwd()
    return [
        os.path.realpath(os.path.join(base, "tmp")),
        os.path.realpath(os.path.join(base, "claude_statements", uid)),
    ]


def resolve_pinned_pdf(pinned_pdf_path, user_id, cwd: str | None = None) -> str:
    """Return the realpath of the run's PDF if it passes the pin, else
    raise ToolPathRejected."""
    if not pinned_pdf_path:
        raise ToolPathRejected("no PDF is attached to this processing run")
    try:
        roots = allowed_pdf_roots(user_id, cwd)
    except Exception:
        raise ToolPathRejected("invalid user for file access")
    real = os.path.realpath(pinned_pdf_path)
    if not any(_within(real, r) for r in roots):
        raise ToolPathRejected("PDF is outside the allowed statement directories")
    if not real.lower().endswith(".pdf"):
        raise ToolPathRejected("only PDF files can be accessed")
    if not os.path.isfile(real):
        raise ToolPathRejected("PDF file not found")
    return real


def model_path_differs(model_path, pinned_real: str) -> bool:
    """True if the model asked for some OTHER file (logged as a flag
    only; the request is never honoured)."""
    if not model_path:
        return False
    try:
        return os.path.realpath(str(model_path)) != pinned_real
    except Exception:
        return True


def safe_attachment_dest(user_id, category, filename, cwd: str | None = None) -> str:
    """Destination path for save_attachment, confined to
    <cwd>/attachments/<uid>/<category>/ with a validated category and a
    separator-free filename. Raises ToolPathRejected."""
    from utils.firebase_id_validator import assert_valid_firebase_id_for_path
    try:
        uid = assert_valid_firebase_id_for_path(user_id)
    except Exception:
        raise ToolPathRejected("invalid user for file access")
    if category not in SAVE_ATTACHMENT_CATEGORIES:
        raise ToolPathRejected("unknown attachment category")
    name = os.path.basename(str(filename or "").replace("\\", "/"))
    name = name.replace("..", "")
    if not name or name in (".",):
        raise ToolPathRejected("invalid filename")
    base = cwd or os.getcwd()
    save_dir = os.path.realpath(os.path.join(base, "attachments", uid, category))
    dest = os.path.realpath(os.path.join(save_dir, name))
    if not _within(dest, save_dir) or dest == save_dir:
        raise ToolPathRejected("invalid filename")
    return dest
