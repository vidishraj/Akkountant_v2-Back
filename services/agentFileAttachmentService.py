"""ak-9dz: read + write access to persistent per-conversation agent file
attachments.

Two consumers:
  * `attach_file_to_chat` MCP tool (WRITE path) — copy an agent-produced
    file into user-scoped served storage + insert an
    agent_file_attachments row. Returns the row's uuid so the tool can
    emit the download-card content block to the LLM.
  * GET /api/user-files/<uuid> endpoint (READ path) — fetch a row by
    uuid, verify user_id matches g.firebase_id (cross-user isolation),
    return (row, disk_path) for the controller to stream.

Storage layout:
  * Base dir: `/var/akkountant/user_files/` (0o750, opc:opc, infra
    creates once via `sudo mkdir -p ... && sudo chown opc:opc ... &&
    sudo chmod 750 ...`).
  * Per-user subdir: `<base>/{user_id}/` — created on demand at first
    attach (0o750, ensures per-user isolation at fs-permission level).
  * Filename: `{uuid}_{sanitized_display_name}` — uuid prefix keeps
    collisions impossible per-user; display_name sanitized to reject
    path traversal (`..`, `/`, `\\`) + shell metacharacters.

Failure modes (all raise, no silent success):
  * Source path missing / unreadable → OSError from shutil.copy2.
  * Source path outside allowlisted scratch dirs → ValueError. Guards
    against prompt-injection-driven arbitrary-file-read: a compromised
    LLM told to `attach_file_to_chat("/etc/passwd", ...)` must NOT be
    able to copy server secrets into user-served storage. realpath is
    used before the allowlist check so symlink escapes resolve first.
  * Source file exceeds MAX_ATTACHMENT_SIZE_BYTES → ValueError. Bounds
    per-attachment disk cost + protects against a prompt-injected agent
    attaching e.g. `/var/log/messages` and blowing user storage.
  * Base dir doesn't exist / wrong permissions → OSError.
  * DB insert fails → re-raised after best-effort disk cleanup.
  * Sanitized filename empty (all-illegal input) → ValueError.

Read-path failure modes (return None; controller maps to 404):
  * uuid not found in table.
  * uuid found but user_id mismatch (cross-user isolation).
  * disk file gone (DB row survived a filesystem event) — logged then
    None so caller returns 404 rather than 500.
  * Pre-migration deploy window (table missing) → SELECT errors, caught,
    return None → 404 gracefully.
"""

from __future__ import annotations  # PEP 604 (`dict | None`) on 3.9

import mimetypes
import os
import re
import shutil
import uuid as _uuid_mod

from services.Base_Service import BaseService
from utils.logger import Logger

from models.agentFileAttachments import AgentFileAttachment


# Base directory for all per-user attachment storage. Infra sets up the
# root once (0o750, opc:opc). Per-user subdirs created on-demand.
# Env override for LOCAL dev where /var/ isn't writable by the app user.
_BASE_DIR = os.getenv(
    "AK_USER_FILES_DIR",
    "/var/akkountant/user_files",
)

# ── ak-9dz v2 CRITICAL: source-path allowlist ─────────────────────────
# Investment + Freelance agents process UNTRUSTED input (invoice text,
# receipt OCR, customer names). A prompt-injected agent CAN be steered
# into calling `attach_file_to_chat(local_path="/etc/passwd", ...)`
# — or worse: `/home/opc/.config/firebase/*.json`, `.env` secrets,
# `~/.dolt-data/`, repo config. Without a source allowlist, the tool
# becomes an arbitrary-file-read primitive that streams the read result
# via `/api/user-files/<uuid>`.
#
# Defence: BEFORE any copy, realpath the source (resolves symlink
# escapes like /tmp/x -> /etc/passwd) and reject any path that doesn't
# live under an allowed scratch dir. Trailing slashes on the entries
# are load-bearing: without them `/tmp` matches `/tmpfoo/`.
_ALLOWED_SCRATCH_BASES: tuple[str, ...] = (
    "/tmp/",
)

# Per-attachment size cap (MINOR fold-2 alongside the CRITICAL). Bounds
# a single prompt-injected attach from filling user storage or being a
# noticeable download. 50 MB is generous for the CSV / PDF / image /
# receipt-scan payloads the agents actually produce.
_MAX_ATTACHMENT_SIZE_BYTES = 50 * 1024 * 1024  # 50 MB

# ak-9dz display_name sanitizer: strips path traversal + shell chars.
# Whitelist alphanumerics, underscore, hyphen, dot, space (Indian
# filenames sometimes include spaces — the on-disk basename is quoted
# consistently by the Content-Disposition path so shell escaping is
# irrelevant; only path-shape safety matters here). Anything else
# collapses to underscore. Leading dots + slashes stripped explicitly
# to eliminate `../` + hidden-file traversal.
_SANITIZE_RE = re.compile(r"[^A-Za-z0-9_.\- ]+")
_LEADING_DOTS_RE = re.compile(r"^\.+")


def validate_source_path(local_path: str) -> str:
    """ak-9dz v2: enforce the source-path allowlist + size cap.

    Split out as a module-level pure function so the security boundary
    can be unit-tested without instantiating `AgentFileAttachmentService`
    (which pulls in `BaseService` → google.oauth2 / firebase — heavy
    for a bare-env test). `attach()` calls this before touching disk.

    Returns the post-realpath source path on success (callers use this
    for the actual copy so the path they authorized matches the path
    they read).

    Raises:
      * ValueError — path resolves outside `_ALLOWED_SCRATCH_BASES`
        (arbitrary-file-read guard) OR file exceeds
        `_MAX_ATTACHMENT_SIZE_BYTES` (storage-abuse guard).
      * FileNotFoundError — realpath resolves to a non-file (missing /
        directory).
    """
    real_source = os.path.realpath(local_path)
    # Trailing-slash comparison: `/tmp/file` starts with `/tmp/`, but
    # `/tmpfoo/file` does NOT. Also allow exact match on `/tmp` (bare
    # dir path) but downstream isfile() will still reject it as
    # not-a-file — the allowlist only guarantees LOCATION safety.
    if not any(
        real_source == base.rstrip("/") or real_source.startswith(base)
        for base in _ALLOWED_SCRATCH_BASES
    ):
        raise ValueError(
            f"source path outside allowed scratch dirs. "
            f"Allowed prefixes: {_ALLOWED_SCRATCH_BASES}. "
            f"Received realpath: {real_source!r}"
        )
    if not os.path.isfile(real_source):
        raise FileNotFoundError(
            f"attach_file_to_chat: source file missing: {local_path!r} "
            f"(realpath: {real_source!r})"
        )
    source_size = os.path.getsize(real_source)
    if source_size > _MAX_ATTACHMENT_SIZE_BYTES:
        raise ValueError(
            f"file size {source_size} bytes exceeds limit "
            f"{_MAX_ATTACHMENT_SIZE_BYTES} bytes "
            f"({source_size / 1_000_000:.1f} MB > "
            f"{_MAX_ATTACHMENT_SIZE_BYTES / 1_000_000:.0f} MB)"
        )
    return real_source


def sanitize_display_name(name: str) -> str:
    """Strip path traversal + shell chars from a user-facing filename.

    Rules:
      * empty / None → ValueError
      * strip surrounding whitespace
      * replace any run of chars outside `[A-Za-z0-9_.\\- ]` with `_`
      * strip leading `.` chars (kills `..`, hidden-file patterns)
      * truncate to 200 chars to bound on-disk path length
      * if the result is empty after sanitization → ValueError

    Returns the sanitized basename. Never returns a value containing
    `/` or `\\` — safe to concatenate into a disk path without further
    escaping.
    """
    if not name:
        raise ValueError("display_name is empty")
    stripped = name.strip()
    if not stripped:
        raise ValueError("display_name is whitespace-only")
    # Replace non-whitelist chars (this catches `/`, `\`, `:`, `;`, `|`,
    # `&`, `$`, backticks, quotes — the traversal + shell surface).
    sanitized = _SANITIZE_RE.sub("_", stripped)
    # Strip leading dots to kill `..`, `...`, `.hidden` patterns.
    sanitized = _LEADING_DOTS_RE.sub("", sanitized)
    # Bound total length to keep the on-disk path (base + uuid_ + name)
    # well under filesystem NAME_MAX (typically 255).
    sanitized = sanitized[:200]
    if not sanitized:
        raise ValueError(
            f"display_name={name!r} produced empty basename after sanitize"
        )
    return sanitized


class AgentFileAttachmentService(BaseService):
    """Persistent per-conversation file attachment store."""

    def __init__(self):
        super().__init__()
        self.logger = Logger(__name__).get_logger()

    # ── write path (MCP tool → this) ───────────────────────────────────

    def attach(
        self,
        *,
        user_id: str,
        local_path: str,
        display_name: str,
        conversation_id: int | None = None,
        message_id: int | None = None,
    ) -> dict:
        """Copy `local_path` into user-scoped storage + insert a row.

        Returns a dict with the on-wire content-block shape the MCP
        tool emits to the agent (and, transitively, that the FE
        renders as a download card):

          {
            "type": "file_attachment",
            "url": "/api/user-files/<uuid>",
            "name": "<sanitized display_name>",
            "size_bytes": <int>,
            "mime_type": "<detected or None>",
            "uuid": "<uuid>"
          }

        Raises (no silent success):
          * ValueError — display_name sanitizes to empty; source path
            resolves outside `_ALLOWED_SCRATCH_BASES`; source file
            exceeds `_MAX_ATTACHMENT_SIZE_BYTES`.
          * FileNotFoundError — local_path doesn't exist.
          * OSError — base dir setup / copy failed.
          * Any DB error from the insert bubbles after a best-effort
            disk cleanup.
        """
        if not user_id:
            raise ValueError("user_id is required")
        if not local_path:
            raise ValueError("local_path is required")
        safe_name = sanitize_display_name(display_name)

        # ak-9dz v2: source-path allowlist + size cap. Returns the
        # post-realpath path — copy2 below reads from it so the path we
        # copy matches the path we authorized (closes a TOCTOU window
        # even though the single-user threat model makes it academic).
        real_source = validate_source_path(local_path)

        # Per-user subdir (0o750 — group-r-x deliberate for opc:opc so
        # nginx or a sidecar in the same group could serve if we ever
        # front the directory; other-world has no access).
        user_dir = os.path.join(_BASE_DIR, user_id)
        os.makedirs(user_dir, mode=0o750, exist_ok=True)

        # UUID prefix keeps per-user filenames unique + lets the on-disk
        # path be reconstructed from just (user_id, uuid) if we ever
        # need to reverse-map without the DB (recovery pathway).
        file_uuid = str(_uuid_mod.uuid4())
        disk_name = f"{file_uuid}_{safe_name}"
        disk_path = os.path.join(user_dir, disk_name)

        # copy2 preserves mtime + mode for a cleaner audit trail. Read
        # from `real_source` (post-realpath) to match what the allowlist
        # authorized — closes a TOCTOU window even if the single-user
        # threat model makes it academic.
        shutil.copy2(real_source, disk_path)
        try:
            size_bytes = os.path.getsize(disk_path)
            mime_type, _enc = mimetypes.guess_type(safe_name)

            row = AgentFileAttachment(
                uuid=file_uuid,
                user_id=user_id,
                conversation_id=conversation_id,
                message_id=message_id,
                display_name=safe_name,
                mime_type=mime_type,
                size_bytes=size_bytes,
                disk_path=disk_path,
            )
            session = self.db.session
            session.add(row)
            session.commit()
        except Exception:
            # DB insert failed — best-effort unlink the file so we
            # don't leak disk state without a matching row. Re-raise
            # so the tool surfaces the failure to the agent (never
            # silent success).
            try:
                if os.path.isfile(disk_path):
                    os.remove(disk_path)
            except OSError as _unlink_exc:
                self.logger.warning(
                    f"attach: cleanup of orphan {disk_path!r} failed: {_unlink_exc}"
                )
            try:
                self.db.session.rollback()
            except Exception:
                pass
            raise

        self.logger.info(
            f"attach: uuid={file_uuid} user={user_id[:8]}... "
            f"name={safe_name!r} size={size_bytes} mime={mime_type!r} "
            f"conv={conversation_id} msg={message_id}"
        )

        return {
            "type": "file_attachment",
            "url": f"/api/user-files/{file_uuid}",
            "name": safe_name,
            "size_bytes": size_bytes,
            "mime_type": mime_type,
            "uuid": file_uuid,
        }

    # ── read path (endpoint → this) ────────────────────────────────────

    def get_for_download(self, user_id: str, uuid: str) -> tuple[dict, str] | None:
        """Look up a file by uuid, verify user_id matches, return the
        (metadata_dict, disk_path) pair.

        Returns None on any of:
          * missing uuid → not-found.
          * uuid found but user_id mismatch → cross-user isolation
            (generic 404 at controller so existence never leaks).
          * uuid found + owned but disk file missing → filesystem
            drift; log WARN, return None so caller returns 404 rather
            than a broken 500 stream.
          * pre-migration deploy window (table missing) → catch DB
            error, log WARN, return None → 404 gracefully.

        Never raises — the endpoint contract is "None → 404, tuple →
        stream". Genuine DB errors log at ERROR and still return None
        (safer than propagating a 500 on a download).
        """
        if not user_id or not uuid:
            return None
        try:
            row = (
                self.db.session.query(AgentFileAttachment)
                .filter(AgentFileAttachment.uuid == uuid)
                .first()
            )
        except Exception as exc:
            try:
                self.db.session.rollback()
            except Exception:
                pass
            self.logger.error(
                f"get_for_download: DB read failed for uuid={uuid[:8]}...: "
                f"{type(exc).__name__}: {exc}"
            )
            return None
        if row is None:
            return None
        # Cross-user isolation — reject BEFORE reading disk. Matches the
        # AgentConversation.get_conversation pattern (soft-deleted / other-
        # user look identical to a true miss).
        if row.user_id != user_id:
            self.logger.warning(
                f"get_for_download: uuid={uuid[:8]}... requested by "
                f"user={user_id[:8]}... but owned by "
                f"{(row.user_id or '')[:8]}... — returning generic 404"
            )
            return None
        if not row.disk_path or not os.path.isfile(row.disk_path):
            self.logger.warning(
                f"get_for_download: uuid={uuid[:8]}... row present but "
                f"disk file missing at {row.disk_path!r} — returning 404"
            )
            return None
        return (
            {
                "uuid": row.uuid,
                "display_name": row.display_name,
                "mime_type": row.mime_type or "application/octet-stream",
                "size_bytes": row.size_bytes,
            },
            row.disk_path,
        )
