"""Investment-agent chat attachment helpers (ak-1x4).

Backs the paperclip UX on /agent/chat (investment agent only). Ephemeral
per-turn lifecycle: FE uploads a file → BE saves under
/tmp/akkountant-agent-attachments/<user_id>/<uuid>/<filename>; FE sends
the attachment_id back on the next /agent/chat call; BE injects the
absolute path into the user prompt for Claude's Read tool; BE rmtree's
the directory in the agentService.stream_chat finally block.

Path A (filesystem + Claude Code's native Read tool) is chosen because
claude_agent_sdk routes through Claude Code CLI which takes a string
prompt, not Anthropic content blocks — we cannot pass `{type: "image",
source: {...}}` directly. Claude's Read handles PDF / PNG / JPEG / WebP
/ GIF natively, so the cheapest bridge is a filesystem path.

Failure modes:
  - Upload too big / wrong MIME → ValueError raised by save_upload, the
    controller maps to 400.
  - Resolve hits a missing or wrong-user attachment_id →
    AttachmentNotFound raised; the controller surfaces as a structured
    error event over SSE.
  - Cleanup failures (already gone, partial rmtree) → silently swallowed
    in cleanup(); the sweeper will pick up any orphans.

Security:
  - filename sanitized to a path-traversal-safe alphabet; falls back to
    'attachment-<uuid>.<ext>' when the original is empty after sanitize
  - per-user namespacing makes cross-user lookup return AttachmentNotFound
    without ever touching the other user's tree
  - magic-byte check in addition to browser-reported content_type (a
    multipart attacker can lie about content_type; bytes don't lie cheaply)
  - bounded sweep walks only the per-user subtree depth, never follows
    symlinks (rmtree honors os.path.islink)
"""

from __future__ import annotations

import os
import re
import shutil
import time
import uuid as _uuid
from typing import Optional

# Storage root lives in /tmp because:
#   - clean-on-reboot is fine for ephemeral-per-turn lifecycle
#   - no DB schema migration / volume mount required by infra
#   - PROD container has /tmp as tmpfs by default; size limits are kernel-managed
STORAGE_ROOT = "/tmp/akkountant-agent-attachments"

# Spec-locked allowlist. NOTE: we DO NOT include text/plain or other types
# — Claude's Read handles many formats but the agent spec restricts to
# financial-document-flavored uploads (PDFs from banks, image scans of
# statements / cheques). Expanding this needs an explicit Lead greenlight.
ALLOWED_MIME = frozenset({
    "application/pdf",
    "image/png",
    "image/jpeg",
    "image/webp",
    "image/gif",
})

# Anthropic's documented practical PDF cap. Image limit is higher than
# this but 10MB keeps the upload latency budget reasonable. The Flask
# app.config.MAX_CONTENT_LENGTH should be set slightly above this
# (multipart overhead) — see app.py.
MAX_BYTES = 10 * 1024 * 1024  # 10 MB

# Spec-locked. FE must also enforce this client-side for snappy UX.
MAX_PER_MESSAGE = 4

# Stale-upload sweep horizon. A user could upload then never send; sweep
# them out an hour later to bound disk usage. Cheap to do on every upload
# since the tree is per-user and shallow.
MAX_AGE_SECONDS = 3600

# Filename sanitize: keep alnum + dot + dash + underscore. Strip path
# components by basenaming first; drop anything else. Worst case the user
# loses a Unicode name; that's acceptable for a financial-data attachment.
_SAFE_FILENAME_RE = re.compile(r"[^A-Za-z0-9._-]")

# Maximum sanitized filename length to keep paths well under PATH_MAX even
# inside deeply-nested PROD container paths. 96 leaves headroom for the
# /tmp/akkountant-agent-attachments/<firebase_id>/<uuid>/ prefix.
_MAX_FILENAME_LEN = 96

# Hidden sidecar that records the magic-byte-validated content_type at
# upload time. resolve() reads this back rather than re-deriving from the
# filename extension (which is user-controlled and may be missing/wrong).
# ak-1x4 pass 3 (reviewer hq-wisp-sh1to MAJOR): closes the text-fallback
# prompt-injection channel + the media_type-mismatch minor by removing
# the filename → content_type trust path entirely. Starts with "." so
# it's already filtered out by the existing entries listing in resolve.
_CONTENT_TYPE_SIDECAR = ".content_type"

# Magic-byte prefixes for the allowed types. Each entry is a tuple of
# (prefix_at_offset_0, ...) — the file matches if ANY prefix matches.
# Entry value of None means "use _check_magic_complex" instead (used
# for WebP which has a non-prefix signature: "RIFF" + 4-byte size +
# "WEBP" at offset 8).
_MAGIC_PREFIXES = {
    "application/pdf": (b"%PDF-",),
    "image/png": (b"\x89PNG\r\n\x1a\n",),
    # JPEG: SOI marker FF D8 FF, then a one-byte app marker (E0 JFIF, E1 EXIF, E2 ICC, DB quant, etc.)
    "image/jpeg": (b"\xff\xd8\xff",),
    # GIF87a / GIF89a
    "image/gif": (b"GIF87a", b"GIF89a"),
    # WebP is a special case — see _check_magic_bytes below. "RIFF"
    # alone matches WAV / AVI / ANI containers; we additionally check
    # bytes 8..12 == b"WEBP" to be sure.
    "image/webp": None,
}


class AttachmentNotFound(Exception):
    """Raised when an attachment_id can't be resolved for the given user.

    Intentionally generic: 'not found' is the right surface for both a
    missing attachment AND a cross-user lookup — never leak which one
    via the error string (per spec: 'attachment not found', not 'wrong
    user').
    """


def _safe_filename(raw: Optional[str], fallback_ext: str = "bin") -> str:
    """Strip path components + drop unsafe characters.

    Returns a filename that's safe to join under STORAGE_ROOT. Never
    returns an empty string; falls back to 'attachment.<ext>' if the
    sanitized result is empty.
    """
    if not raw:
        return f"attachment.{fallback_ext}"

    # Drop any directory components (defends against
    # "../../../etc/passwd" style names from a hostile FE).
    base = os.path.basename(raw)

    # Replace anything outside the safe alphabet with '_'. Doing a
    # blanket-replace beats a strict regex match because we want to
    # preserve as much of the user's name as possible (so they see
    # "bank_stmt.pdf" not "attachment-uuid.pdf").
    sanitized = _SAFE_FILENAME_RE.sub("_", base)

    # Collapse runs of underscores so we don't produce
    # "foo___bar.pdf" from "foo bar (1).pdf".
    sanitized = re.sub(r"_+", "_", sanitized).strip("._-")

    if not sanitized:
        return f"attachment.{fallback_ext}"

    # Truncate but preserve extension.
    if len(sanitized) > _MAX_FILENAME_LEN:
        root, ext = os.path.splitext(sanitized)
        if ext and len(ext) <= 8:
            keep = _MAX_FILENAME_LEN - len(ext)
            sanitized = root[:keep] + ext
        else:
            sanitized = sanitized[:_MAX_FILENAME_LEN]

    return sanitized


def _ext_for_mime(content_type: str) -> str:
    """Best-effort extension for a content_type. Used only as a fallback
    when the FE-supplied filename is empty or all-illegal characters."""
    return {
        "application/pdf": "pdf",
        "image/png": "png",
        "image/jpeg": "jpg",
        "image/webp": "webp",
        "image/gif": "gif",
    }.get(content_type, "bin")


def _user_dir(user_id: str) -> str:
    """Per-user subtree. firebase_id is opaque + safe (alnum) so no
    sanitize needed, but defensively basename it anyway in case some
    code path passes a path-shaped string."""
    safe_user = os.path.basename((user_id or "").strip()) or "_unknown"
    return os.path.join(STORAGE_ROOT, safe_user)


def _check_magic_from_head(head: bytes, declared_type: str) -> bool:
    """Apply the magic-byte test to an in-memory head buffer.

    Shared between the post-write check (legacy code path) and the
    short-circuit first-chunk check inside save_upload (reviewer fix
    MINOR: avoid wasting disk on hostile 10MB blobs by validating the
    first chunk before writing the rest).
    """
    # WebP: RIFF<4-byte size>WEBP — needs both the RIFF marker AND
    # the WEBP marker at offset 8, otherwise we'd accept any RIFF
    # container (WAV / AVI / ANI).
    if declared_type == "image/webp":
        return (
            len(head) >= 12
            and head.startswith(b"RIFF")
            and head[8:12] == b"WEBP"
        )
    prefixes = _MAGIC_PREFIXES.get(declared_type)
    if prefixes is None:
        # No prefix rule → fail closed. Earlier this defaulted to True
        # which was a footgun if a new MIME got added to the allowlist
        # without a magic rule. With the type allowlist already locked,
        # we never reach this branch for an allowed type, but be strict.
        return False
    return any(head.startswith(p) for p in prefixes)


def _check_magic_bytes(path: str, declared_type: str) -> bool:
    """Read the first 16 bytes off disk and apply the magic-byte test."""
    try:
        with open(path, "rb") as fh:
            head = fh.read(16)
    except OSError:
        return False
    return _check_magic_from_head(head, declared_type)


def save_upload(user_id: str, file_storage, *,
                content_type_hint: Optional[str] = None) -> dict:
    """Save a single uploaded file under the per-user dir.

    Args:
        user_id: Firebase ID (from g.firebase_id).
        file_storage: A werkzeug FileStorage (request.files['file']).
        content_type_hint: Override for the browser-reported content_type.
            Only used when file_storage.content_type is empty.

    Returns:
        dict {attachment_id, filename, size, content_type, path}

    Raises:
        ValueError: invalid type / oversized / write failure.
    """
    declared = (file_storage.content_type or content_type_hint or "").lower()
    if declared not in ALLOWED_MIME:
        raise ValueError(
            f"Unsupported file type: {declared or 'unknown'}. "
            f"Allowed: {sorted(ALLOWED_MIME)}"
        )

    raw_name = getattr(file_storage, "filename", None)
    safe_name = _safe_filename(raw_name, fallback_ext=_ext_for_mime(declared))

    attachment_id = str(_uuid.uuid4())
    parent = os.path.join(_user_dir(user_id), attachment_id)

    # mkdir -p with restrictive perms. The /tmp tmpfs is shared with other
    # processes in the container — keep our subtree owner-only.
    os.makedirs(parent, mode=0o700, exist_ok=False)
    final_path = os.path.join(parent, safe_name)

    # Stream-copy in a chunked way and short-circuit on cap exceed OR
    # magic-byte mismatch on the very first chunk. The first-chunk check
    # is the reviewer fix (MINOR): a hostile 10MB blob can no longer
    # waste 10MB of disk + write IO before we notice the type lied.
    bytes_written = 0
    chunk_size = 64 * 1024
    magic_checked = False
    try:
        with open(final_path, "wb") as out:
            while True:
                chunk = file_storage.stream.read(chunk_size)
                if not chunk:
                    break
                # First-chunk magic check: 64KB chunk size guarantees
                # ≥12 bytes for WebP and ≥8 bytes for every other
                # allowed type. If declared MIME doesn't match the
                # first-chunk bytes, fail fast without writing.
                if not magic_checked:
                    if not _check_magic_from_head(chunk, declared):
                        raise ValueError(
                            f"File contents do not match declared type {declared}"
                        )
                    magic_checked = True
                bytes_written += len(chunk)
                if bytes_written > MAX_BYTES:
                    raise ValueError(
                        f"File exceeds {MAX_BYTES // (1024 * 1024)}MB cap"
                    )
                out.write(chunk)
    except ValueError:
        # Caller maps to HTTP 400. Clean up the partial write before
        # bubbling so we don't leak disk.
        shutil.rmtree(parent, ignore_errors=True)
        raise
    except OSError as exc:
        shutil.rmtree(parent, ignore_errors=True)
        raise ValueError(f"Write failed: {exc}") from exc

    if bytes_written == 0:
        shutil.rmtree(parent, ignore_errors=True)
        raise ValueError("Empty file")

    # ak-1x4 pass 3: persist the magic-byte-validated content_type so
    # resolve() can return it later instead of re-deriving from the
    # user-controlled filename extension. Writing this AFTER the file
    # data + AFTER magic validation guarantees the sidecar reflects a
    # type we actually verified. Sidecar contents = the MIME string
    # only (no JSON / framing) — minimal surface area.
    sidecar_path = os.path.join(parent, _CONTENT_TYPE_SIDECAR)
    try:
        with open(sidecar_path, "w", encoding="ascii") as sidecar:
            sidecar.write(declared)
    except OSError as exc:
        # Sidecar write failure invalidates the persistence guarantee;
        # rather than letting resolve() silently fall back to extension
        # guessing later, reject the whole upload now.
        shutil.rmtree(parent, ignore_errors=True)
        raise ValueError(f"Write failed (sidecar): {exc}") from exc

    return {
        "attachment_id": attachment_id,
        "filename": safe_name,
        "size": bytes_written,
        "content_type": declared,
        "path": final_path,
    }


def resolve(user_id: str, attachment_id: str) -> dict:
    """Resolve an attachment_id to its on-disk record for one specific user.

    Raises AttachmentNotFound if the id doesn't exist, the directory is
    empty, OR the user_id doesn't match (cross-user lookup). The error is
    intentionally generic — see class docstring.
    """
    # Defense: reject anything that's not a uuid-shaped string so we
    # never join a malformed segment under the user dir.
    try:
        _uuid.UUID(str(attachment_id))
    except (ValueError, TypeError):
        raise AttachmentNotFound(f"Invalid attachment_id: {attachment_id}")

    parent = os.path.join(_user_dir(user_id), attachment_id)
    if not os.path.isdir(parent):
        raise AttachmentNotFound(f"Attachment not found: {attachment_id}")

    # The dir should contain exactly one file (the upload). If empty or
    # missing, treat as not-found.
    entries = [e for e in os.listdir(parent) if not e.startswith(".")]
    if not entries:
        raise AttachmentNotFound(f"Attachment not found: {attachment_id}")

    filename = entries[0]
    path = os.path.join(parent, filename)
    try:
        size = os.path.getsize(path)
    except OSError:
        raise AttachmentNotFound(f"Attachment not found: {attachment_id}")

    # ak-1x4 pass 3: content_type comes from the persisted sidecar
    # (.content_type) written at upload time AFTER magic-byte validation.
    # This eliminates the previous filename → mime re-derivation, which
    # was a trust-the-user vector (no-ext filename → octet-stream → text
    # fallback in agentService._content_block_for → prompt injection).
    # If the sidecar is missing (data-on-disk from a pre-pass-3 build,
    # or hand-placed), fail closed: treat the attachment as not-found so
    # we don't fall back to an unvalidated content_type.
    sidecar_path = os.path.join(parent, _CONTENT_TYPE_SIDECAR)
    try:
        with open(sidecar_path, "r", encoding="ascii") as sidecar:
            content_type = sidecar.read().strip()
    except OSError:
        raise AttachmentNotFound(
            f"Attachment {attachment_id} missing content_type metadata"
        )

    # Defense in depth: if the sidecar value isn't on the upload-time
    # allowlist anymore (would only happen if disk was tampered with
    # post-upload), reject. The handler then surfaces an isError tool
    # result rather than ever passing an unvalidated type downstream.
    if content_type not in ALLOWED_MIME:
        raise AttachmentNotFound(
            f"Attachment {attachment_id} has unsupported content_type"
        )

    return {
        "attachment_id": attachment_id,
        "filename": filename,
        "size": size,
        "content_type": content_type,
        "path": path,
    }


def cleanup(user_id: str, attachment_id: str) -> None:
    """Best-effort rmtree of one attachment's per-id subtree.

    Errors are intentionally swallowed: this is called from
    agentService.stream_chat's finally block where we'd rather degrade
    quietly (the sweeper will pick up orphans) than raise inside an
    error-recovery path.
    """
    if not attachment_id:
        return
    try:
        _uuid.UUID(str(attachment_id))
    except (ValueError, TypeError):
        return  # Not a valid id — nothing to clean.
    parent = os.path.join(_user_dir(user_id), attachment_id)
    shutil.rmtree(parent, ignore_errors=True)


def sweep_stale(max_age_seconds: int = MAX_AGE_SECONDS) -> int:
    """Walk STORAGE_ROOT and delete attachment subtrees older than the
    threshold. Returns the number of subtrees deleted.

    Called from the controller's attach() at the start of every upload
    (cheap because the tree is bounded by users * recent uploads).
    Safe under concurrent uploads because each subtree is owned by a
    single uuid; we never touch the user_dir itself.

    Uses directory mtime — when we create a parent dir, its mtime is
    set; subsequent reads / writes within the dir don't reset it on
    most filesystems, but on a few they do. To be conservative we use
    max(dir_mtime, file_mtime_in_dir).
    """
    now = time.time()
    deleted = 0

    if not os.path.isdir(STORAGE_ROOT):
        return 0

    try:
        user_dirs = os.listdir(STORAGE_ROOT)
    except OSError:
        return 0

    for user in user_dirs:
        user_path = os.path.join(STORAGE_ROOT, user)
        if not os.path.isdir(user_path):
            continue
        try:
            attachment_ids = os.listdir(user_path)
        except OSError:
            continue
        for att_id in attachment_ids:
            att_path = os.path.join(user_path, att_id)
            if not os.path.isdir(att_path):
                continue
            # ak-1x4 pass 3 fix: "age" of an attachment is the time of
            # its EARLIEST on-disk write — the upload file (written
            # before the sidecar) is the canonical "upload happened at"
            # marker. We take min() of the contained files' mtimes
            # rather than max() so that an internal "newer" sibling
            # (the .content_type sidecar, written ~milliseconds after
            # the upload during save_upload) doesn't keep an old upload
            # alive.
            #
            # Previously this used max(dir_mtime, any_file_mtime), but
            # after the sidecar was introduced that meant a sidecar
            # touched at upload-time would dominate even when the upload
            # file itself was clearly stale (the test back-dates only
            # the upload file's mtime, mirroring how a real stale dir
            # would look on disk).
            #
            # On the empty-dir edge case (no files inside, just a
            # dangling directory from a rmtree race) we fall back to
            # the dir mtime so we still sweep it eventually.
            try:
                file_mtimes = []
                for entry in os.listdir(att_path):
                    entry_path = os.path.join(att_path, entry)
                    try:
                        file_mtimes.append(os.path.getmtime(entry_path))
                    except OSError:
                        continue
                if file_mtimes:
                    mtime = min(file_mtimes)
                else:
                    mtime = os.path.getmtime(att_path)
            except OSError:
                continue
            if (now - mtime) >= max_age_seconds:
                shutil.rmtree(att_path, ignore_errors=True)
                deleted += 1

    return deleted


def format_for_prompt(records: list) -> str:
    """Format a list of resolved attachment records for injection into
    the user prompt. ak-1x4 pass 2 (reviewer hq-wisp-sh1to): the block
    lists each attachment_id (the opaque UUID), NOT the on-disk path.
    The investment agent calls the read_attachment MCP tool with this
    id; the tool's closure looks up the path internally. Paths never
    appear in the chat surface, which is both cleaner UX and removes a
    path-leak vector.

    Returns an empty string if records is empty so the caller can do a
    trivial concat with the original user content.
    """
    if not records:
        return ""

    lines = [
        "",
        "[Attachments — call the read_attachment MCP tool with each "
        "attachment_id to inspect the file contents before responding]",
    ]
    for r in records:
        size_kb = (r.get("size") or 0) / 1024.0
        if size_kb >= 1024:
            size_str = f"{size_kb / 1024:.1f}MB"
        else:
            size_str = f"{size_kb:.0f}KB"
        filename = r.get("filename") or "attachment"
        ctype = r.get("content_type") or "unknown"
        att_id = r.get("attachment_id") or "?"
        lines.append(
            f"- attachment_id={att_id} (filename={filename}, "
            f"type={ctype}, {size_str})"
        )
    return "\n".join(lines)
