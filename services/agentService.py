"""
Core agent orchestration service.
Uses claude_agent_sdk to route queries through Claude Code CLI (no API key needed).
Tool calls are handled via an in-process MCP server.
"""

import hashlib
import json
import os
import time
import anyio
from services.Base_Service import BaseService
from services.agent_tools import get_agent_config
from services.agent_tool_executor import execute_tool, TOOL_ERROR_KEY
from utils.logger import Logger
from utils.sdk_runner import emit_agent_run, _make_sdk_stderr_logger
import base64 as _base64  # ak-1x4 pass 2: encode attachment bytes for tool result
from utils.agent_attachments import (
    AttachmentNotFound,
    cleanup as attachment_cleanup,
    format_for_prompt as attachments_format_for_prompt,
    resolve as attachment_resolve,
    MAX_PER_MESSAGE as ATTACHMENTS_MAX_PER_MESSAGE,
)
from claude_agent_sdk import (
    query,
    ClaudeAgentOptions,
    SdkMcpTool,
    create_sdk_mcp_server,
    AssistantMessage,
    ResultMessage,
    TextBlock,
    ToolUseBlock,
)

MCP_SERVER_NAME = "agent_tools"


# Tools that require user confirmation before execution
DESTRUCTIVE_TOOLS = {
    "delete_single_investment",
    "delete_all_investments",
    "delete_file",
    "delete_invoice",
    "delete_customer",
    "scan_emails_for_transactions",
    "scan_statements",
    "sync_kite_holdings",
    "process_mail_pipeline",
}

# Tools that mutate data (used to signal frontend to refresh)
MUTATION_TOOLS = {
    "insert_investment",
    "delete_single_investment",
    "delete_all_investments",
    "sync_kite_holdings",
    "update_transaction",
    "scan_emails_for_transactions",
    "scan_statements",
    "delete_file",
    "create_invoice",
    "update_invoice",
    "delete_invoice",
    "create_customer",
    "update_customer",
    "delete_customer",
    "trigger_rate_refresh",
    "process_mail_pipeline",
}

MAX_TURNS = 20

# Name of the per-turn custom MCP tool that exposes attached files to
# the investment agent. Confinement is enforced inside the handler's
# closure (membership check against the resolved attachment_id set);
# CLI permission semantics are not relied on.
READ_ATTACHMENT_TOOL_NAME = "read_attachment"


def compute_allowed_tools(agent_type, mcp_tool_names, has_attachments):
    """Build the allowed_tools list for a given chat turn.

    ak-1x4 pass 2 (reviewer hq-wisp-sh1to architectural fix): the
    investment agent gets ONE additional MCP tool name when attachments
    are present this turn — `mcp__agent_tools__read_attachment`. No
    built-in tool (Read, Bash, …) is added; the prior approach used
    Claude Code's built-in Read with a `can_use_tool` gate, which was
    architecturally inert under permission_mode='bypassPermissions'
    (allowed_tools entries are pre-approved → CLI never calls the gate).

    Extracted to module scope so the wiring shape is directly unit-
    testable from the test suite without a live SDK / MCP server.
    """
    tools = list(mcp_tool_names)
    if agent_type == "investment" and has_attachments:
        tools.append(f"mcp__{MCP_SERVER_NAME}__{READ_ATTACHMENT_TOOL_NAME}")
    return tools


# Per-allowed-MIME content-block shape for the MCP tool result.
#
# ak-4tc P0 (2026-08-03): the SDK's MCP bridge
# (claude_agent_sdk/__init__.py::create_sdk_mcp_server, call_tool
# handler lines ~301-313) only handles two content-block shapes:
#   * {type: 'text',  text: <str>}          → wrapped in TextContent
#   * {type: 'image', data: <b64>, mimeType: <mime>}  → wrapped in ImageContent
# Anything else (including Anthropic-API-native `{type: 'image',
# source: {type: 'base64', media_type, data}}` and `{type: 'document',
# ...}`) is SILENTLY DROPPED — the SDK builds an empty content list and
# the LLM confabulates from thin air. This bug was live ~4 weeks from
# ak-1x4 landing and produced fully fabricated investments (Overseer's
# repro: BOI Consumption Fund screenshot → "HDFC Flexi Cap Fund" with
# every field made up).
#
# Fix: return the MCP shape the SDK actually reads. PDFs get text-
# extracted via PyMuPDF (fitz) as an MVP fallback — the SDK bridge has
# no 'document' branch, so vision-native PDF handling requires a bigger
# refactor (either per-page image render à la mailProcessorToolExecutor
# ._handle_get_pdf_pages, or waiting for the SDK to add a document
# branch). Text-extract is the immediate-ship option that stops the
# hallucination class today.
class _UnsupportedAttachmentMime(Exception):
    """Internal signal that _content_block_for got a MIME outside the
    image / application-pdf families, OR PDF extraction failed hard.
    Should be unreachable for any attachment that came through
    save_upload (which enforces ALLOWED_MIME + magic-byte sniffing +
    persists the validated type via the sidecar). Raised so the
    read_attachment handler can return isError instead of letting an
    unvalidated / unrenderable type reach the agent."""


# Cap PDF text extract at 200KB. The SDK's inbound JSON buffer is 1MB
# (see mailProcessorToolExecutor._MAX_CONTENT_BYTES), and this is a
# read_attachment tool _result_ so we want headroom for the envelope +
# other content blocks. 200KB is roughly ~40k tokens — plenty for any
# statement/screenshot text that a user would upload as a single file.
_PDF_EXTRACT_MAX_CHARS = 200_000


def _content_block_for(content_type, data_bytes, *, filename=None):
    """Build the MCP-shape content block for a given (mime, bytes) pair.

    Used by the read_attachment SdkMcpTool handler. The returned dict
    is consumed by claude_agent_sdk's MCP bridge, which only handles
    two shapes (see module-level ak-4tc comment above). Anthropic-API-
    native shapes with a nested `source: {...}` are dropped.

    Image branch → {type: 'image', data: <b64>, mimeType: <mime>}
    PDF branch   → {type: 'text',  text:  <fitz text-extract>}   (MVP fallback)

    Raises _UnsupportedAttachmentMime for any other content_type OR
    when PDF text extraction fails hard (unreadable/corrupt PDF).

    ak-1x4 pass 3 (kept from prior fix) — fails closed instead of
    degrading to a utf-8-replace text dump for unknown types, which
    was a prompt-injection vector when content_type used to be
    re-derived from the user-controlled filename extension. Sidecar
    persistence in agent_attachments.resolve() makes the input MIME
    trustworthy today; this raise is defense-in-depth.
    """
    if content_type and content_type.startswith("image/"):
        # MCP shape — top-level `data` + `mimeType`. NO `source` nesting.
        # This is the exact shape the SDK's call_tool ImageContent
        # constructor reads (item['data'], item['mimeType']).
        return {
            "type": "image",
            "data": _base64.b64encode(data_bytes).decode("ascii"),
            "mimeType": content_type,
        }
    if content_type == "application/pdf":
        # ak-4tc MVP: text-extract via PyMuPDF (already a required dep;
        # used throughout mailProcessorService + parsers). The SDK bridge
        # has no 'document' branch, so this is the fastest path to non-
        # hallucinated PDF handling. Vision-native PDF (per-page render)
        # is a follow-up if fidelity turns out to be insufficient — see
        # mailProcessorToolExecutor._handle_get_pdf_pages for the pattern.
        try:
            import fitz  # PyMuPDF — imported lazily so unit tests can
            # exercise the image branch without the fitz dep installed.
        except ImportError as exc:
            raise _UnsupportedAttachmentMime(
                f"PDF text extraction unavailable "
                f"(PyMuPDF/fitz import failed: {exc})"
            )
        try:
            doc = fitz.open(stream=data_bytes, filetype="pdf")
        except Exception as exc:
            raise _UnsupportedAttachmentMime(
                f"PDF could not be opened for text extraction: {exc}"
            )
        try:
            page_chunks = []
            for i, page in enumerate(doc, start=1):
                try:
                    body = page.get_text() or ""
                except Exception:
                    # Extraction failure on one page shouldn't kill the
                    # whole file — mark it and continue.
                    body = "(page extraction failed)"
                page_chunks.append(f"--- Page {i} ---\n{body.strip()}")
            extracted = "\n\n".join(page_chunks).strip()
        finally:
            try:
                doc.close()
            except Exception:
                pass
        if not extracted:
            # Common with image-only / scanned PDFs — text layer is empty.
            # Return a clear signal instead of an empty string so the LLM
            # doesn't try to summarize nothing.
            extracted = (
                "(no extractable text — this PDF appears to be image-based "
                "or scanned; ask the user for a text-based PDF or a "
                "screenshot instead)"
            )
        if len(extracted) > _PDF_EXTRACT_MAX_CHARS:
            extracted = (
                extracted[:_PDF_EXTRACT_MAX_CHARS]
                + "\n\n[…truncated — PDF text exceeds "
                f"{_PDF_EXTRACT_MAX_CHARS // 1000}KB cap]"
            )
        header_bits = ["[PDF text extract"]
        if filename:
            header_bits.append(f" — {filename}")
        header_bits.append("]\n\n")
        return {
            "type": "text",
            "text": "".join(header_bits) + extracted,
        }
    raise _UnsupportedAttachmentMime(
        f"unsupported content_type for read_attachment: {content_type!r}"
    )


def make_read_attachment_tool(user_id, attachment_records, logger=None):
    """Build an SdkMcpTool that exposes ONLY this turn's attachments.

    ak-1x4 pass 2 (reviewer hq-wisp-sh1to): confinement is enforced
    inside the closure via a membership check against the resolved
    attachment_id set. This is mode-independent — it does not rely on
    permission_mode or any CLI permission control surface. Cross-user
    isolation is guaranteed because (a) the membership set is built
    from records resolve()'d under user_id only, and (b) any other id
    fails the membership check and returns isError without touching
    disk.

    The handler reads the file off the per-user attachment subtree
    (path comes from the resolved record, never reconstructed from
    the model's input) and returns an image/document/text content
    block as appropriate.
    """
    # Build the lookup once: attachment_id → (path, content_type, filename).
    # Closing over the dict (not the records list) keeps the membership
    # check O(1) and avoids re-resolving anything during the turn.
    scope = {
        rec["attachment_id"]: rec
        for rec in (attachment_records or [])
        if rec and rec.get("attachment_id")
    }

    async def _handler(args):
        att_id = (args or {}).get("attachment_id")
        # ak-4tc: structured log line for post-hoc detection of the
        # hallucination class. If the LLM invented facts about an
        # attachment, this log confirms whether it even called the
        # read_attachment tool at all. Emitted BEFORE any early-exit
        # branch so grepping `read_attachment_invoked` gives a
        # complete audit trail per-turn.
        if logger:
            logger.info(
                "read_attachment_invoked user=%s att_id=%r scope_size=%d",
                user_id, att_id, len(scope),
            )

        if not att_id or not isinstance(att_id, str):
            return {
                "content": [{
                    "type": "text",
                    "text": "Error: attachment_id is required.",
                }],
                "isError": True,
            }

        # Membership check is the security boundary. Any id outside
        # this turn's resolved set — cross-user, expired, malformed,
        # or just made up by the model — gets the same generic error.
        rec = scope.get(att_id)
        if rec is None:
            if logger:
                logger.warning(
                    "read_attachment denied: user=%s requested_id=%r "
                    "(not in turn scope; scope_size=%d)",
                    user_id, att_id, len(scope),
                )
            return {
                "content": [{
                    "type": "text",
                    "text": (
                        f"Error: attachment '{att_id}' is not in this "
                        f"chat's attachment scope."
                    ),
                }],
                "isError": True,
            }

        # Defense in depth: re-resolve the record from disk to catch
        # the (unlikely) case where the file was swept out from under
        # us between the stream_chat pre-resolve and this handler call.
        # AttachmentNotFound here surfaces as an isError tool result
        # rather than a thrown exception that would crash the turn.
        try:
            current = attachment_resolve(user_id, att_id)
        except AttachmentNotFound:
            return {
                "content": [{
                    "type": "text",
                    "text": (
                        f"Error: attachment '{att_id}' is no longer "
                        f"available (may have been swept after upload)."
                    ),
                }],
                "isError": True,
            }
        path = current.get("path") or rec["path"]
        content_type = current.get("content_type") or rec.get("content_type")
        filename = current.get("filename") or rec.get("filename")

        try:
            with open(path, "rb") as fh:
                data_bytes = fh.read()
        except OSError as exc:
            return {
                "content": [{
                    "type": "text",
                    "text": f"Error reading attachment '{att_id}': {exc}",
                }],
                "isError": True,
            }

        # ak-1x4 pass 3 + ak-4tc: _content_block_for returns MCP shape
        # ({type,data,mimeType} for images, {type:text,text} for PDF
        # text extract). Raises _UnsupportedAttachmentMime on hard
        # extraction failure or a MIME outside the image / PDF families.
        # With sidecar-persisted content_type from save_upload reaching
        # us here, the MIME branch should be unreachable for any
        # legitimate upload; PDF-extraction failure is possible on
        # exotic files and surfaces as an isError tool result rather
        # than crashing the turn.
        try:
            block = _content_block_for(
                content_type, data_bytes, filename=filename,
            )
        except _UnsupportedAttachmentMime as exc:
            if logger:
                logger.warning(
                    "read_attachment unsupported MIME: user=%s id=%s "
                    "content_type=%r — %s",
                    user_id, att_id, content_type, exc,
                )
            return {
                "content": [{
                    "type": "text",
                    "text": (
                        f"Error: attachment '{att_id}' has an "
                        f"unsupported content type."
                    ),
                }],
                "isError": True,
            }

        return {
            "content": [block],
            "isError": False,
        }

    return SdkMcpTool(
        name=READ_ATTACHMENT_TOOL_NAME,
        description=(
            "Read a file the user attached to this message. Pass the "
            "attachment_id field shown in the [Attachments] block at "
            "the end of the user's message. For image attachments "
            "(PNG/JPEG/WebP/GIF) returns the file as a vision-native "
            "image content block. For PDF attachments returns the "
            "extracted text (ak-4tc MVP — SDK MCP bridge lacks a native "
            "document branch, so PDF text is the reliable shape today). "
            "Only attachments from this chat turn are accessible."
        ),
        input_schema={
            "type": "object",
            "properties": {
                "attachment_id": {
                    "type": "string",
                    "description": (
                        "The attachment_id (UUID) from the [Attachments] "
                        "block in the user's message."
                    ),
                    # uuid4 shape — defensive, the closure membership
                    # check is the real boundary.
                    "pattern": "^[a-fA-F0-9-]{8,64}$",
                },
            },
            "required": ["attachment_id"],
        },
        handler=_handler,
    )


class AgentService(BaseService):
    _instance = None

    def __new__(cls):
        if cls._instance is None:
            cls._instance = super(AgentService, cls).__new__(cls)
            cls.logger = Logger(__name__).get_logger()
        return cls._instance

    def __init__(self):
        super().__init__()
        self.investment_service = None
        self.transaction_service = None
        self.invoice_service = None
        self.customer_service = None
        self.dashboard_service = None
        self.mail_processor = None
        # ak-bq5: injected by app.py during route setup. None means
        # persistence is OFF (stateless legacy behavior) — code paths
        # guard on `self.conversation_service is not None` so the SDK
        # loop runs cleanly even without it (useful in offline tests
        # that mock the heavy app boot).
        self.conversation_service = None

    def set_services(self, investment_service=None, transaction_service=None,
                     invoice_service=None, customer_service=None,
                     dashboard_service=None, mail_processor=None,
                     conversation_service=None):
        """Called from app.py to inject existing service instances."""
        self.investment_service = investment_service
        self.transaction_service = transaction_service
        self.invoice_service = invoice_service
        self.customer_service = customer_service
        self.dashboard_service = dashboard_service
        self.mail_processor = mail_processor
        # ak-bq5: optional — only wired when chat persistence is active.
        if conversation_service is not None:
            self.conversation_service = conversation_service

    def stream_chat(self, agent_type, messages, user_id, confirmed_tools=None,
                    attachments=None, conversation_id=None):
        """
        Generator that yields SSE events for the agent chat.
        Uses claude_agent_sdk with MCP tools to process queries.

        Event types:
        - conversation_id: {"type": "conversation_id", "id": int}  (ak-bq5)
        - text: {"type": "text", "content": "..."}
        - tool_exec: {"type": "tool_exec", "tool": "..."}
        - confirm: {"type": "confirm", "tool": "...", "input": {...}, "message": "..."}
        - done: {"type": "done", "mutations": [...]}
        - error: {"type": "error", "message": "..."}

        attachments (ak-1x4): list of attachment_id strings previously
        returned by POST /agent/attach. Investment agent only. Each id
        is resolved to an absolute filesystem path under the user's
        per-user dir; paths are injected into the user's latest message
        for Claude's Read tool to pick up. The attachment files are
        deleted in the finally block — ephemeral per-turn lifecycle.
        Resolution failures (deleted, cross-user, malformed id) emit a
        single SSE error event and bail before the SDK runs.

        conversation_id (ak-bq5): optional int. When None AND a
        conversation_service is wired, a new conversation row is created
        from the last user message (title derived from first 60 chars)
        and the assigned id is emitted as the leading SSE event. When
        present, the user message is appended to that thread. When the
        id is provided but doesn't belong to the caller (or is
        soft-deleted), an SSE error event is yielded and the stream
        bails BEFORE the SDK runs — saves a costly model call on a
        scope-violating request.
        """
        confirmed_tools = confirmed_tools or []
        attachments = attachments or []
        tool_events = []  # Collected intermediate events (tool_exec, confirm)
        mutations = []
        # ak-bq5: track the active conversation_id + collected assistant
        # text so the finally block can persist whatever made it out
        # (full or partial) without depending on the happy-path tail.
        active_conversation_id = conversation_id
        persisted_assistant = False  # set True after a successful save
        # Track resolved attachment_ids so the finally block can clean
        # them up regardless of which exit path the SDK takes.
        attachment_ids_to_cleanup = []

        try:
            # ak-bq5: conversation_id pre-flight. Three cases:
            #
            # (a) no conversation_service wired → persistence OFF,
            #     legacy stateless behavior. Skip the whole block.
            # (b) caller passed a conversation_id → confirm it belongs
            #     to user_id (soft-deleted / cross-user → SSE error +
            #     bail without spinning up Claude).
            # (c) caller passed nothing → create a fresh conversation
            #     row, derive the title from the last user message,
            #     emit the new id as the leading SSE event so the FE
            #     can pin it.
            #
            # ak-bq5 pass-2 reviewer fix (hq-wisp-4tbck MAJOR): the
            # user message is NO LONGER persisted here. We defer it
            # until AFTER attachment_records is resolved so the row's
            # attachments_meta carries full descriptors (filename,
            # content_type, size) — not just the opaque id. On a
            # second device the /tmp file is gone, so the metadata
            # IS the only thing the FE has to render. See "user msg
            # persist (deferred)" below.
            last_user_text = ""
            if self.conversation_service is not None:
                last_user_text = self._extract_last_user_content(messages)
                if active_conversation_id is None:
                    try:
                        active_conversation_id = (
                            self.conversation_service.create_conversation(
                                user_id=user_id,
                                agent_type=agent_type,
                                first_user_message=last_user_text,
                            )
                        )
                    except ValueError as exc:
                        yield self._sse_event("error", {"message": str(exc)})
                        return
                    except Exception as exc:
                        # DB write failure is a hard error — don't run
                        # the SDK if we can't track the turn.
                        self.logger.exception(
                            "conversation create failed for user=%s "
                            "agent_type=%s", user_id, agent_type,
                        )
                        yield self._sse_event("error", {
                            "message": (
                                "Could not start a new conversation. "
                                "Please retry."
                            )
                        })
                        return
                else:
                    # Existing id — verify ownership before doing
                    # anything else. get_conversation returns None on
                    # miss / soft-delete / cross-user (no leak).
                    owned = self.conversation_service.get_conversation(
                        user_id, active_conversation_id,
                    )
                    if owned is None:
                        yield self._sse_event("error", {
                            "message": (
                                "Conversation not found. It may have "
                                "been deleted; please start a new chat."
                            )
                        })
                        return

                # Emit the id as the leading SSE event so the FE can
                # pin it for follow-up turns. User message persistence
                # is deferred — see below.
                yield self._sse_event("conversation_id", {
                    "id": active_conversation_id,
                })

            # ak-1x4: resolve attachments + spec-cap enforcement BEFORE
            # SDK setup. If anything fails we want to surface a single
            # clean error event and exit — no MCP server boot, no Claude
            # subprocess. Investment agent only; other agents ignore.
            attachment_records = []
            if attachments:
                if agent_type != "investment":
                    yield self._sse_event("error", {
                        "message": (
                            "Attachments are only supported in the "
                            "investment agent at this time."
                        )
                    })
                    return
                if len(attachments) > ATTACHMENTS_MAX_PER_MESSAGE:
                    yield self._sse_event("error", {
                        "message": (
                            f"Too many attachments: max "
                            f"{ATTACHMENTS_MAX_PER_MESSAGE} per message."
                        )
                    })
                    return
                for att_id in attachments:
                    try:
                        rec = attachment_resolve(user_id, att_id)
                    except AttachmentNotFound:
                        # Track the id anyway so the finally block can
                        # attempt cleanup (no-op if it doesn't exist).
                        attachment_ids_to_cleanup.append(att_id)
                        yield self._sse_event("error", {
                            "message": (
                                "Attachment not found or expired. Please "
                                "re-attach the file."
                            )
                        })
                        return
                    attachment_records.append(rec)
                    attachment_ids_to_cleanup.append(att_id)

            # ak-bq5 pass-2 reviewer fix (hq-wisp-4tbck MAJOR): persist
            # the inbound user message HERE — after attachment resolution
            # has succeeded. attachments_meta now carries the full
            # descriptor (id + filename + content_type + size) for each
            # attachment, not just the bare id. When a second device
            # reads the thread back, the /tmp file is gone (sweep ran),
            # so the metadata IS the FE's only render source.
            #
            # If resolution failed we've already SSE-errored and
            # returned above — we never reach this point with an
            # invalid attachment ref, so a persisted user row with full
            # descriptors is always trustworthy.
            if (
                self.conversation_service is not None
                and active_conversation_id is not None
            ):
                attachments_meta = (
                    [
                        {
                            "attachment_id": r.get("attachment_id"),
                            "filename": r.get("filename"),
                            "content_type": r.get("content_type"),
                            "size": r.get("size"),
                        }
                        for r in attachment_records
                    ]
                    if attachment_records else None
                )
                try:
                    self.conversation_service.append_message(
                        user_id=user_id,
                        conversation_id=active_conversation_id,
                        role="user",
                        content=last_user_text or "",
                        attachments_meta=attachments_meta,
                    )
                except Exception:
                    self.logger.exception(
                        "append user message failed for conv=%s",
                        active_conversation_id,
                    )
                    # Don't bail — the SDK loop can still run; the
                    # missing user-msg row is a non-fatal degradation
                    # we'd rather surface to logs than block on.

            config = get_agent_config(agent_type)

            # Build MCP tools from tool definitions
            sdk_tools = self._build_sdk_tools(
                agent_type, config["tools"], user_id,
                tool_events, mutations, confirmed_tools,
            )

            # ak-1x4 pass 2: when this turn carries attachments, register
            # the per-turn read_attachment MCP tool BEFORE creating the
            # MCP server. The tool closes over the resolved attachment
            # record set, so it can only return files in this user's
            # current scope — confinement lives in the closure, not in
            # CLI permission semantics (which are bypassed via
            # permission_mode='bypassPermissions').
            if attachment_records and agent_type == "investment":
                sdk_tools.append(make_read_attachment_tool(
                    user_id=user_id,
                    attachment_records=attachment_records,
                    logger=self.logger,
                ))

            # Create in-process MCP server
            mcp_server = create_sdk_mcp_server(
                name=MCP_SERVER_NAME,
                tools=sdk_tools,
            )

            # Format conversation history as prompt text
            prompt_text = self._format_conversation(messages)

            # ak-1x4 pass 2: inject the attachments block into the last
            # user message. The block lists each attachment_id (NOT the
            # on-disk path) so the agent calls read_attachment with the
            # opaque id — paths never leak to the chat surface.
            if attachment_records:
                attachments_block = attachments_format_for_prompt(
                    attachment_records
                )
                if attachments_block:
                    prompt_text = f"{prompt_text}\n{attachments_block}"

            # allowed_tools is required: with permission_mode='bypassPermissions'
            # and no list, built-in tools are silently unavailable and the agent
            # returns zero TextBlocks. Bash is intentionally NOT included for
            # user-facing chat — too broad under bypassPermissions. MCP tool
            # names are listed explicitly (defensive: SDK's "MCP tools always
            # available" contract is not relied on).
            #
            # WebSearch + WebFetch were previously listed defensively after the
            # April outage, but the three chat personas operate exclusively on
            # the Overseer's own data via MCP — they have no web-data need. The
            # combination of sonnet + WebSearch + WebFetch + tool-implying
            # system prompts was producing the empty-TextBlocks failure (see
            # SetGoldRate parity, Overseer-confirmed empty `done` SSE event).
            # Dropping them restores chat replies. Rate fetchers that genuinely
            # need WebSearch/WebFetch (rate.gold, rate.epf, rate.ppf,
            # stocks.ipo) get a separate fix in Family B.
            mcp_tool_names = [
                f"mcp__{MCP_SERVER_NAME}__{t['name']}" for t in config["tools"]
            ]

            # ak-1x4 pass 2 (reviewer hq-wisp-sh1to architectural fix):
            # the investment agent gets the per-turn read_attachment
            # MCP tool name when attachments are present. NO built-in
            # tool (Read, Bash, …) is added — the previous approach used
            # Claude Code's Read with a can_use_tool gate, but that gate
            # is inert under permission_mode='bypassPermissions'
            # (allowed_tools entries are pre-approved, so the CLI never
            # calls the gate). Confinement now lives entirely in the
            # read_attachment handler's closure (membership check on
            # the resolved attachment_id set).
            mcp_tool_names = compute_allowed_tools(
                agent_type, mcp_tool_names,
                has_attachments=bool(attachment_records),
            )

            # v3.2 instrumentation (hq-wisp-0kdnk): log the SDK call-boundary
            # signature so infra can verify which build is running and what
            # actually went to sonnet. Specifically:
            # - sys_prompt_sha / tools_json_sha: change when source changes,
            #   so infra can confirm post-vN build is running (gunicorn
            #   worker bytecode cache concern)
            # - has_get_invoices_rule: explicit Fix-2b marker (literal
            #   substring check in the live system_prompt)
            # - from_required / to_required: explicit Fix-2a markers from
            #   the create_invoice tool schema actually in the tool list
            # - messages_count + total_prompt_len: detect stuck history
            # NOTE: user message content is NOT logged (PII). Length only.
            sys_prompt = config["system_prompt"]
            sys_prompt_sha = hashlib.sha256(sys_prompt.encode()).hexdigest()[:12]
            tools_json = json.dumps(config["tools"], sort_keys=True, default=str)
            tools_json_sha = hashlib.sha256(tools_json.encode()).hexdigest()[:12]
            has_get_invoices_rule = "get_invoices(page=1, limit=1)" in sys_prompt
            ci_tool = next(
                (t for t in config["tools"] if t["name"] == "create_invoice"),
                None,
            )
            ci_from_required = []
            ci_to_required = []
            if ci_tool:
                try:
                    ci_data_props = ci_tool["input_schema"]["properties"]["data"]["properties"]
                    ci_from_required = ci_data_props["from"].get("required", [])
                    ci_to_required = ci_data_props["to"].get("required", [])
                except (KeyError, TypeError):
                    pass
            self.logger.info(
                "chat_request agent=chat.%s model=sonnet max_turns=%d "
                "perm=bypassPermissions sys_prompt_len=%d sys_prompt_sha=%s "
                "tools_count=%d tools_json_sha=%s has_get_invoices_rule=%s "
                "ci_from_required=%s ci_to_required=%s "
                "messages_count=%d total_prompt_len=%d",
                agent_type, MAX_TURNS, len(sys_prompt), sys_prompt_sha,
                len(mcp_tool_names), tools_json_sha, has_get_invoices_rule,
                ci_from_required, ci_to_required,
                len(messages), len(prompt_text),
            )

            options = ClaudeAgentOptions(
                system_prompt=config["system_prompt"],
                mcp_servers={MCP_SERVER_NAME: mcp_server},
                permission_mode="bypassPermissions",
                max_turns=MAX_TURNS,
                model="sonnet",
                allowed_tools=mcp_tool_names,
                # Stderr capture for cross-family diagnosis (hq-wisp-l2jfz,
                # hq-wisp-wzagg). v3.4: extra_args adds --debug-to-stderr so
                # CLI emits resolved model_id + request_id even on fast-fail
                # paths where the stderr buffer might otherwise be empty by
                # the time the SDK reaps the subprocess.
                stderr=_make_sdk_stderr_logger(f"chat.{agent_type}"),
                extra_args={"debug-to-stderr": None},
            )
            # ak-1x4 pass 2: can_use_tool is NOT set. The earlier scope
            # gate approach was architecturally inert under bypass mode
            # (allowed_tools entries are pre-approved → CLI never invokes
            # the gate). Confinement now lives inside the read_attachment
            # MCP tool's handler closure, which is mode-independent.

            # Run the query (blocking — collects all results then yields).
            # Streaming + tool_events make the run_query_collect wrapper
            # awkward, so we drive the loop manually and emit the agent_run
            # log line at the end via emit_agent_run.
            text_parts = []
            error_msg = None
            error_class = None
            turns = 0
            tool_calls = 0
            run_start = time.monotonic()

            async def run_query():
                nonlocal error_msg, error_class, turns, tool_calls

                # Use AsyncIterable prompt to avoid SDK bug where string prompts
                # close stdin before MCP control responses can be written back.
                async def make_prompt():
                    yield {
                        "type": "user",
                        "session_id": "",
                        "message": {"role": "user", "content": prompt_text},
                        "parent_tool_use_id": None,
                    }

                async for message in query(prompt=make_prompt(), options=options):
                    if isinstance(message, AssistantMessage):
                        turns += 1
                        if message.error:
                            error_msg = f"Claude error: {message.error}"
                            error_class = "assistant_error"
                            return
                        for block in message.content:
                            if isinstance(block, TextBlock):
                                text_parts.append(block.text)
                            elif isinstance(block, ToolUseBlock):
                                tool_calls += 1
                    elif isinstance(message, ResultMessage):
                        if message.is_error:
                            error_msg = message.result or "Query failed"
                            error_class = "result_error"

            try:
                anyio.run(run_query)
            except Exception as e:
                error_msg = f"SDK exception: {e}"
                error_class = e.__class__.__name__

            emit_agent_run(
                agent=f"chat.{agent_type}",
                model=options.model,
                turns=turns,
                tools_called=tool_calls,
                latency_ms=int((time.monotonic() - run_start) * 1000),
                status="error" if error_msg else "ok",
                error_class=error_class,
            )

            if error_msg:
                yield self._sse_event("error", {"message": error_msg})
                return

            # Yield collected tool/confirm events
            for event_type, data in tool_events:
                yield self._sse_event(event_type, data)

            # Yield final text
            full_text = "".join(text_parts)
            if not full_text and not tool_events:
                self.logger.warning(
                    "Agent returned ResultMessage with zero TextBlocks "
                    "(model=%s, max_turns=%s) — likely missing allowed_tools or model refused",
                    options.model, MAX_TURNS,
                )
                # Sticky-poison stopgap (v3): if we send nothing, the
                # frontend persists an empty assistant turn into
                # conversation history. sonnet then treats that empty
                # turn as a refusal signal and continues to silent-refuse
                # ALL subsequent turns (proven by "Hello?" failing after
                # a single create_invoice trigger — see hq-wisp-wf8kb).
                # A sentinel string breaks the loop AND surfaces the
                # failure to the user instead of black-holing it.
                full_text = (
                    "I hit an internal issue and couldn't reply. Please "
                    "rephrase or click 'New Chat' to reset."
                )
                # v3.2 marker (hq-wisp-0kdnk): explicit log line so infra
                # can grep sentinel-emitted instead of inferring from
                # response body size (41B vs 169B).
                self.logger.info(
                    "stopgap_sentinel_emitted agent=chat.%s "
                    "reason=zero_textblocks_no_tools",
                    agent_type,
                )
            if full_text:
                yield self._sse_event("text", {"content": full_text})

            # ak-bq5: persist the assistant message BEFORE yielding
            # `done`. This way the read-back endpoint shows the full
            # turn for any client that pings GET /agent/conversations/<id>
            # the moment the FE marks the message as final. We mark
            # persisted_assistant=True so the finally block doesn't
            # double-save with partial=True.
            if (
                self.conversation_service is not None
                and active_conversation_id is not None
            ):
                try:
                    self.conversation_service.append_message(
                        user_id=user_id,
                        conversation_id=active_conversation_id,
                        role="assistant",
                        content=full_text or "",
                        partial=False,
                    )
                    persisted_assistant = True
                except Exception:
                    self.logger.exception(
                        "append assistant message failed for conv=%s",
                        active_conversation_id,
                    )

            # Done
            yield self._sse_event("done", {"mutations": mutations})

        except Exception as e:
            self.logger.error(f"Agent error: {e}")
            yield self._sse_event("error", {"message": str(e)})
        finally:
            # ak-bq5: if the SDK loop crashed OR the generator was
            # closed early (FE disconnect after first text chunk),
            # persisted_assistant is still False AND we've collected
            # partial text. Save it with partial=True so the FE can
            # render a "stream interrupted" badge.
            #
            # We only fire this in the conversation_service-wired path
            # AND only when there's something worth saving (text or
            # tool events). Empty-turn case is already handled by the
            # happy path's v3 sentinel.
            if (
                self.conversation_service is not None
                and active_conversation_id is not None
                and not persisted_assistant
            ):
                partial_text = ""
                try:
                    partial_text = "".join(text_parts)
                except Exception:
                    partial_text = ""
                if partial_text or tool_events:
                    try:
                        self.conversation_service.append_message(
                            user_id=user_id,
                            conversation_id=active_conversation_id,
                            role="assistant",
                            content=partial_text,
                            partial=True,
                        )
                    except Exception:
                        self.logger.exception(
                            "append partial assistant message failed "
                            "for conv=%s", active_conversation_id,
                        )

            # ak-1x4: ephemeral per-turn cleanup. Best-effort rmtree of
            # any attachment subtrees this stream_chat call resolved (or
            # tried to resolve). Failures are swallowed inside
            # attachment_cleanup; the sweeper picks up orphans.
            # Generator finally fires whether the generator is exhausted
            # OR closed early (FE disconnect, error), so a flaky upstream
            # never leaks disk.
            for att_id in attachment_ids_to_cleanup:
                try:
                    attachment_cleanup(user_id, att_id)
                except Exception:
                    self.logger.exception(
                        "attachment cleanup failed for %s (id=%s)",
                        user_id, att_id,
                    )

    @staticmethod
    def _extract_last_user_content(messages):
        """ak-bq5: pull the most recent user message's content for
        title derivation + persistence. Returns "" if absent or shaped
        unexpectedly so callers can pass it directly to
        derive_title (which itself falls back to a default)."""
        if not messages:
            return ""
        # Scan from the end — the FE typically sends history with the
        # newest user turn at messages[-1], but be tolerant of an
        # `assistant`-trailing payload.
        for msg in reversed(messages):
            if not isinstance(msg, dict):
                continue
            if msg.get("role") != "user":
                continue
            content = msg.get("content", "")
            return content if isinstance(content, str) else str(content)
        return ""

    def _build_sdk_tools(self, agent_type, tool_defs, user_id,
                         tool_events, mutations, confirmed_tools):
        """Build SdkMcpTool objects from tool definitions with handler closures."""
        sdk_tools = []

        for tool_def in tool_defs:
            tool_name = tool_def["name"]

            # Use default-arg capture to bind loop variable
            async def handler(args, _tn=tool_name, _at=agent_type):
                # Check if destructive and not confirmed
                if _tn in DESTRUCTIVE_TOOLS and _tn not in confirmed_tools:
                    msg = AgentService._get_confirmation_message(_tn, args)
                    tool_events.append(("confirm", {
                        "tool": _tn,
                        "input": args,
                        "message": msg,
                    }))
                    return {
                        "content": [{
                            "type": "text",
                            "text": (
                                f"Action cancelled — user confirmation required for {_tn}. "
                                "Please inform the user and ask them to confirm."
                            ),
                        }],
                        "is_error": True,
                    }

                # Record tool execution event
                tool_events.append(("tool_exec", {"tool": _tn}))

                # Execute via existing services
                result = execute_tool(
                    _at, _tn, args, user_id,
                    investment_service=self.investment_service,
                    transaction_service=self.transaction_service,
                    invoice_service=self.invoice_service,
                    customer_service=self.customer_service,
                    dashboard_service=self.dashboard_service,
                    mail_processor=self.mail_processor,
                )

                # ak-e7g: distinguish handler-layer validation failures
                # (surfaced with is_error=True so the LLM can retry with
                # the correct shape) from happy-path results. The
                # TOOL_ERROR_KEY sentinel is the private contract between
                # execute_tool and this handler — see
                # services/agent_tool_executor.py.
                if isinstance(result, dict) and result.get(TOOL_ERROR_KEY):
                    return {
                        "content": [{
                            "type": "text",
                            "text": result.get("message", "Tool validation error"),
                        }],
                        "is_error": True,
                    }

                # Track mutations (only on non-error results)
                if _tn in MUTATION_TOOLS:
                    mutations.append(_tn)

                return {
                    "content": [{
                        "type": "text",
                        "text": json.dumps(result, default=str),
                    }]
                }

            sdk_tools.append(SdkMcpTool(
                name=tool_name,
                description=tool_def["description"],
                input_schema=tool_def["input_schema"],
                handler=handler,
            ))

        return sdk_tools

    @staticmethod
    def _format_conversation(messages):
        """Format message history into a prompt string for claude_agent_sdk."""
        if not messages:
            return ""

        # Single message — return content directly
        if len(messages) == 1:
            content = messages[0].get("content", "")
            return content if isinstance(content, str) else str(content)

        # Multi-turn — include history
        history_parts = []
        for msg in messages[:-1]:
            role = msg.get("role", "user")
            content = msg.get("content", "")
            if role == "partial_assistant":
                continue
            if isinstance(content, str):
                prefix = "User" if role == "user" else "Assistant"
                history_parts.append(f"{prefix}: {content}")

        latest_msg = messages[-1]
        latest_content = latest_msg.get("content", "")
        if not isinstance(latest_content, str):
            latest_content = str(latest_content)

        if history_parts:
            history = "\n\n".join(history_parts)
            return (
                f"Previous conversation:\n{history}\n\n"
                f"User's latest message: {latest_content}"
            )

        return latest_content

    @staticmethod
    def _sse_event(event_type, data):
        """Format an SSE event string."""
        payload = {"type": event_type, **data}
        return f"data: {json.dumps(payload, default=str)}\n\n"

    @staticmethod
    def _get_confirmation_message(tool_name, tool_input):
        """Generate a human-readable confirmation message for destructive tools."""
        messages = {
            "delete_single_investment": f"Delete investment record (buy ID: {tool_input.get('buy_id', 'unknown')})?",
            "delete_all_investments": f"Delete ALL {tool_input.get('service_type', '')} investments? This cannot be undone.",
            "delete_file": f"Delete file {tool_input.get('file_id', 'unknown')} and all its transactions?",
            "delete_invoice": f"Delete invoice {tool_input.get('invoice_number', 'unknown')}?",
            "delete_customer": f"Delete customer (ID: {tool_input.get('customer_id', 'unknown')})?",
            "scan_emails_for_transactions": "Scan Gmail for transaction emails? This may take several minutes.",
            "scan_statements": "Scan Gmail for bank statements? This may take several minutes.",
            "sync_kite_holdings": "Sync holdings from Kite Connect into local database?",
            "process_mail_pipeline": "Scan Gmail for all financial emails and process them? This may take several minutes.",
        }
        return messages.get(tool_name, f"Execute {tool_name}?")
