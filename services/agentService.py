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
from services.agent_tool_executor import execute_tool
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


# Per-allowed-MIME content-block shape for the MCP tool result. Anthropic
# accepts type=image source=base64 for raster images and type=document
# source=base64 for PDFs in tool_result content arrays — Claude's vision
# stack inspects them natively. Anything outside our upload allowlist
# raises so the handler surfaces an isError tool result rather than
# falling back to a text dump of the bytes.
class _UnsupportedAttachmentMime(Exception):
    """Internal signal that _content_block_for got a MIME outside the
    image / application-pdf families. Should be unreachable for any
    attachment that came through save_upload (which enforces
    ALLOWED_MIME + magic-byte sniffing + persists the validated type
    via the sidecar). Raised so the read_attachment handler can return
    isError instead of letting an unvalidated type reach the agent."""


def _content_block_for(content_type, data_bytes):
    """Build the Anthropic content block for a given (mime, bytes) pair.

    Used by the read_attachment SdkMcpTool handler. Encoded inline so
    the tool handler is a single round-trip — Claude receives the
    actual bytes (vision-native for image + PDF), not just a description.

    ak-1x4 pass 3 (reviewer hq-wisp-z1fyr MAJOR): fails closed instead
    of degrading to a utf-8-replace text block. The text-fallback was a
    prompt-injection channel because:
      1. resolve() previously re-derived content_type from the user-
         controlled filename extension; a real PDF saved as "blob"
         resolved to application/octet-stream.
      2. octet-stream hit the text branch here.
      3. data_bytes.decode("utf-8", errors="replace") shoved the PDF
         (potentially with embedded crafted ASCII) into the prompt
         instruction channel as TEXT, bypassing Claude's vision sandbox.
    With pass 3's sidecar persistence in agent_attachments.resolve()
    the content_type that reaches us is guaranteed to be on
    ALLOWED_MIME, so this raise is a defense-in-depth wall, not the
    primary protection.
    """
    if content_type and content_type.startswith("image/"):
        return {
            "type": "image",
            "source": {
                "type": "base64",
                "media_type": content_type,
                "data": _base64.b64encode(data_bytes).decode("ascii"),
            },
        }
    if content_type == "application/pdf":
        return {
            "type": "document",
            "source": {
                "type": "base64",
                "media_type": content_type,
                "data": _base64.b64encode(data_bytes).decode("ascii"),
            },
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

        # ak-1x4 pass 3: _content_block_for now raises on any non-image /
        # non-application-pdf MIME instead of degrading to a text dump
        # of the raw bytes. With the sidecar-persisted content_type from
        # save_upload reaching us here, this should be unreachable for
        # any legitimate upload; but defense-in-depth surface an isError
        # rather than crashing the turn if it does fire.
        try:
            block = _content_block_for(content_type, data_bytes)
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
            "the end of the user's message. Returns the file contents "
            "as a vision-native content block (image for image/* MIMEs, "
            "document for application/pdf). Only attachments from this "
            "chat turn are accessible."
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

    def set_services(self, investment_service=None, transaction_service=None,
                     invoice_service=None, customer_service=None,
                     dashboard_service=None, mail_processor=None):
        """Called from app.py to inject existing service instances."""
        self.investment_service = investment_service
        self.transaction_service = transaction_service
        self.invoice_service = invoice_service
        self.customer_service = customer_service
        self.dashboard_service = dashboard_service
        self.mail_processor = mail_processor

    def stream_chat(self, agent_type, messages, user_id, confirmed_tools=None,
                    attachments=None):
        """
        Generator that yields SSE events for the agent chat.
        Uses claude_agent_sdk with MCP tools to process queries.

        Event types:
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
        """
        confirmed_tools = confirmed_tools or []
        attachments = attachments or []
        tool_events = []  # Collected intermediate events (tool_exec, confirm)
        mutations = []
        # Track resolved attachment_ids so the finally block can clean
        # them up regardless of which exit path the SDK takes.
        attachment_ids_to_cleanup = []

        try:
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

            # Done
            yield self._sse_event("done", {"mutations": mutations})

        except Exception as e:
            self.logger.error(f"Agent error: {e}")
            yield self._sse_event("error", {"message": str(e)})
        finally:
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

                # Track mutations
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
