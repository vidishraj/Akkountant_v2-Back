"""
SSE endpoint for the AI agent chat + attachment upload (ak-1x4).
"""

from flask import request, jsonify, Response, stream_with_context, g
from services.agentService import AgentService
from utils.agent_attachments import (
    MAX_PER_MESSAGE,
    save_upload,
    sweep_stale,
)
from utils.logger import Logger


class AgentController:
    def __init__(self, agent_service: AgentService):
        self.agent_service = agent_service
        self.logger = Logger(__name__).get_logger()

    @Logger.standardLogger
    def chat(self):
        """
        POST /agent/chat
        Body: {agent_type: str, messages: list, confirmed_tools?: list,
               attachments?: list[str], conversation_id?: int}
        Returns: SSE stream

        attachments (ak-1x4): list of attachment_ids previously returned
        by POST /agent/attach. Investment agent only.

        conversation_id (ak-bq5): optional int — when present, the user
        message is appended to the existing thread + the assistant
        reply is appended after the stream completes. When absent, the
        service layer creates a new conversation row, derives a title
        from the first user message, and emits a leading SSE event
        `{"type":"conversation_id","id": ...}` so the FE can pin the
        id for follow-up turns.
        """
        data = request.get_json(force=True)
        agent_type = data.get("agent_type")
        messages = data.get("messages", [])
        confirmed_tools = data.get("confirmed_tools", [])
        attachments = data.get("attachments", []) or []
        conversation_id = data.get("conversation_id")  # ak-bq5
        user_id = g.get("firebase_id")

        if not agent_type:
            return jsonify({"error": "agent_type is required"}), 400
        if agent_type not in ("investment", "transaction", "freelance"):
            return jsonify({"error": f"Invalid agent_type: {agent_type}"}), 400
        if not messages:
            return jsonify({"error": "messages cannot be empty"}), 400

        # Light pre-flight on the attachments field shape. Deeper validation
        # (cross-user, expired, agent-type-supports-attachments) happens
        # inside agentService.stream_chat where it can yield SSE error
        # events the FE already knows how to render.
        if attachments and not isinstance(attachments, list):
            return jsonify({"error": "attachments must be a list of strings"}), 400
        if len(attachments) > MAX_PER_MESSAGE:
            return jsonify({
                "error": (
                    f"Too many attachments: max {MAX_PER_MESSAGE} per message"
                )
            }), 400

        # ak-bq5: optional conversation_id sanity — must be int-coercible.
        # Membership scope is validated inside the service layer (404 on
        # miss / cross-user / soft-deleted; surfaced as an SSE error).
        if conversation_id is not None:
            try:
                conversation_id = int(conversation_id)
            except (TypeError, ValueError):
                return jsonify({
                    "error": "conversation_id must be an integer"
                }), 400

        def generate():
            for event in self.agent_service.stream_chat(
                agent_type=agent_type,
                messages=messages,
                user_id=user_id,
                confirmed_tools=confirmed_tools,
                attachments=attachments,
                conversation_id=conversation_id,
            ):
                yield event

        response = Response(
            stream_with_context(generate()),
            content_type="text/event-stream",
        )
        response.headers["Cache-Control"] = "no-cache"
        response.headers["X-Accel-Buffering"] = "no"
        return response

    # ak-1x4 reviewer fix: per-route ceiling for the attach endpoint.
    # 10 MB body + multipart overhead (boundary + headers per part).
    # Enforced by the Content-Length pre-check below; this constant is
    # the source of truth (mirrored in utils.agent_attachments.MAX_BYTES
    # for the byte-stream cap).
    _ATTACH_REQUEST_MAX = 12 * 1024 * 1024  # 12 MB

    @Logger.standardLogger
    def attach(self):
        """
        POST /agent/attach (ak-1x4)

        Accepts multipart/form-data with a single 'file' field. Validates
        type + size, saves to /tmp/akkountant-agent-attachments/<user_id>
        /<uuid>/<filename>, returns the attachment_id for the FE to pass
        on the subsequent /agent/chat request.

        FE calls this N times to attach N files (single-file-per-request
        keeps the endpoint simple + lets the FE show per-file progress
        without parsing a batch response).

        Auth: X-Firebase-ID (existing middleware, sets g.firebase_id).

        Size policy (reviewer fix hq-wisp-51ovh MAJOR 1): the Flask app
        no longer carries a global MAX_CONTENT_LENGTH, so legitimate
        large uploads on /files/upload + bank-statement endpoints are
        not collateral-capped. The cap is enforced here instead via a
        Content-Length pre-check, with utils.agent_attachments.save_upload
        as a streaming byte-counter safety net for chunked / missing-
        header requests.
        """
        user_id = g.get("firebase_id")

        # ak-1x4 reviewer fix MINOR: drop the unreachable defensive 401
        # — app.before_request already 401s on missing X-Firebase-ID.

        # Per-route size ceiling (reviewer fix MAJOR 1). Content-Length
        # may be absent on chunked requests; in that case we lean on
        # save_upload's streaming cap. Content-Length present + too big
        # → reject immediately so we never start the multipart parse.
        content_length = request.content_length
        if content_length is not None and content_length > self._ATTACH_REQUEST_MAX:
            return jsonify({
                "error": (
                    f"Request exceeds attachment size cap "
                    f"({self._ATTACH_REQUEST_MAX // (1024 * 1024)}MB)"
                )
            }), 413

        if "file" not in request.files:
            return jsonify({"error": "No file provided"}), 400

        file_storage = request.files["file"]
        if not file_storage or not file_storage.filename:
            return jsonify({"error": "No file provided"}), 400

        # Sweep stale uploads opportunistically — cheap (per-user shallow
        # walk) and self-healing. We do this BEFORE the new write so an
        # accumulation of stale files can't keep us from a fresh upload
        # under tight disk constraints.
        try:
            swept = sweep_stale()
            if swept:
                self.logger.info(
                    "agent_attachments: swept %d stale upload(s)", swept
                )
        except Exception:
            # Sweeper failures are non-fatal; log and continue.
            self.logger.exception("agent_attachments: sweep_stale failed")

        try:
            record = save_upload(user_id, file_storage)
        except ValueError as exc:
            # Includes: wrong MIME, oversized, empty, magic-byte mismatch.
            self.logger.info(
                "agent_attachments: rejected upload user=%s reason=%s",
                user_id, exc,
            )
            return jsonify({"error": str(exc)}), 400
        except Exception:
            self.logger.exception("agent_attachments: unexpected upload error")
            return jsonify({"error": "Internal server error"}), 500

        # Spec response shape — never leak the on-disk path; FE only
        # needs the id to reference on the next /agent/chat call.
        return jsonify({
            "attachment_id": record["attachment_id"],
            "filename": record["filename"],
            "size": record["size"],
            "content_type": record["content_type"],
        }), 201
