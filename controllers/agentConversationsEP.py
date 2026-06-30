"""ak-bq5: REST endpoints for cross-device agent chat persistence.

Routes registered in app.py:
  GET    /agent/conversations?agent_type=X      → list
  GET    /agent/conversations/<id>              → full thread
  POST   /agent/conversations                   → explicit create
  DELETE /agent/conversations/<id>              → soft delete

All endpoints filter by g.firebase_id (set by the before_request
middleware). Non-owner / non-existent / soft-deleted → 404 with a
generic "not found" message — never leak existence via a 403.
"""

from flask import g, jsonify, request

from services.agentConversationService import (
    AgentConversationService,
    VALID_AGENT_TYPES,
)
from utils.logger import Logger


class AgentConversationsController:
    def __init__(self, service: AgentConversationService):
        self.service = service
        self.logger = Logger(__name__).get_logger()

    @Logger.standardLogger
    def list(self):
        """
        GET /agent/conversations
        Optional query: agent_type=investment|transaction|freelance
        Returns: {conversations: [{id, agent_type, title, created_at,
                                   updated_at, msg_count}, ...]}
        """
        user_id = g.get("firebase_id")
        if not user_id:
            return jsonify({"error": "Unauthorized"}), 401

        agent_type = request.args.get("agent_type") or None
        if agent_type and agent_type not in VALID_AGENT_TYPES:
            return jsonify({
                "error": (
                    f"Invalid agent_type {agent_type!r}; "
                    f"expected one of {list(VALID_AGENT_TYPES)}"
                )
            }), 400

        try:
            convs = self.service.list_conversations(user_id, agent_type=agent_type)
        except Exception:
            self.logger.exception("agent_conversations: list failed")
            return jsonify({"error": "Internal server error"}), 500
        return jsonify({"conversations": convs}), 200

    @Logger.standardLogger
    def get(self, conversation_id):
        """
        GET /agent/conversations/<conversation_id>
        Returns: {id, agent_type, title, created_at, updated_at,
                  msg_count, messages: [{id, role, content,
                  attachments_meta, partial, ts}, ...]}
        404 if not owned by current user (cross-user lookup looks
        identical to a true miss — no existence leak).
        """
        user_id = g.get("firebase_id")
        if not user_id:
            return jsonify({"error": "Unauthorized"}), 401

        try:
            payload = self.service.get_conversation(user_id, conversation_id)
        except Exception:
            self.logger.exception(
                "agent_conversations: get failed for id=%s", conversation_id
            )
            return jsonify({"error": "Internal server error"}), 500

        if payload is None:
            return jsonify({"error": "Conversation not found"}), 404
        return jsonify(payload), 200

    @Logger.standardLogger
    def create(self):
        """
        POST /agent/conversations
        Body: {agent_type, title?}
        Returns: {id, agent_type, title, created_at, updated_at, msg_count: 0}

        Optional endpoint per dispatch — the FE typically lets
        /agent/chat auto-create on first message. Exposed here for
        explicit "new conversation" UI affordances.
        """
        user_id = g.get("firebase_id")
        if not user_id:
            return jsonify({"error": "Unauthorized"}), 401

        data = request.get_json(force=True, silent=True) or {}
        agent_type = data.get("agent_type")
        if not agent_type:
            return jsonify({"error": "agent_type is required"}), 400
        if agent_type not in VALID_AGENT_TYPES:
            return jsonify({
                "error": (
                    f"Invalid agent_type {agent_type!r}; "
                    f"expected one of {list(VALID_AGENT_TYPES)}"
                )
            }), 400

        try:
            cid = self.service.create_conversation(
                user_id, agent_type, title=data.get("title"),
            )
        except ValueError as exc:
            return jsonify({"error": str(exc)}), 400
        except Exception:
            self.logger.exception("agent_conversations: create failed")
            return jsonify({"error": "Internal server error"}), 500

        # Round-trip to surface the auto-set timestamps + canonicalized
        # title to the caller.
        payload = self.service.get_conversation(user_id, cid)
        return jsonify(payload), 201

    @Logger.standardLogger
    def delete(self, conversation_id):
        """
        DELETE /agent/conversations/<conversation_id>
        Soft delete (deleted_at = now). 404 if not owned by current
        user. Idempotent on a second call to a deleted conversation
        (returns 404 the second time — that's correct, the row is
        no longer in the live set).
        """
        user_id = g.get("firebase_id")
        if not user_id:
            return jsonify({"error": "Unauthorized"}), 401

        try:
            ok = self.service.soft_delete(user_id, conversation_id)
        except Exception:
            self.logger.exception(
                "agent_conversations: soft_delete failed for id=%s",
                conversation_id,
            )
            return jsonify({"error": "Internal server error"}), 500

        if not ok:
            return jsonify({"error": "Conversation not found"}), 404
        return jsonify({"message": "Conversation deleted"}), 200
