"""
SSE endpoint for the AI agent chat.
"""

from flask import request, jsonify, Response, stream_with_context, g
from services.agentService import AgentService
from utils.logger import Logger


class AgentController:
    def __init__(self, agent_service: AgentService):
        self.agent_service = agent_service
        self.logger = Logger(__name__).get_logger()

    @Logger.standardLogger
    def chat(self):
        """
        POST /agent/chat
        Body: {agent_type: str, messages: list, confirmed_tools?: list}
        Returns: SSE stream
        """
        data = request.get_json(force=True)
        agent_type = data.get("agent_type")
        messages = data.get("messages", [])
        confirmed_tools = data.get("confirmed_tools", [])
        user_id = g.get("firebase_id")

        if not agent_type:
            return jsonify({"error": "agent_type is required"}), 400
        if agent_type not in ("investment", "transaction", "freelance"):
            return jsonify({"error": f"Invalid agent_type: {agent_type}"}), 400
        if not messages:
            return jsonify({"error": "messages cannot be empty"}), 400

        def generate():
            for event in self.agent_service.stream_chat(
                agent_type=agent_type,
                messages=messages,
                user_id=user_id,
                confirmed_tools=confirmed_tools,
            ):
                yield event

        response = Response(
            stream_with_context(generate()),
            content_type="text/event-stream",
        )
        response.headers["Cache-Control"] = "no-cache"
        response.headers["X-Accel-Buffering"] = "no"
        return response
