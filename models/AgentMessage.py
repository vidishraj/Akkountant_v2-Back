"""ak-bq5: agent_messages — one row per chat turn (user / assistant / tool).

Ordering: rows are ordered by id (autoincrement) within a conversation.
We don't rely on `ts` for ordering because two rows could share an
identical millisecond timestamp; id is monotonic per row insert.

content stores the message text verbatim. For the assistant role this
is the concatenation of TextBlocks the SDK returned (matching what
agentService.stream_chat yields as the "text" event payload).

attachments_meta stores DESCRIPTORS only — NOT the bytes. See dispatch
hq-wisp-rr15b:
  > Persisted messages keep attachment metadata (filename, type, size,
  > sha256) but NOT the bytes. Existing /tmp/akkountant-agent-attachments/...
  > sweep behavior unchanged.
The FE renders "[attachment expired]" when the referenced uuid is no
longer on disk.

partial flag: set TRUE if the SSE stream was interrupted mid-assistant-
turn (e.g. FE disconnect, SDK error after some text). The FE can show a
"stream interrupted" badge so the user knows the assistant message is
incomplete.
"""

from sqlalchemy import (
    Column, String, Integer, DateTime, ForeignKey, Boolean, Text,
    JSON, Index, func,
)
from sqlalchemy.orm import relationship

from models.Base import Base


# Roles match the Anthropic message-shape vocabulary. 'tool' isn't
# emitted by the current pipeline (we shape MCP tool results back into
# assistant turns), but the column accepts it so future tool-event
# persistence doesn't require a schema migration.
AGENT_MESSAGE_ROLES = ('user', 'assistant', 'tool')


class AgentMessage(Base):
    __tablename__ = 'agent_messages'

    id = Column(Integer, primary_key=True, autoincrement=True)
    conversation_id = Column(
        Integer,
        ForeignKey('agent_conversations.id', ondelete='CASCADE'),
        nullable=False,
    )
    # Stored as a free string (not Enum) for the same forward-compat
    # reason as agent_type on the conversation header. Validated at
    # write time by agentConversationService.append_message.
    role = Column(String(20), nullable=False)
    content = Column(Text, nullable=False)
    # JSON column: a list of {filename, mime, size, sha256, attachment_id}
    # descriptors when the user attached files this turn. NULL when no
    # attachments. We do NOT store the on-disk path or the bytes —
    # ephemeral sweep semantics from ak-1x4 apply.
    attachments_meta = Column(JSON, nullable=True)
    # See class docstring on `partial`. Default FALSE.
    partial = Column(Boolean, nullable=False, default=False, server_default='0')
    ts = Column(DateTime, nullable=False, server_default=func.now())

    conversation = relationship('AgentConversation', back_populates='messages')

    __table_args__ = (
        # GET /agent/conversations/<id> loads all messages for a given
        # conversation ordered by id. Indexed for that.
        Index('ix_agent_msg_conv_id', 'conversation_id', 'id'),
    )

    def to_dict(self):
        """Shape returned alongside GET /agent/conversations/<id>."""
        return {
            "id": self.id,
            "role": self.role,
            "content": self.content,
            "attachments_meta": self.attachments_meta,  # already JSON-typed
            "partial": bool(self.partial),
            "ts": self.ts.isoformat() if self.ts else None,
        }
