"""ak-bq5: agent_conversations — cross-device chat persistence header table.

One row per conversation thread the user has had with one of the
three domain agents (investment, transaction, freelance).

Lifecycle:
  - Created on the first POST /agent/chat that comes in without a
    conversation_id, OR via explicit POST /agent/conversations.
  - title is derived from the first 60 chars of the first user message
    (newlines stripped, whitespace collapsed) at creation time. No LLM
    call.
  - updated_at bumped on every message append.
  - Soft delete via deleted_at; the list endpoint filters this out.

Scope guards (per dispatch hq-wisp-rr15b):
  - All endpoints filter by user_id == g.firebase_id — cross-user
    isolation is non-negotiable.
  - Title rename, LLM titling, attachment durability all OUT of scope
    here; flag follow-up beads if desired.
"""

from sqlalchemy import (
    Column, String, Integer, DateTime, ForeignKey, Index, func,
)
from sqlalchemy.orm import relationship

from models.Base import Base


class AgentConversation(Base):
    """Header row for a single agent chat thread.

    The (user_id, agent_type, updated_at DESC) compound index makes
    the list query — "what conversations does user X have for the
    investment agent, most recent first" — an index-only scan.
    """

    __tablename__ = 'agent_conversations'

    id = Column(Integer, primary_key=True, autoincrement=True)
    # firebase_id (alphanumeric); CASCADE so when a user is removed
    # in dev/test their conversations follow.
    user_id = Column(
        String(100),
        ForeignKey('users.userID', ondelete='CASCADE'),
        nullable=False,
    )
    # Matches the agent_type validation in controllers/agentEP.py.
    # Stored as a free string (not Enum) so adding a 4th agent later
    # doesn't require a schema migration.
    agent_type = Column(String(40), nullable=False)
    # 60-char cap is enforced at write time (see
    # agentConversationService.derive_title); 120 here gives headroom
    # for an LLM-generated title in a later iteration without re-migrate.
    title = Column(String(120), nullable=False, default="New conversation")
    created_at = Column(DateTime, nullable=False, server_default=func.now())
    # updated_at bumped on every append_message; the list view orders by this
    # so the most-recently-active thread surfaces first.
    updated_at = Column(
        DateTime, nullable=False,
        server_default=func.now(),
        onupdate=func.now(),
    )
    # Soft delete (per scope doc decision 4). NULL == live. List query
    # filters this. Hard delete reserved for a future cleanup job.
    deleted_at = Column(DateTime, nullable=True)

    messages = relationship(
        'AgentMessage',
        back_populates='conversation',
        cascade='all, delete-orphan',
        order_by='AgentMessage.id',
    )

    __table_args__ = (
        # Hot path: list query orders by (user_id, agent_type, updated_at DESC).
        # `updated_at` is the leftmost-DESC column so the index serves
        # the ORDER BY directly. Single composite index covers the
        # common filter + sort.
        Index(
            'ix_agent_conv_user_type_updated',
            'user_id', 'agent_type', 'updated_at',
        ),
        # Quick lookup by id within a user — used by the GET-single
        # and DELETE-single endpoints which scope to user_id.
        Index('ix_agent_conv_user', 'user_id'),
    )

    def to_summary_dict(self, message_count=None):
        """Shape returned by GET /agent/conversations?agent_type=X.

        message_count is computed by the service layer (saves a
        round-trip per row when batched in the caller). Pass None to
        omit it (e.g. for the single-conversation GET).
        """
        out = {
            "id": self.id,
            "agent_type": self.agent_type,
            "title": self.title,
            "created_at": self.created_at.isoformat() if self.created_at else None,
            "updated_at": self.updated_at.isoformat() if self.updated_at else None,
        }
        if message_count is not None:
            out["msg_count"] = message_count
        return out
