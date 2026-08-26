"""ak-9dz: agent_file_attachments — persistent per-conversation storage
for files agents write during a chat turn.

Motivation: Freelance / Investment / WealthDigest agents can write files
via bash/write during a turn (CSV exports, PDF renders, JSON dumps). Prior
UX cited a server-local path like `/tmp/july_2026_invoices.csv` — dead-
end because the browser can't reach it. The `attach_file_to_chat` MCP
tool copies the file into user-scoped served storage and inserts a row
here; the download endpoint (GET /api/user-files/<uuid>) auth-checks
the current firebase_id owns the file and streams it back with the
right Content-Disposition.

Nullable conversation_id / message_id lets attachments outside a
specific chat turn (e.g. future scheduled-task exports) coexist in the
same table without a schema fork. For v1 both are populated when the
tool fires inside an agent-chat turn (conversation_id from the active
turn's AgentConversation; message_id is NULL because the assistant
message row is persisted AFTER the tool loop resolves).

MIGRATION (standalone table — no ALTER on existing):
    CREATE TABLE agent_file_attachments (
      uuid            VARCHAR(36) PRIMARY KEY,
      user_id         VARCHAR(100) NOT NULL,
      conversation_id INT NULL,
      message_id      INT NULL,
      display_name    VARCHAR(255) NOT NULL,
      mime_type       VARCHAR(100) NULL,
      size_bytes      BIGINT NOT NULL,
      disk_path       VARCHAR(500) NOT NULL,
      created_at      TIMESTAMP NOT NULL DEFAULT CURRENT_TIMESTAMP,
      INDEX idx_afa_user_conv (user_id, conversation_id),
      INDEX idx_afa_user_msg (user_id, message_id)
    );

`db.create_all()` in app._setup_database picks this up on first boot
post-deploy (new table, not a column addition — no ALTER required).
Pre-migration deploy window: endpoint returns 404 (table missing → the
initial SELECT errors, service catches + returns None; controller maps
to 404). Downloads unavailable until create_all runs.
"""

from sqlalchemy import (
    Column, String, Integer, BigInteger, DateTime, ForeignKey, Index, func,
)
from sqlalchemy.orm import relationship

from models.Base import Base


class AgentFileAttachment(Base):
    """One row per agent-produced file stored under user-scoped served
    storage. Downloaded via GET /api/user-files/<uuid> with cross-user
    isolation on `user_id == g.firebase_id`."""

    __tablename__ = 'agent_file_attachments'

    # UUID4 hex string — 36 chars including hyphens. Ties the row to
    # the on-disk filename prefix (uuid_<sanitized_display_name>) so a
    # download by uuid can locate + serve the bytes.
    uuid = Column(String(36), primary_key=True)
    # firebase_id (alphanumeric); CASCADE removes attachments when the
    # user row is deleted in dev/test (mirrors AgentConversation shape).
    user_id = Column(
        String(100),
        ForeignKey('users.userID', ondelete='CASCADE'),
        nullable=False,
    )
    # Optional link to the chat conversation the file was produced in.
    # Nullable so a Phase-2 non-chat producer (scheduled task output)
    # can also use this table. CASCADE follows conversation delete.
    conversation_id = Column(
        Integer,
        ForeignKey('agent_conversations.id', ondelete='CASCADE'),
        nullable=True,
    )
    # Optional link to the specific assistant message. Nullable because
    # the tool fires BEFORE the assistant message row is persisted
    # (assistant persistence happens post-turn). Populated by a
    # follow-up sweep if we ever want to backfill the linkage.
    message_id = Column(
        Integer,
        ForeignKey('agent_messages.id', ondelete='SET NULL'),
        nullable=True,
    )
    # Display filename shown in the chat card + Content-Disposition on
    # download. Sanitized at write time (see attach_file_to_chat) —
    # no path traversal / shell chars. Preserved for the download's
    # filename attribute so the browser saves it as the agent intended.
    display_name = Column(String(255), nullable=False)
    # Content-Type header value for the download response. Detected via
    # mimetypes.guess_type at attach time; NULL means unknown → default
    # to application/octet-stream at serve time.
    mime_type = Column(String(100), nullable=True)
    # File size in bytes for the chat card render (human-readable size)
    # + the Content-Length response header. BigInteger for headroom
    # against a future large export use case.
    size_bytes = Column(BigInteger, nullable=False)
    # Absolute filesystem path where the bytes live. Base dir is
    # /var/akkountant/user_files/{user_id}/ (0o750 opc:opc, infra
    # creates once); the per-file basename is uuid_<sanitized_name>.
    # Stored verbatim so the download endpoint can open() it directly
    # without recomputing.
    disk_path = Column(String(500), nullable=False)
    created_at = Column(
        DateTime,
        nullable=False,
        server_default=func.now(),
    )

    __table_args__ = (
        # Hot path: list-attachments-for-a-conversation (future FE
        # feature) + cleanup scan by (user_id, created_at) via the
        # existing PK for full-table sweep.
        Index('idx_afa_user_conv', 'user_id', 'conversation_id'),
        Index('idx_afa_user_msg', 'user_id', 'message_id'),
    )

    def to_dict(self):
        """Shape returned by the download-endpoint metadata paths (if
        we ever add one) + used by the MCP tool's return content-block
        after successful insert."""
        return {
            "uuid": self.uuid,
            "user_id": self.user_id,
            "conversation_id": self.conversation_id,
            "message_id": self.message_id,
            "display_name": self.display_name,
            "mime_type": self.mime_type,
            "size_bytes": self.size_bytes,
            "created_at": self.created_at.isoformat() if self.created_at else None,
        }
