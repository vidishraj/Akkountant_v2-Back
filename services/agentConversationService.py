"""ak-bq5: cross-device agent chat persistence — service layer.

Encapsulates all reads / writes to agent_conversations + agent_messages
so the controllers stay shaped like the existing REST endpoints (thin
JSON shim) and stream_chat in agentService stays focused on the SDK
loop.

Scope guards (per dispatch hq-wisp-rr15b):
  - Every read AND write filters by user_id == g.firebase_id. The
    controller passes user_id through; service refuses to operate on
    a conversation it can't prove belongs to the caller. Returns None
    on mismatch so the controller maps to 404 (no leak via
    "permission denied" vs "not found").
  - Soft delete: deleted_at IS NULL filter on every list / get query.
  - Title derived at create time from first 60 chars of the first
    user message (newlines stripped, whitespace collapsed) — no LLM
    call.
"""

import re
from datetime import datetime

from sqlalchemy import func

from models import AgentConversation, AgentMessage, AGENT_MESSAGE_ROLES
from services.Base_Service import BaseService
from utils.logger import Logger


# Title length cap is the column width minus headroom for later edit;
# matches the dispatch's "60 chars of first user msg" decision.
TITLE_MAX_LEN = 60
DEFAULT_TITLE = "New conversation"

# Matches what controllers/agentEP.py accepts.
VALID_AGENT_TYPES = ('investment', 'transaction', 'freelance')

# ak-ran Phase 1: canonical title for the daily wealth-management
# digest conversation. Shared with WealthDigestTask via import so
# find_or_create_by_title uses the same exact string on both write
# (digest task) and any future read (FE / other jobs). Grep for
# WEALTH_DIGEST_TITLE to find every site that touches this thread.
# NOTE: title lives inside agent_type='investment' — per Lead's Q1
# GO, we reuse the existing agent type rather than mint a new one
# ('wealth_digest' would need VALID_AGENT_TYPES + agentEP whitelist
# + FE-side awareness). If Phase 2 promotes to a dedicated agent
# type, this constant moves with it.
WEALTH_DIGEST_TITLE = "Wealth Digest"

# Whitespace-collapse regex used by derive_title.
_WS_RE = re.compile(r"\s+")


def derive_title(first_user_message_content):
    """Build a conversation title from the first user message.

    Strips newlines + collapses runs of whitespace to a single space,
    then truncates to TITLE_MAX_LEN. Falls back to DEFAULT_TITLE for
    empty / whitespace-only input so the column never carries an empty
    string (the FE renders title prominently in the conversation list).
    """
    if not first_user_message_content or not isinstance(first_user_message_content, str):
        return DEFAULT_TITLE
    cleaned = _WS_RE.sub(" ", first_user_message_content).strip()
    if not cleaned:
        return DEFAULT_TITLE
    if len(cleaned) <= TITLE_MAX_LEN:
        return cleaned
    # Truncate at a word boundary if there's a space within the last
    # ~10 chars — produces nicer titles. Falls back to a hard cut.
    cut = cleaned[:TITLE_MAX_LEN]
    last_space = cut.rfind(" ")
    if last_space >= TITLE_MAX_LEN - 10:
        cut = cut[:last_space]
    return cut + "…"


class AgentConversationService(BaseService):
    """Singleton service following the same pattern as AgentService.

    Methods take a user_id explicitly rather than reading from g so the
    same code is callable from the SSE generator inside
    agentService.stream_chat (which already has user_id in scope).
    """

    _instance = None

    def __new__(cls):
        if cls._instance is None:
            cls._instance = super().__new__(cls)
            cls.logger = Logger(__name__).get_logger()
        return cls._instance

    # ── reads ──────────────────────────────────────────────────────────

    def list_conversations(self, user_id, agent_type=None):
        """Return non-soft-deleted conversations for the user, optionally
        filtered by agent_type. Ordered by updated_at DESC so most-recent
        threads surface first."""
        if not user_id:
            return []
        q = (
            self.db.session.query(
                AgentConversation,
                func.count(AgentMessage.id).label("msg_count"),
            )
            .outerjoin(AgentMessage, AgentMessage.conversation_id == AgentConversation.id)
            .filter(AgentConversation.user_id == user_id)
            .filter(AgentConversation.deleted_at.is_(None))
        )
        if agent_type:
            if agent_type not in VALID_AGENT_TYPES:
                return []
            q = q.filter(AgentConversation.agent_type == agent_type)
        q = q.group_by(AgentConversation.id).order_by(
            AgentConversation.updated_at.desc()
        )
        rows = q.all()
        return [
            row[0].to_summary_dict(message_count=int(row[1] or 0))
            for row in rows
        ]

    def get_conversation(self, user_id, conversation_id):
        """Fetch one conversation + its messages, owned by user_id.
        Returns None if not found, soft-deleted, or owned by a
        different user — controller maps None to 404."""
        if not user_id or not conversation_id:
            return None
        conv = self._fetch_owned(user_id, conversation_id)
        if conv is None:
            return None
        # AgentConversation.messages relationship is order_by id already,
        # but reach via the session-scoped query so SQLAlchemy doesn't
        # cache stale state across requests.
        messages = (
            self.db.session.query(AgentMessage)
            .filter(AgentMessage.conversation_id == conv.id)
            .order_by(AgentMessage.id.asc())
            .all()
        )
        out = conv.to_summary_dict(message_count=len(messages))
        out["messages"] = [m.to_dict() for m in messages]
        return out

    # ── writes ─────────────────────────────────────────────────────────

    def create_conversation(self, user_id, agent_type, title=None,
                            first_user_message=None):
        """Insert a new conversation row. If title is None, derive from
        first_user_message; if both are None, falls back to
        DEFAULT_TITLE.

        Returns the integer id of the new row (NOT the model object —
        the caller often only needs the id for the SSE preamble).

        ak-6si bug fix: BaseService.db is a property that returns a
        NEW scoped_session on every access when g.db is None (the
        background-task path — WealthDigestTask runs inside an
        app_context but has no request_context, so g.db is None).
        Pre-fix, `self.db.session.add(conv)` and `self.db.session.
        commit()` each re-invoked the property → landed on DIFFERENT
        sessions → the row was added to session A but commit ran
        empty on session B → conv.id stayed None because no flush
        ever happened on the session holding conv. Fix:
          (1) cache the session in a local so add/flush/commit all
              hit the SAME session,
          (2) call session.flush() explicitly so conv.id is populated
              from the DB's RETURNING/lastrowid BEFORE we commit —
              defense against SQLAlchemy's default expire_on_commit
              expiring the object and requiring a lazy re-load,
          (3) snapshot conv.id BEFORE commit for the same reason,
          (4) WARN + raise if the snapshot is None so future silent
              failures surface (pre-fix returned None silently → the
              caller trip append_message ownership check with no
              upstream signal).
        """
        if not user_id:
            raise ValueError("create_conversation: user_id required")
        if agent_type not in VALID_AGENT_TYPES:
            raise ValueError(
                f"create_conversation: invalid agent_type {agent_type!r}; "
                f"expected one of {VALID_AGENT_TYPES}"
            )
        if title is None:
            title = derive_title(first_user_message)
        # ak-6si (1): cache the session so add/flush/commit are on
        # the SAME session even when self.db returns a fresh scoped
        # session each call (background-task path).
        session = self.db.session
        conv = AgentConversation(
            user_id=user_id,
            agent_type=agent_type,
            title=title,
        )
        try:
            session.add(conv)
            # ak-6si (2): flush explicitly to populate conv.id from
            # the DB's autoincrement RETURNING before commit expires
            # the object.
            session.flush()
            # ak-6si (3): snapshot the id BEFORE commit. Defense
            # against expire_on_commit lazy-load returning None if
            # the session state gets muddled.
            conv_id = conv.id
            session.commit()
        except Exception:
            session.rollback()
            raise
        # ak-6si (4): defensive — surface silent None returns instead
        # of propagating them to callers who trip ownership checks
        # downstream with no upstream signal.
        if conv_id is None:
            msg = (
                f"create_conversation: conv.id is None after flush+commit "
                f"(user_id={user_id[:8]}... agent_type={agent_type!r} "
                f"title={title!r}). Likely session-caching / autoincrement "
                f"issue. Refusing to return None."
            )
            self.logger.error(msg)
            raise RuntimeError(msg)
        return conv_id

    def find_or_create_by_title(self, user_id, agent_type, title):
        """ak-ran Phase 1: idempotent lookup for a conversation
        identified by (user_id, agent_type, title). Returns the
        conversation_id; creates a new row if none exists.

        Purpose: WealthDigestTask fires daily and needs to append to
        the SAME conversation each run (not create N conversations
        over time). This is the primitive that guarantees convergence
        on a single thread per user.

        Matching rules:
          - user_id + agent_type + title all match exactly.
          - deleted_at IS NULL — a soft-deleted conversation is
            treated as "not found" so a fresh one is created (the
            user explicitly deleted the old thread; don't resurrect).
          - If multiple non-deleted matches exist (should be rare —
            only happens if a caller creates the conv directly
            without using this helper), return the OLDEST (min id)
            so all future runs converge on the first-created one.

        Args mirror create_conversation for symmetry."""
        if not user_id:
            raise ValueError("find_or_create_by_title: user_id required")
        if agent_type not in VALID_AGENT_TYPES:
            raise ValueError(
                f"find_or_create_by_title: invalid agent_type "
                f"{agent_type!r}; expected one of {VALID_AGENT_TYPES}"
            )
        if not title:
            raise ValueError("find_or_create_by_title: title required")
        # ak-6si: cache the session so the SELECT + fall-through to
        # create_conversation are on the SAME scoped_session (in the
        # background-task path BaseService.db returns a fresh session
        # per attribute access; without caching, the SELECT could see
        # different data than what create_conversation subsequently
        # writes).
        session = self.db.session
        existing = (
            session.query(AgentConversation)
            .filter(AgentConversation.user_id == user_id)
            .filter(AgentConversation.agent_type == agent_type)
            .filter(AgentConversation.title == title)
            .filter(AgentConversation.deleted_at.is_(None))
            .order_by(AgentConversation.id.asc())
            .first()
        )
        if existing is not None:
            if existing.id is None:
                # Defensive: an existing row without an id would be a
                # pathological ORM state, but call it out so a future
                # silent None return has a log trail.
                self.logger.error(
                    f"find_or_create_by_title: existing row has id=None "
                    f"(user_id={user_id[:8]}... agent_type={agent_type!r} "
                    f"title={title!r})"
                )
                raise RuntimeError(
                    "find_or_create_by_title: existing row has id=None"
                )
            return existing.id
        # Create fresh with the explicit title (bypass derive_title —
        # we already know the exact string). create_conversation now
        # raises RuntimeError on None-id return (ak-6si), so this
        # propagates loudly rather than passing None to the caller.
        conv_id = self.create_conversation(
            user_id=user_id,
            agent_type=agent_type,
            title=title,
        )
        # ak-6si: belt-and-braces — even though create_conversation
        # now raises on None, guard here too so the invariant is
        # explicit at both callers.
        if conv_id is None:
            msg = (
                f"find_or_create_by_title: create_conversation returned "
                f"None (user_id={user_id[:8]}... agent_type={agent_type!r} "
                f"title={title!r})"
            )
            self.logger.error(msg)
            raise RuntimeError(msg)
        return conv_id

    def append_message(self, user_id, conversation_id, role, content,
                       attachments_meta=None, partial=False):
        """Insert one message row. Bumps the conversation's updated_at
        so list ordering reflects activity.

        Validates the role against AGENT_MESSAGE_ROLES and confirms the
        conversation belongs to user_id before writing. Returns the
        new message id, or None if the conversation doesn't belong to
        the user / doesn't exist. Raises RuntimeError if the DB write
        succeeded but msg.id ended up None (ak-6si-style defensive
        guard so silent None returns can't propagate to callers).

        ak-m6k: session cached in a local at method entry + threaded
        into _fetch_owned so the ownership SELECT and the subsequent
        INSERT + updated_at UPDATE all hit the SAME scoped_session
        (and therefore the SAME engine + connection). Pre-fix,
        _fetch_owned's `self.db.session.query(...)` and this method's
        `self.db.session.add(msg)` each materialized a fresh engine
        (background-task path where g.db is None), so the ownership
        check couldn't see a conv that had JUST been committed by
        find_or_create_by_title via a DIFFERENT fresh engine.
        """
        if role not in AGENT_MESSAGE_ROLES:
            raise ValueError(
                f"append_message: invalid role {role!r}; "
                f"expected one of {AGENT_MESSAGE_ROLES}"
            )
        if content is None:
            content = ""
        # ak-m6k: cache session in a local + thread into _fetch_owned
        # so the ownership check + write path share the same session/
        # engine. Prevents the read-side manifestation of the
        # ak-6si-class bug (INSTANCE 2 per ak-m6k bead body).
        session = self.db.session
        conv = self._fetch_owned(user_id, conversation_id, session=session)
        if conv is None:
            return None
        msg = AgentMessage(
            conversation_id=conv.id,
            role=role,
            content=str(content),
            attachments_meta=attachments_meta,
            partial=bool(partial),
        )
        try:
            session.add(msg)
            # Explicit updated_at bump so the ORDER BY in list_conversations
            # reflects the new activity even if the DB's onupdate trigger
            # doesn't fire on this dialect.
            conv.updated_at = datetime.utcnow()
            # ak-m6k / ak-6si: flush before commit populates msg.id
            # from RETURNING before commit's expire_on_commit fires
            # + snapshot BEFORE commit so lazy-load can't return None.
            session.flush()
            msg_id = msg.id
            session.commit()
        except Exception:
            session.rollback()
            raise
        # ak-6si-style defensive: raise rather than return None so
        # future silent-None returns surface immediately at the caller.
        if msg_id is None:
            fail_msg = (
                f"append_message: msg.id is None after flush+commit "
                f"(user_id={user_id[:8]}... conv_id={conversation_id!r} "
                f"role={role!r}). Likely session-caching / autoincrement "
                f"issue. Refusing to return None."
            )
            self.logger.error(fail_msg)
            raise RuntimeError(fail_msg)
        return msg_id

    def soft_delete(self, user_id, conversation_id):
        """Set deleted_at on a user's conversation. Returns True on
        success, False if not owned / not found / already deleted.

        ak-m6k: session cached + threaded (same class of read/write
        cross-session issue as append_message)."""
        session = self.db.session
        conv = self._fetch_owned(user_id, conversation_id, session=session)
        if conv is None:
            return False
        try:
            conv.deleted_at = datetime.utcnow()
            session.commit()
        except Exception:
            session.rollback()
            raise
        return True

    # ── helpers ────────────────────────────────────────────────────────

    def _fetch_owned(self, user_id, conversation_id, *, session=None):
        """Return a live (non-soft-deleted) AgentConversation owned by
        user_id, else None. Single source of truth for the membership
        check — every read/write helper goes through here so a future
        permissions change has exactly one site to update.

        ak-m6k: accepts an optional `session=` kwarg so callers can
        thread their cached session in and guarantee read+write happen
        on the SAME engine. When session is None (backward-compat
        callers that don't cache), falls back to self.db.session
        (fresh scoped_session per call in the background-task path).
        Callers doing a subsequent write MUST pass session= to avoid
        the ak-m6k read-side snapshot issue.
        """
        if not user_id or not conversation_id:
            return None
        try:
            cid = int(conversation_id)
        except (TypeError, ValueError):
            return None
        # ak-m6k: use caller-provided session if any, else fall back
        # to self.db.session (single-shot readers that don't need
        # cross-method consistency).
        query_session = session if session is not None else self.db.session
        return (
            query_session.query(AgentConversation)
            .filter(AgentConversation.id == cid)
            .filter(AgentConversation.user_id == user_id)
            .filter(AgentConversation.deleted_at.is_(None))
            .first()
        )
