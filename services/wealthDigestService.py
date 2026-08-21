"""ak-5vg: read-side service for the WealthDigest dedicated page (ak-hqm).

Companion to WealthDigestTask (which writes) — this service only READS
the persisted digest + surfaces it to the frontend via three endpoints:

  GET /wealth-digest/latest             — latest generated digest
  GET /wealth-digest?date=YYYY-MM-DD    — digest for a specific past date
  POST /wealth-digest/mark-read         — stamp users.wealth_digest_last_read_at

Data source: the single AgentConversation with
`(agent_type=investment, title="Wealth Digest")` per firebase_id. Daily
runs of WealthDigestTask append one assistant message to this thread; the
"latest" digest is the most recent assistant message. "By date" indexes
into the same thread by AgentMessage.ts::date == requested_date.

Error surface:
  * If env var WEALTH_DIGEST_USER_ID is unset system-wide, the feature
    isn't configured — callers propagate as HTTP 400 with error code
    `WEALTH_DIGEST_USER_ID_UNSET` so the frontend renders the
    "not configured" full-page empty state.
  * Digest absent for the requested user/date → return None; caller
    maps to HTTP 404 with a generic message (no cross-user existence
    leak; matches AgentConversationsController.get shape).
  * ORM error when reading `users.wealth_digest_last_read_at` (pre-
    migration deploy window) → treated as read_at=None; the digest
    text still surfaces so the page is usable while infra runs the
    ALTER TABLE.

Preserves WealthDigestTask behavior: this bead is read-side only.
"""

from __future__ import annotations  # PEP 604 (`dict | None`) on 3.9

import json
import os
from datetime import datetime, date

from sqlalchemy.exc import OperationalError, ProgrammingError

from services.Base_Service import BaseService
from utils.logger import Logger


# ak-5vg v2 (reviewer MINOR-1): pre-migration column-missing errors
# manifest as OperationalError (MySQL/pymysql: "1054 (42S22): Unknown
# column 'wealth_digest_last_read_at' in 'field list'") or
# ProgrammingError (some drivers). String-match against the fragments
# both variants emit — narrower than catching all OperationalError
# / ProgrammingError which would still mask genuine schema-adjacent
# faults (missing table, permission denied, etc.).
_MIGRATION_ERROR_FRAGMENTS = (
    "unknown column",     # MySQL / pymysql
    "no such column",     # SQLite
)


def _is_migration_gap(exc: Exception) -> bool:
    """True iff `exc` matches the specific 'column doesn't exist yet'
    class from a pre-migration deploy window. Falls back to False on
    anything else so genuine DB errors bubble up instead of being
    masked as gracefully-degraded pre-migration state."""
    if not isinstance(exc, (OperationalError, ProgrammingError)):
        return False
    msg = str(exc).lower()
    return any(frag in msg for frag in _MIGRATION_ERROR_FRAGMENTS)

from models.AgentConversation import AgentConversation
from models.AgentMessage import AgentMessage
from models.users import User
from models.Jobs import Job
from services.tasks.wealth_digest_constants import WEALTH_DIGEST_JOB_TITLE
from services.agentConversationService import WEALTH_DIGEST_TITLE


# The env variable that gates WealthDigestTask cycles. Unset = feature
# not configured; endpoints return a distinguishable 400 so the frontend
# knows to render "not configured" rather than "no digest yet."
_ENV_USER_ID = "WEALTH_DIGEST_USER_ID"

# agent_type value the WealthDigest conversation uses. Kept in sync
# with WealthDigestTask._AGENT_TYPE (both "investment") — no cross-
# import to avoid a cycle through the SDK-heavy task module.
_AGENT_TYPE = "investment"

# Distinguishable error code for the "env unset" 400 response so the
# frontend can key off it without string-matching the message.
ENV_UNSET_ERROR_CODE = "WEALTH_DIGEST_USER_ID_UNSET"

# ak-6p4 Wave 3: the four fields the FE renders as distinct components.
# `parse_digest_content` guarantees all four are present in the return
# dict — the endpoint response can wire straight through without
# per-field null-checking.
_STRUCTURED_KEYS = ("text", "actions", "watch_items", "news")


def parse_digest_content(raw: str | None) -> dict:
    """Parse an AgentMessage.content into the structured Wave 3 shape.

    Wave 3 digests are persisted as JSON strings containing
    `{text, actions, watch_items, news}`. Pre-Wave-3 (legacy) digests
    are pure markdown starting with the `[Personal use…]` header.
    Both must render in the new page layout — legacy digests go
    text-only (empty structured arrays), Wave 3 digests populate the
    typed sections.

    Recognition + fallback:
      * Empty / None → `{text:"", actions:[], watch_items:[], news:[]}`
      * Valid JSON with `text` key AND at least one of the array keys
        → treat as structured; missing arrays fill with []; ignore
        extras.
      * Anything else (plain markdown, malformed JSON, JSON that isn't
        the digest shape) → legacy wrap: full raw string goes into
        `text`, arrays empty.

    Returns dict guaranteed to have all four `_STRUCTURED_KEYS` present
    with type-correct values (`text` is str, arrays are lists).
    """
    if not raw:
        return {
            "text": "",
            "actions": [],
            "watch_items": [],
            "news": [],
        }
    stripped = raw.lstrip()
    # Only try JSON parse when the payload looks like a JSON object —
    # avoids false-positive on a legacy digest that happens to contain
    # a JSON-shaped substring somewhere in its markdown body.
    if stripped.startswith("{"):
        try:
            parsed = json.loads(raw)
        except (json.JSONDecodeError, ValueError):
            parsed = None
        if isinstance(parsed, dict) and "text" in parsed and any(
            k in parsed for k in ("actions", "watch_items", "news")
        ):
            # Structured shape recognized. Normalize types + fill
            # missing arrays.
            out = {"text": str(parsed.get("text") or "")}
            for k in ("actions", "watch_items", "news"):
                v = parsed.get(k)
                out[k] = v if isinstance(v, list) else []
            return out
    # Legacy digest — wrap raw as text, arrays empty.
    return {
        "text": raw,
        "actions": [],
        "watch_items": [],
        "news": [],
    }


class WealthDigestNotConfiguredError(Exception):
    """Raised when the WEALTH_DIGEST_USER_ID env var is unset. Caller
    maps to HTTP 400 with `error_code=WEALTH_DIGEST_USER_ID_UNSET`."""


class WealthDigestService(BaseService):
    """Read-side WealthDigest access. Instantiated once in app._setup_instances."""

    def __init__(self):
        super().__init__()
        self.logger = Logger(__name__).get_logger()

    # ── env-configuration check ────────────────────────────────────────

    @staticmethod
    def _assert_configured():
        """Raise WealthDigestNotConfiguredError if WEALTH_DIGEST_USER_ID is unset.

        System-wide config check (not per-user). Called by every public
        method so callers get a consistent early-fail.
        """
        if not os.getenv(_ENV_USER_ID):
            raise WealthDigestNotConfiguredError(
                f"{_ENV_USER_ID} env var unset — WealthDigest feature "
                f"is not configured on this deploy"
            )

    # ── reads ──────────────────────────────────────────────────────────

    def get_latest(self, user_id: str) -> dict | None:
        """Return the latest WealthDigest for user_id or None.

        Shape (Wave 3 structured — matches ak-753 FE component types):
          {
            "date": "YYYY-MM-DD",             # date of the digest message
            "generated_at": "ISO-8601",       # message ts
            "text": "<narrative markdown>",   # opening + patterns + close
            "actions": [                      # typed action items
              {"title", "detail",
               "category" ∈ {cash,equity,debt,gold,other},
               "priority" ∈ {high,medium,low}},
              ...
            ],
            "watch_items": [                  # typed watch items
              {"title", "detail",
               "severity" ∈ {critical,warning,info}},
              ...
            ],
            "news": [                         # typed market-context refs
              {"headline", "detail",
               "related_holdings": [...]},
              ...
            ],
            "last_error": {                   # from Jobs table if latest
              "at": "ISO-8601",               #   WealthDigest job Failed;
              "message": "<Job.result>"       #   null otherwise.
            } | null,
            "read_at": "ISO-8601" | None,     # user's mark-read timestamp
          }
        Backwards compat: legacy (pre-Wave-3) digests were pure
        markdown. `parse_digest_content` wraps those as
        `{text: raw_markdown, actions:[], watch_items:[], news:[]}`
        — FE renders text-only in the new component layout.

        Returns None if no digest has ever been generated for user_id
        (missing conversation OR conversation has zero assistant
        messages) — caller maps to HTTP 404.
        """
        self._assert_configured()
        if not user_id:
            return None

        latest_msg = self._latest_assistant_message(user_id)
        if latest_msg is None:
            return None
        return self._shape_response(
            user_id, latest_msg, source_label="latest",
        )

    def get_by_date(self, user_id: str, requested_date: date) -> dict | None:
        """Return the WealthDigest for user_id on requested_date or None.

        Same shape as get_latest. The digest for a given date is the
        LAST assistant message that day — WealthDigestTask nominally
        appends once per 24h cycle, so there's typically exactly one,
        but if a re-run happened we surface the latest of the day.
        """
        self._assert_configured()
        if not user_id or requested_date is None:
            return None

        msg = self._assistant_message_on_date(user_id, requested_date)
        if msg is None:
            return None
        return self._shape_response(
            user_id, msg, source_label=f"date={requested_date.isoformat()}",
        )

    # ── writes ─────────────────────────────────────────────────────────

    def mark_read(self, user_id: str) -> dict | None:
        """Stamp users.wealth_digest_last_read_at = NOW() for user_id.

        Returns {"read_at": "<ISO-8601>"} echo on success, None if the
        user row doesn't exist (unauthenticated / stale firebase_id →
        caller returns 404 rather than creating the row implicitly).

        Pre-migration deploy window: only the SPECIFIC "column doesn't
        exist yet" error class (`_is_migration_gap`) degrades
        gracefully — we log a WARN and return an optimistic timestamp
        so the FE badge-cleared UX stays consistent (the next
        successful cycle post-migration actually stamps the column).

        ak-5vg v2 (reviewer MINOR-1): NARROW the except so a genuine
        DB failure (connection loss, deadlock, txn abort, permission
        denied) bubbles up to the endpoint handler → HTTP 500 →
        Flask's error path. Prior broad `except Exception` returned
        the optimistic timestamp on ANY error → FE thought mark-read
        succeeded → cross-device sync silently wrong until next fetch
        self-corrected.
        """
        self._assert_configured()
        if not user_id:
            return None

        now = datetime.utcnow()
        session = self.db.session
        try:
            row = session.query(User).filter(User.userID == user_id).first()
            if row is None:
                return None
            row.wealth_digest_last_read_at = now
            session.commit()
            return {"read_at": now.isoformat() + "Z"}
        except Exception as exc:
            try:
                session.rollback()
            except Exception:
                pass
            if _is_migration_gap(exc):
                # Genuine pre-migration state — graceful degrade so
                # FE UX stays consistent. Migration will land soon;
                # next mark-read post-ALTER actually stamps.
                self.logger.warning(
                    f"mark_read: users.wealth_digest_last_read_at write "
                    f"failed for user={user_id[:8]}... "
                    f"(pre-migration column-missing): {exc}. "
                    f"Returning optimistic timestamp; FE UX unaffected."
                )
                return {"read_at": now.isoformat() + "Z"}
            # Genuine DB error — do NOT mask as pre-migration. Let it
            # bubble to the endpoint handler → 500. FE optimistic UI
            # will self-correct on next /latest fetch.
            self.logger.error(
                f"mark_read: unexpected DB error for user={user_id[:8]}...: "
                f"{type(exc).__name__}: {exc}"
            )
            raise

    # ── internals ──────────────────────────────────────────────────────

    def _find_conversation(self, user_id: str):
        """Locate the (single) WealthDigest conversation for user_id.

        WealthDigestTask uses find_or_create_by_title which converges
        on one row per (user_id, agent_type='investment',
        title='Wealth Digest'). Soft-deleted rows are excluded (matches
        agent_conversations service semantics — a user who soft-
        deleted their digest thread has no digest to surface).
        """
        return (
            self.db.session.query(AgentConversation)
            .filter(
                AgentConversation.user_id == user_id,
                AgentConversation.agent_type == _AGENT_TYPE,
                AgentConversation.title == WEALTH_DIGEST_TITLE,
                AgentConversation.deleted_at.is_(None),
            )
            .first()
        )

    def _latest_assistant_message(self, user_id: str):
        """Most-recent assistant AgentMessage in the user's WealthDigest
        conversation, or None if the conversation is absent or empty."""
        conv = self._find_conversation(user_id)
        if conv is None:
            return None
        return (
            self.db.session.query(AgentMessage)
            .filter(
                AgentMessage.conversation_id == conv.id,
                AgentMessage.role == "assistant",
            )
            .order_by(AgentMessage.id.desc())
            .first()
        )

    def _assistant_message_on_date(self, user_id: str, d: date):
        """Latest assistant message whose ts::date == d, or None."""
        conv = self._find_conversation(user_id)
        if conv is None:
            return None
        # Date-boundaries in UTC (matches WealthDigestTask's
        # datetime.utcnow() writes). MySQL / SQLite both handle
        # DATE(ts) == :d as a straight compare via SQLAlchemy `func`.
        from sqlalchemy import func as sa_func
        return (
            self.db.session.query(AgentMessage)
            .filter(
                AgentMessage.conversation_id == conv.id,
                AgentMessage.role == "assistant",
                sa_func.date(AgentMessage.ts) == d,
            )
            .order_by(AgentMessage.id.desc())
            .first()
        )

    def _latest_job_error(self, user_id: str) -> dict | None:
        """Latest WealthDigest Job error surfaced as `{at, message}` or None.

        Shape (ak-5vg v3 — matches FE type `last_error: {at, message} | null`):
          {"at": "<ISO-8601 datetime>", "message": "<Job.result string>"}

        Returns None if:
          * no WealthDigest Job row exists for the user, OR
          * the most recent Job's status != "Failed" (task recovered).

        Jobs table logs one row per scheduled cycle attempt; a Failed
        row's `result` column carries the operator-actionable message
        (e.g. "SDK timeout after 300s"). Surface only the latest so
        the FE banner can pair the error with its timestamp
        ("Latest digest failed at 08:32 UTC: SDK timeout after 300s")
        alongside a possibly-stale but still-valid last-good digest.

        Note on `at`: the Jobs model has no explicit `updated_at` /
        `completed_at` — `due_date` is the closest DateTime column
        (when the cycle was scheduled to run) and is used here as a
        PROXY for when the failure was recorded. For WealthDigest's
        24h interval this proxy is within the cycle window of the
        actual failure. If Phase-2 tightens this signal (add a real
        completed_at to Jobs), swap here.

        ak-5vg v2 Phase-2 note (reviewer MINOR-2): filters
        `Job.user_id == user_id` (the requesting firebase_id). Fine
        in Phase-1 single-user (WEALTH_DIGEST_USER_ID env == caller;
        bootstrap-seeded Job.user_id matches). If Phase-2 ever
        introduces multi-user or a bootstrap-changes Job.user_id to
        SYSTEM_BOOTSTRAP-style fallback (recall ak-ran), this filter
        silently returns None → the "task errored" banner would not
        show even when the task genuinely failed. Revisit when Phase-2
        multi-user or bootstrap-shape change lands."""
        latest = (
            self.db.session.query(Job)
            .filter(
                Job.title == WEALTH_DIGEST_JOB_TITLE,
                Job.user_id == user_id,
            )
            .order_by(Job.id.desc())
            .first()
        )
        if latest is None or latest.status != "Failed":
            return None
        return {
            "at": latest.due_date.isoformat() if latest.due_date else None,
            "message": latest.result or "",
        }

    def _get_user_read_at(self, user_id: str):
        """Read users.wealth_digest_last_read_at.

        Pre-migration deploy window: only the SPECIFIC "column doesn't
        exist yet" error (`_is_migration_gap`) degrades to None so the
        digest text still surfaces. Genuine DB failures are logged at
        ERROR and also return None — the read_at signal is optional
        (badge state), so serving the digest with read_at=null on a
        transient DB glitch beats propagating the error and blanking
        the whole endpoint. Genuine errors are visible in the log.

        ak-5vg v2 (reviewer MINOR-1): narrower except distinguishes
        pre-migration state (WARN) from genuine DB error (ERROR),
        instead of the prior broad-except-swallow-all pattern that
        blurred the two into the same log signal.
        """
        try:
            row = (
                self.db.session.query(User)
                .filter(User.userID == user_id)
                .first()
            )
            if row is None:
                return None
            return getattr(row, "wealth_digest_last_read_at", None)
        except Exception as exc:
            try:
                self.db.session.rollback()
            except Exception:
                pass
            if _is_migration_gap(exc):
                self.logger.warning(
                    f"_get_user_read_at: pre-migration column-missing "
                    f"for user={user_id[:8]}...: {exc}. "
                    f"Returning None (badge state degrades gracefully)."
                )
            else:
                self.logger.error(
                    f"_get_user_read_at: unexpected DB error for "
                    f"user={user_id[:8]}...: {type(exc).__name__}: {exc}. "
                    f"Returning None; digest text still serves."
                )
            return None

    def _shape_response(self, user_id: str, msg: AgentMessage,
                        source_label: str) -> dict:
        """Build the response dict from an AgentMessage row + look up
        the user's read_at + latest job error.

        ak-6p4 Wave 3: `msg.content` now carries a JSON string with the
        structured shape `{text, actions, watch_items, news}`. Legacy
        (pre-Wave-3) digests are pure markdown starting with the
        `[Personal use…]` header. `parse_digest_content` recognizes
        both and normalizes to the same output dict — legacy digests
        fill `text` with the raw markdown and emit empty arrays for
        the typed sections so the FE renders them as text-only in
        the new component-based layout.
        """
        ts = msg.ts
        read_at = self._get_user_read_at(user_id)
        last_error = self._latest_job_error(user_id)
        parsed = parse_digest_content(msg.content)
        return {
            "date": ts.date().isoformat() if ts else None,
            "generated_at": ts.isoformat() + "Z" if ts else None,
            "text": parsed["text"],
            "actions": parsed["actions"],
            "watch_items": parsed["watch_items"],
            "news": parsed["news"],
            "last_error": last_error,
            "read_at": read_at.isoformat() + "Z" if read_at else None,
        }
