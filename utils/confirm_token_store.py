"""ak-erkm: DB-backed single-use state for confirm tokens.

utils/confirm_tokens.py keeps the HMAC / payload checks; this module only
answers two questions across ALL gunicorn workers:

  * mark(nonce, ...)  -> True iff THIS call moved the token from live to
    used (one atomic INSERT on the unique `nonce`; a duplicate key means
    someone else, in any worker, already used or revoked it)
  * is_used(nonce)    -> True / False, or None if the DB could not be read

It runs on its own short transaction from the engine (never the request
session), so marking a token never commits or rolls back the caller's
work. No args or secrets are stored.

Fail-closed: a DB error in mark() returns False (the call is refused and
the user re-confirms); is_used() returns None and the caller decides.
"""
from __future__ import annotations

import datetime
import logging
import threading
import time

from sqlalchemy import delete, insert, select
from sqlalchemy.exc import IntegrityError

from models.confirmTokenState import ConfirmTokenState

_LOG = logging.getLogger(__name__)

_PRUNE_EVERY_S = 60
_prune_lock = threading.Lock()
_last_prune = 0.0

_TABLE = ConfirmTokenState.__table__


def _utc(ts: int) -> datetime.datetime:
    return datetime.datetime.utcfromtimestamp(int(ts))


class DbTokenStore:
    def __init__(self, engine):
        self.engine = engine

    def mark(self, nonce: str, uid: str, tool: str, exp: int, state: str) -> bool:
        try:
            with self.engine.begin() as conn:
                conn.execute(insert(_TABLE).values(
                    nonce=nonce, uid=uid, tool=tool, state=state,
                    expires_at=_utc(exp)))
        except IntegrityError:
            return False                      # already consumed / revoked
        except Exception as e:
            _LOG.error("[ak-erkm] confirm-token mark failed (%s); refusing", type(e).__name__)
            return False
        self._maybe_prune()
        return True

    def is_used(self, nonce: str):
        try:
            with self.engine.connect() as conn:
                row = conn.execute(
                    select(_TABLE.c.nonce).where(_TABLE.c.nonce == nonce)).first()
            return row is not None
        except Exception as e:
            _LOG.error("[ak-erkm] confirm-token lookup failed (%s)", type(e).__name__)
            return None

    def prune(self, now: float | None = None) -> int:
        cutoff = _utc(now if now is not None else time.time())
        with self.engine.begin() as conn:
            res = conn.execute(delete(_TABLE).where(_TABLE.c.expires_at < cutoff))
        return res.rowcount or 0

    def _maybe_prune(self):
        global _last_prune
        now = time.time()
        with _prune_lock:
            if now - _last_prune < _PRUNE_EVERY_S:
                return
            _last_prune = now
        try:
            self.prune(now)
        except Exception as e:
            _LOG.warning("[ak-erkm] confirm-token prune skipped (%s)", type(e).__name__)


def current_store():
    """The DB store for the active Flask app, or None outside an app
    context (unit tests / CLI, which use the in-process fallback)."""
    try:
        from flask import current_app, has_app_context
        if not has_app_context():
            return None
        ext = current_app.extensions.get("sqlalchemy")
        if ext is None:
            return None
        return DbTokenStore(ext.engine)
    except Exception:
        return None


def _reset_prune_clock_for_tests():
    global _last_prune
    with _prune_lock:
        _last_prune = 0.0
