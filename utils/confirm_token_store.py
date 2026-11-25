"""ak-erkm: DB-backed single-use state for confirm tokens.

utils/confirm_tokens.py keeps the HMAC / payload checks; this module only
answers two questions across ALL gunicorn workers:

  * mark(nonce, ...)  -> True iff THIS call moved the token from live to
    used (one atomic INSERT on the unique `nonce`); False if someone, in
    any worker, already used or revoked it; None on a DB error
  * is_used(nonce)    -> True / False, or None if the DB could not be read

It runs on its own short transaction from the engine (never the request
session), so marking a token never commits or rolls back the caller's
work. No args or secrets are stored.

Fail-closed: a DB error in mark() returns None (callers treat anything
but True as refused); is_used() returns None and the caller decides.
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


def _utc(ts) -> datetime.datetime:
    """Naive UTC datetime for the (naive) DateTime column. ak-u8c4:
    replaces the deprecated datetime.utcfromtimestamp."""
    return datetime.datetime.fromtimestamp(
        int(ts), tz=datetime.timezone.utc).replace(tzinfo=None)


class DbTokenStore:
    def __init__(self, engine):
        self.engine = engine

    def mark(self, nonce: str, uid: str, tool: str, exp: int, state: str):
        try:
            with self.engine.begin() as conn:
                conn.execute(insert(_TABLE).values(
                    nonce=nonce, uid=uid, tool=tool, state=state,
                    expires_at=_utc(exp)))
        except IntegrityError:
            return False                      # already consumed / revoked
        except Exception as e:
            _LOG.error("[ak-erkm] confirm-token mark failed (%s); refusing", type(e).__name__)
            return None
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


_warned_unresolved = False


def _warn_unresolved_once(reason: str) -> None:
    """ak-u8c4: an app context exists but no DB store resolved, so
    single-use silently falls back to PER-WORKER memory (the pre-ak-erkm
    gap). Say so once per process, loudly."""
    global _warned_unresolved
    if _warned_unresolved:
        return
    _warned_unresolved = True
    _LOG.warning(
        "[ak-u8c4] confirm-token store unresolved inside an app context (%s): "
        "falling back to per-worker single-use. Check flask_sqlalchemy>=3 "
        "and that the SQLAlchemy extension is initialised.", reason)


def current_store():
    """The DB store for the active Flask app, or None outside an app
    context (unit tests / CLI, which use the in-process fallback)."""
    try:
        from flask import current_app, has_app_context
    except Exception:
        return None
    if not has_app_context():
        return None
    try:
        ext = current_app.extensions.get("sqlalchemy")
        if ext is None:
            _warn_unresolved_once("no 'sqlalchemy' extension")
            return None
        engine = getattr(ext, "engine", None)
        if engine is None:
            _warn_unresolved_once(f"{type(ext).__name__} has no .engine")
            return None
        return DbTokenStore(engine)
    except Exception as e:
        _warn_unresolved_once(type(e).__name__)
        return None


def _reset_prune_clock_for_tests():
    global _last_prune
    with _prune_lock:
        _last_prune = 0.0


def _reset_warning_for_tests():
    global _warned_unresolved
    _warned_unresolved = False
