"""ak-05af: persistent watchlist CRUD service.

Backs the `/watchlist` + `/watchlist/<id>` + `/watchlist/<id>/note`
+ `/watchlist/ltp` endpoints. Category C #16 of the Kite
capability roadmap — persistent symbol list the Overseer wants
to monitor without holding, enriched with live LTP once the
WebSocket bead (ak-vue6) lands.

## Design notes

* **user_id-scoped on every operation** — single-user app today
  but cheap to preserve the pattern so a future multi-user move
  isn't a security defect. Same precedent as
  ``ReconciliationService.list_trade_issues`` (ak-cipj).
* **404 on wrong-user id** (not 403) — same shape as a nonexistent
  id so an issue id cannot be enumerated across users.
* **Idempotent add** — ``(user_id, instrument_token)`` is unique;
  re-adding an already-watched symbol returns the EXISTING row
  with ``idempotent=True`` rather than raising IntegrityError.
  Matches the ``resolve_trade_issue`` idempotent-repeat shape.
* **Note truncated at 500 chars** at the service boundary — same
  convenience-over-strictness semantic as ``resolution_note``.
* **LTP batch** — ``fetch_ltp`` returns a dict keyed by the
  canonical ``EXCHANGE:TRADINGSYMBOL`` instrument string (the
  shape Kite's ``.ltp()`` method accepts + returns). FE polls
  every 30s until the WebSocket bead lands. Empty watchlist
  returns ``{}`` without hitting Kite.
* **KiteAuthError bubbles** — ``fetch_ltp`` does NOT swallow
  KiteAuthError; the controller catches it and returns the
  ``_kiteReconnectResponse`` shape so the FE can prompt the
  user to re-authenticate.
"""
from datetime import datetime

from sqlalchemy.exc import IntegrityError

from models.watchlistItem import WatchlistItem
from services.Base_Service import BaseService
from utils.logger import Logger


# ak-05af: operator-supplied note is capped at the column width
# (500) at the service boundary. Longer input is truncated
# rather than 400'd — convenience annotation, not a strict
# contract. Mirrors ``_RESOLUTION_NOTE_MAX_LEN`` in the
# reconciliation service.
_NOTE_MAX_LEN = 500

# ak-05af: safety ceiling on LTP batch size. Kite's `.ltp()` accepts
# up to 500 instruments per call per their docs. If the user ever
# ends up with more than 500 watchlist items (unlikely but possible
# over many years), we chunk — this constant says at what boundary.
_LTP_BATCH_LIMIT = 500


class WatchlistService(BaseService):
    _instance = None

    def __new__(cls, kite_service=None):
        if cls._instance is None:
            cls._instance = super(WatchlistService, cls).__new__(cls)
            cls.logger = Logger(__name__).get_logger()
        return cls._instance

    def __init__(self, kite_service=None):
        # BaseService.__init__ is idempotent-ish (re-constructs
        # utility singletons each time) but the module-level
        # _instance singleton means we only want to wire the
        # kite_service dependency ONCE. Set it on first init
        # and preserve across subsequent __init__ calls.
        if not hasattr(self, "_initialized"):
            super().__init__()
            self._kite_service = kite_service
            self._initialized = True
        elif kite_service is not None and self._kite_service is None:
            # Allow late wire-up if the service was constructed
            # before KiteService was ready — shouldn't happen in
            # the canonical app init but keeps tests flexible.
            self._kite_service = kite_service

    # ── Read ────────────────────────────────────────────────────

    def list_items(self, user_id):
        """Return all watchlist items for user, newest first.

        Returns:
            list of dicts shaped by ``_serialize_item``.
        """
        rows = (
            self.db.session.query(WatchlistItem)
            .filter(WatchlistItem.user_id == user_id)
            .order_by(WatchlistItem.added_at.desc())
            .all()
        )
        return [self._serialize_item(r) for r in rows]

    # ── Create ──────────────────────────────────────────────────

    def add_item(
        self, user_id, instrument_token, tradingsymbol,
        exchange, note=None,
    ):
        """Add a symbol to the user's watchlist. Idempotent.

        Args:
            user_id: Firebase user id.
            instrument_token: Kite 64-bit token (int). REQUIRED.
            tradingsymbol: display symbol (REQUIRED non-empty).
            exchange: exchange code (REQUIRED non-empty — NSE /
                BSE / NFO / etc).
            note: optional operator-supplied note; truncated at
                500 chars.

        Returns:
            Tuple ``(result_dict, http_status)``:
              * ``(400, {"error": ...})`` — missing / empty
                required field or non-integer instrument_token.
              * ``(200, {<item>, "idempotent": True})`` — symbol
                already in the watchlist; returns the EXISTING
                row (does NOT overwrite the stored note on
                re-add).
              * ``(201, {<item>, "idempotent": False})`` — fresh
                insert.
              * ``(500, {"error": ...})`` — DB commit failed;
                rolled back.
        """
        # Validate + coerce. instrument_token is the FK shape —
        # accept int + numeric-string (JSON sometimes stringifies
        # 64-bit ints).
        try:
            token_int = int(instrument_token)
        except (TypeError, ValueError):
            return {
                "error": (
                    f"instrument_token must be an integer; got "
                    f"{instrument_token!r}"
                ),
            }, 400
        if token_int <= 0:
            return {
                "error": "instrument_token must be positive",
            }, 400

        if not isinstance(tradingsymbol, str) or not tradingsymbol.strip():
            return {
                "error": "tradingsymbol is required (non-empty string)",
            }, 400
        if not isinstance(exchange, str) or not exchange.strip():
            return {
                "error": "exchange is required (non-empty string)",
            }, 400
        symbol_clean = tradingsymbol.strip()
        exchange_clean = exchange.strip().upper()

        # Length guards match the column widths so DB won't 500
        # on silent truncation.
        if len(symbol_clean) > 80:
            return {
                "error": "tradingsymbol is too long (max 80 chars)",
            }, 400
        if len(exchange_clean) > 10:
            return {
                "error": "exchange is too long (max 10 chars)",
            }, 400

        # Idempotency check — matches the uniqueness constraint
        # ``(user_id, instrument_token)``. Pre-check saves the
        # IntegrityError roundtrip on the common repeat-add.
        existing = (
            self.db.session.query(WatchlistItem)
            .filter(
                WatchlistItem.user_id == user_id,
                WatchlistItem.instrument_token == token_int,
            )
            .first()
        )
        if existing is not None:
            return {
                "item": self._serialize_item(existing),
                "idempotent": True,
            }, 200

        note_clean = None
        if note is not None and isinstance(note, str) and note.strip():
            note_clean = note[:_NOTE_MAX_LEN]

        row = WatchlistItem(
            user_id=user_id,
            instrument_token=token_int,
            tradingsymbol=symbol_clean,
            exchange=exchange_clean,
            note=note_clean,
        )
        self.db.session.add(row)
        try:
            self.db.session.commit()
        except IntegrityError:
            # Race: another request inserted the same (user,
            # instrument) between our pre-check + commit. Roll
            # back + re-read; return the now-present row as the
            # idempotent-success shape.
            self.db.session.rollback()
            existing = (
                self.db.session.query(WatchlistItem)
                .filter(
                    WatchlistItem.user_id == user_id,
                    WatchlistItem.instrument_token == token_int,
                )
                .first()
            )
            if existing is not None:
                return {
                    "item": self._serialize_item(existing),
                    "idempotent": True,
                }, 200
            # Unique constraint fired but the row isn't visible —
            # genuine DB fault.
            self.logger.error(
                f"WatchlistItem add IntegrityError without recoverable "
                f"row user_id={user_id} token={token_int}"
            )
            return {"error": "Add failed"}, 500
        except Exception as exc:
            self.db.session.rollback()
            self.logger.error(
                f"WatchlistItem add failed user_id={user_id} "
                f"token={token_int}: {exc}"
            )
            return {"error": "Add failed"}, 500

        return {
            "item": self._serialize_item(row),
            "idempotent": False,
        }, 201

    # ── Delete ──────────────────────────────────────────────────

    def remove_item(self, user_id, item_id):
        """Remove a watchlist item. user_id-scoped.

        Returns:
            Tuple ``(result_dict, http_status)``:
              * ``(404, {"error": ...})`` — not found OR wrong
                user (same shape in both cases).
              * ``(200, {"id": ..., "removed": True})``.
              * ``(500, {"error": ...})`` — DB commit failed.
        """
        row = (
            self.db.session.query(WatchlistItem)
            .filter(
                WatchlistItem.id == item_id,
                WatchlistItem.user_id == user_id,
            )
            .first()
        )
        if row is None:
            return {"error": "Item not found"}, 404

        try:
            self.db.session.delete(row)
            self.db.session.commit()
        except Exception as exc:
            self.db.session.rollback()
            self.logger.error(
                f"WatchlistItem remove failed user_id={user_id} "
                f"id={item_id}: {exc}"
            )
            return {"error": "Remove failed"}, 500

        return {"id": item_id, "removed": True}, 200

    # ── Update note ─────────────────────────────────────────────

    def update_note(self, user_id, item_id, note):
        """Set / clear the operator note on a watchlist item.

        Passing ``note=None`` or an empty / whitespace-only string
        CLEARS the stored note (sets NULL). Non-empty input is
        truncated silently at 500 chars.

        Returns:
            Tuple ``(result_dict, http_status)``:
              * ``(404, ...)`` — not found OR wrong user.
              * ``(200, {<serialized item>})``.
              * ``(500, ...)`` — DB commit failed.
        """
        row = (
            self.db.session.query(WatchlistItem)
            .filter(
                WatchlistItem.id == item_id,
                WatchlistItem.user_id == user_id,
            )
            .first()
        )
        if row is None:
            return {"error": "Item not found"}, 404

        if note is None or not isinstance(note, str) or not note.strip():
            row.note = None
        else:
            row.note = note[:_NOTE_MAX_LEN]

        try:
            self.db.session.commit()
        except Exception as exc:
            self.db.session.rollback()
            self.logger.error(
                f"WatchlistItem update_note failed user_id={user_id} "
                f"id={item_id}: {exc}"
            )
            return {"error": "Update failed"}, 500

        return self._serialize_item(row), 200

    # ── LTP batch ───────────────────────────────────────────────

    def fetch_ltp(self, user_id):
        """Batch-fetch LTP for every symbol in the user's watchlist.

        Shape returned:
          ``{"ltp": {"NSE:RELIANCE": {"instrument_token": N,
                                       "last_price": 2900.50}, ...},
             "count": N}``

        Empty watchlist returns ``{"ltp": {}, "count": 0}`` WITHOUT
        hitting Kite.

        §23 #2 CONSUME: Kite's native dict shape flows through
        unchanged for the FE.

        KiteAuthError is NOT caught here; it bubbles up so the
        controller can map it to ``_kiteReconnectResponse``. The
        FE needs the reconnect-prompt affordance on the LTP
        polling fallback until the WebSocket bead lands.

        Chunks at 500 instruments per Kite docs (``_LTP_BATCH_LIMIT``).
        """
        if self._kite_service is None:
            # Configuration bug — service wired without its Kite
            # dependency. Log loudly; return empty rather than
            # cascading a NoneType failure into the FE.
            self.logger.error(
                "WatchlistService.fetch_ltp invoked but _kite_service "
                "is None — wire-up gap in app.py"
            )
            return {"ltp": {}, "count": 0}, 500

        rows = (
            self.db.session.query(WatchlistItem)
            .filter(WatchlistItem.user_id == user_id)
            .all()
        )
        if not rows:
            return {"ltp": {}, "count": 0}, 200

        # Build Kite's "EXCHANGE:SYMBOL" keys. Kite accepts either
        # that form or raw instrument_tokens; we use the human-
        # readable form so the response keys are self-describing
        # for FE consumption.
        instruments = [
            f"{r.exchange}:{r.tradingsymbol}" for r in rows
        ]

        merged = {}
        for start in range(0, len(instruments), _LTP_BATCH_LIMIT):
            chunk = instruments[start:start + _LTP_BATCH_LIMIT]
            # KiteAuthError bubbles here — caught at controller.
            resp = self._kite_service.get_ltp(user_id, chunk)
            if isinstance(resp, dict):
                merged.update(resp)

        return {"ltp": merged, "count": len(instruments)}, 200

    # ── Serialization ───────────────────────────────────────────

    @staticmethod
    def _serialize_item(row):
        """Shape a WatchlistItem row for the API response."""
        return {
            "id": row.id,
            "instrument_token": (
                int(row.instrument_token) if row.instrument_token is not None
                else None
            ),
            "tradingsymbol": row.tradingsymbol,
            "exchange": row.exchange,
            "added_at": (
                row.added_at.isoformat() if row.added_at else None
            ),
            "note": row.note,
        }
