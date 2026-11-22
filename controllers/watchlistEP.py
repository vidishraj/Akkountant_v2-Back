"""ak-05af: Watchlist controller.

Surfaces the ``WatchlistService`` CRUD + LTP batch to the FE.
Category C #16 of the Kite capability roadmap — persistent
symbol list the Overseer wants to monitor without holding,
polling ``/watchlist/ltp`` every 30s until the WebSocket bead
(ak-vue6) lands.

Routes (wired in app.py):
  * ``GET  /watchlist``                — list items (newest first)
  * ``POST /watchlist``                — add symbol (idempotent)
  * ``DELETE /watchlist/<item_id>``    — remove symbol
  * ``PUT  /watchlist/<item_id>/note`` — edit / clear note
  * ``GET  /watchlist/ltp``            — batch LTP for watched symbols

Auth: the existing ``before_request`` middleware in app.py
(ak-c8l) rejects any request without a valid firebase token
with a 401. ``g.firebase_id`` is therefore guaranteed set
inside these handlers.

**This is the first Kite-roadmap bead with INTENTIONAL
mutations** (POST / DELETE / PUT) — single-user, non-destructive
own-data shape per bead's §23 analysis. The READ-ONLY drift
guard pattern in the test suite is adapted here to assert the
EXPECTED route verbs rather than reject all mutations (see
``test_watchlist.py`` for the pinned shape).
"""
from flask import g, jsonify, request

from services.KiteService import KiteAuthError
from services.watchlistService import WatchlistService
from utils.logger import Logger


class WatchlistController:
    watchlist_service: WatchlistService

    def __init__(self, watchlist_service, investment_service=None):
        self.watchlist_service = watchlist_service
        # Used ONLY to build the reconnect payload (same login URL
        # the rest of the Kite controllers emit on auth failure).
        # None-tolerant; the fallback just omits the login_url
        # field rather than crashing the auth-error path.
        self._investment_service = investment_service
        self.logger = Logger(__name__).get_logger()

    def _kiteReconnectResponse(self, exc):
        """401 payload telling the UI to walk the Kite login flow
        again. Mirrors ``InvestmentController._kiteReconnectResponse``
        so the FE sees the same reconnect-prompt shape across every
        Kite surface. See that method's docstring for the
        6-AM-IST-expiry rationale.
        """
        self.logger.warning(f"Kite reconnect required: {str(exc)}")
        payload = {
            "error": str(exc),
            "reconnect_required": True,
            "kite_connected": False,
        }
        if self._investment_service is not None:
            try:
                payload["login_url"] = (
                    self._investment_service.getKiteLoginUrl()
                )
            except Exception as urlErr:
                self.logger.error(
                    f"Could not build Kite login URL: {urlErr}"
                )
                payload["login_url"] = None
        else:
            payload["login_url"] = None
        return jsonify(payload), 401

    @Logger.standardLogger
    def listItems(self):
        """GET /watchlist — list watchlist items (newest first)."""
        user_id = g.get("firebase_id")
        if not user_id:
            return jsonify({"error": "User ID not found"}), 400

        items = self.watchlist_service.list_items(user_id)
        return jsonify({"items": items, "count": len(items)}), 200

    @Logger.standardLogger
    def addItem(self):
        """POST /watchlist — add a symbol to the watchlist.

        Body (JSON, REQUIRED):
          * ``instrument_token`` (int / numeric-string) — Kite 64-bit token
          * ``tradingsymbol`` (str, non-empty)
          * ``exchange`` (str, non-empty — NSE / BSE / NFO / ...)
          * ``note`` (str, optional) — truncated to 500 chars

        Responses:
          * 200 + ``idempotent=True``  — symbol already watched;
            returns the existing row (does NOT overwrite the
            stored note).
          * 201 + ``idempotent=False`` — fresh insert.
          * 400 — missing / invalid field.
          * 500 — DB commit failed.
        """
        user_id = g.get("firebase_id")
        if not user_id:
            return jsonify({"error": "User ID not found"}), 400

        if not request.is_json:
            return jsonify({
                "error": "Request body must be application/json",
            }), 400
        data = request.get_json(silent=True) or {}
        if not isinstance(data, dict):
            return jsonify({
                "error": "Request body must be a JSON object",
            }), 400

        if "instrument_token" not in data:
            return jsonify({
                "error": "instrument_token is required",
            }), 400
        if "tradingsymbol" not in data or not data.get("tradingsymbol"):
            return jsonify({
                "error": "tradingsymbol is required (non-empty)",
            }), 400
        if "exchange" not in data or not data.get("exchange"):
            return jsonify({
                "error": "exchange is required (non-empty)",
            }), 400

        result, status_code = self.watchlist_service.add_item(
            user_id,
            instrument_token=data.get("instrument_token"),
            tradingsymbol=data.get("tradingsymbol"),
            exchange=data.get("exchange"),
            note=data.get("note"),
        )
        return jsonify(result), status_code

    @Logger.standardLogger
    def removeItem(self, item_id):
        """DELETE /watchlist/<item_id> — remove a symbol.

        single-delete is low-stakes per bead §23 #5 (no
        destructive-gate confirmation required); a future
        bulk-delete bead would add the gate.
        """
        user_id = g.get("firebase_id")
        if not user_id:
            return jsonify({"error": "User ID not found"}), 400

        try:
            item_id_int = int(item_id)
        except (TypeError, ValueError):
            return jsonify({
                "error": f"item_id must be an integer; got {item_id!r}",
            }), 400

        result, status_code = self.watchlist_service.remove_item(
            user_id, item_id_int,
        )
        return jsonify(result), status_code

    @Logger.standardLogger
    def updateNote(self, item_id):
        """PUT /watchlist/<item_id>/note — set / clear the note.

        Body (JSON, OPTIONAL): ``{"note": "..."}``.
        Missing body / empty-string note CLEARS the stored note
        (sets NULL). Non-empty input is truncated at 500 chars.
        """
        user_id = g.get("firebase_id")
        if not user_id:
            return jsonify({"error": "User ID not found"}), 400

        try:
            item_id_int = int(item_id)
        except (TypeError, ValueError):
            return jsonify({
                "error": f"item_id must be an integer; got {item_id!r}",
            }), 400

        note = None
        if request.content_length and request.is_json:
            data = request.get_json(silent=True) or {}
            if isinstance(data, dict):
                raw = data.get("note")
                # None / empty / whitespace → clear. Pass through
                # whatever shape the body carries; service does
                # the final cleanup + truncation.
                if raw is not None and isinstance(raw, str):
                    note = raw

        result, status_code = self.watchlist_service.update_note(
            user_id, item_id_int, note,
        )
        return jsonify(result), status_code

    @Logger.standardLogger
    def fetchLtp(self):
        """GET /watchlist/ltp — batch LTP for every watched symbol.

        Returns ``{"ltp": {<EX:SYM>: {instrument_token, last_price},
        ...}, "count": N}``. Empty watchlist returns ``{"ltp":
        {}, "count": 0}`` without touching Kite.

        KiteAuthError → 401 reconnect-required payload (FE walks
        the Kite login flow again).
        """
        user_id = g.get("firebase_id")
        if not user_id:
            return jsonify({"error": "User ID not found"}), 400

        try:
            result, status_code = self.watchlist_service.fetch_ltp(
                user_id,
            )
            return jsonify(result), status_code
        except KiteAuthError as e:
            return self._kiteReconnectResponse(e)
        except Exception as exc:
            self.logger.error(
                f"Watchlist LTP fetch failed user_id={user_id}: {exc}"
            )
            return jsonify({"error": "Failed to fetch LTP"}), 500
