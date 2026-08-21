"""ak-5vg + ak-6p4: REST endpoints for the WealthDigest dedicated page.

Routes registered in app.py:
  GET  /wealth-digest/latest             → latest digest (or 404)
  GET  /wealth-digest?date=YYYY-MM-DD    → digest on that date (or 404)
  POST /wealth-digest/mark-read          → stamp read_at, echo

Response shape (Wave 3):
  {
    "date", "generated_at",              # from the digest message
    "text",                              # narrative markdown body
    "actions", "watch_items", "news",    # typed component arrays
    "last_error", "read_at",             # ak-5vg fields
  }

Backwards compat: pre-Wave-3 digests were pure markdown; endpoint
wraps those as `text`+empty-arrays so the FE renders text-only in the
new component-based layout.

Auth: standard g.firebase_id from the before_request middleware. Every
endpoint is user-scoped — the digest for a given firebase_id is derived
from that user's AgentConversation with title="Wealth Digest". No
cross-user leak (soft-deleted / other-user conversations look identical
to a true miss → generic 404).

If WEALTH_DIGEST_USER_ID env var is unset, the whole feature isn't
configured on this deploy → all three endpoints return HTTP 400 with
error_code=WEALTH_DIGEST_USER_ID_UNSET so the FE renders the
"not configured" full-page empty state.

## Deploy prerequisite

`ALTER TABLE users ADD COLUMN wealth_digest_last_read_at TIMESTAMP
NULL DEFAULT NULL;` is a HARD prereq for the endpoint's mark_read +
read_at behavior — see models/users.py for the full asymmetry
breakdown. Recommended sequencing: run the ALTER before mayor-merge,
or atomically with the GHA-triggered app.service restart. Not "run
whenever."

Pre-migration behavior (asymmetric — post ak-5vg v2 fix):
  * READ side: digest text still surfaces; read_at falls to null
    (badge state degrades gracefully). WARN log per-request.
  * WRITE side (WealthDigestTask cycle): no longer halts — the
    write-path User query was narrowed to `query(User.userID)` so
    adding the new column doesn't couple the cycle to the migration.
    New digests continue to generate normally.
  * mark_read on the endpoint: returns optimistic timestamp on the
    specific column-missing error (FE badge-cleared UX preserved).
    Genuine DB failures now bubble to HTTP 500 (v2 narrowed the
    except — prior broad except would have masked genuine failures
    as fake-success).
"""

from __future__ import annotations  # PEP 604 (`dict | None`) on 3.9

from datetime import date, datetime
from flask import g, jsonify, request

from services.wealthDigestService import (
    ENV_UNSET_ERROR_CODE,
    WealthDigestNotConfiguredError,
    WealthDigestService,
)
from utils.logger import Logger


class WealthDigestController:
    def __init__(self, service: WealthDigestService):
        self.service = service
        self.logger = Logger(__name__).get_logger()

    # ── helpers ────────────────────────────────────────────────────────

    @staticmethod
    def _auth_or_401():
        """Extract g.firebase_id or return a 401 tuple. Returns
        (user_id, None) on success or (None, response) on missing auth."""
        user_id = g.get("firebase_id")
        if not user_id:
            return None, (jsonify({"error": "Unauthorized"}), 401)
        return user_id, None

    @staticmethod
    def _not_configured_response():
        """Distinguishable 400 for the WEALTH_DIGEST_USER_ID_UNSET case."""
        return jsonify({
            "error": "WealthDigest is not configured on this deploy",
            "error_code": ENV_UNSET_ERROR_CODE,
        }), 400

    # ── endpoints ──────────────────────────────────────────────────────

    @Logger.standardLogger
    def latest(self):
        """GET /wealth-digest/latest — latest digest for the authenticated user."""
        user_id, err = self._auth_or_401()
        if err is not None:
            return err
        try:
            payload = self.service.get_latest(user_id)
        except WealthDigestNotConfiguredError:
            return self._not_configured_response()
        except Exception:
            self.logger.exception(
                "wealth_digest: latest failed for user=%s...", user_id[:8]
            )
            return jsonify({"error": "Internal server error"}), 500
        if payload is None:
            return jsonify({"error": "No wealth digest available"}), 404
        return jsonify(payload), 200

    @Logger.standardLogger
    def by_date(self):
        """GET /wealth-digest?date=YYYY-MM-DD — digest on a specific date."""
        user_id, err = self._auth_or_401()
        if err is not None:
            return err
        raw = request.args.get("date")
        if not raw:
            return jsonify({
                "error": "Missing required query parameter 'date'"
            }), 400
        try:
            requested_date = datetime.strptime(raw, "%Y-%m-%d").date()
        except ValueError:
            return jsonify({
                "error": f"Invalid date {raw!r}; expected YYYY-MM-DD"
            }), 400
        try:
            payload = self.service.get_by_date(user_id, requested_date)
        except WealthDigestNotConfiguredError:
            return self._not_configured_response()
        except Exception:
            self.logger.exception(
                "wealth_digest: by_date failed for user=%s... date=%s",
                user_id[:8], requested_date,
            )
            return jsonify({"error": "Internal server error"}), 500
        if payload is None:
            return jsonify({
                "error": f"No wealth digest available for {requested_date.isoformat()}"
            }), 404
        return jsonify(payload), 200

    @Logger.standardLogger
    def mark_read(self):
        """POST /wealth-digest/mark-read — stamp users.wealth_digest_last_read_at."""
        user_id, err = self._auth_or_401()
        if err is not None:
            return err
        try:
            payload = self.service.mark_read(user_id)
        except WealthDigestNotConfiguredError:
            return self._not_configured_response()
        except Exception:
            self.logger.exception(
                "wealth_digest: mark_read failed for user=%s...", user_id[:8]
            )
            return jsonify({"error": "Internal server error"}), 500
        if payload is None:
            # User row missing — treat as 404 rather than implicitly
            # creating the row.
            return jsonify({"error": "User not found"}), 404
        return jsonify(payload), 200
