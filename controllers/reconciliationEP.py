"""ak-cipj: Trade-reconciliation read + resolve controller.

Surfaces the ``ReconciliationIssue`` pipeline written by
``ReconcileTradesTask`` (ak-zai2) to the Overseer. Pre-ak-cipj
the pipeline was write-only — these two endpoints close the loop
so the Home-card badge can show real numbers and the review
surface can resolve individual issues.

Routes (wired in app.py):
  * ``GET  /reconciliation/issues?status=open|resolved|all``
  * ``POST /reconciliation/issues/<issue_id>/resolve``

Auth: the existing ``before_request`` middleware in app.py rejects
any request without ``X-Firebase-ID`` with a 401. ``g.firebase_id``
is therefore guaranteed set inside these handlers.

Scoping: every query goes through ``ReconciliationService`` with
the firebase_id; the service enforces user_id scoping on both read
and mutate. A 404 is returned on not-found-OR-user-mismatch — same
shape in both cases, so an issue id cannot be enumerated across
users (single-user app today, pattern preserved for future).
"""
from flask import g, jsonify, request

from services.reconciliationService import ReconciliationService
from utils.logger import Logger


# Valid values for the ``status`` filter. Mirrors the service's
# ``_LIST_STATUS_VALID`` constant — duplicated at the controller so
# we 400 bad input at the HTTP boundary (rather than silently
# falling back to 'open' inside the service and pretending a bad
# request succeeded).
_STATUS_ALLOWED = frozenset({"open", "resolved", "all"})


class ReconciliationController:
    reconciliation_service: ReconciliationService

    def __init__(self, reconciliation_service):
        self.reconciliation_service = reconciliation_service
        self.logger = Logger(__name__).get_logger()

    @Logger.standardLogger
    def listIssues(self):
        """GET /reconciliation/issues?status=<open|resolved|all>.

        Returns ``{"issues": [...], "count": N, "status": "open"}``.
        Default status is 'open' — matches the FE home-card badge
        query shape ("N issues to review" counts only unresolved).
        """
        user_id = g.get("firebase_id")
        status = (request.args.get("status") or "open").strip().lower()
        if status not in _STATUS_ALLOWED:
            return jsonify({
                "error": (
                    f"status must be one of {sorted(_STATUS_ALLOWED)}; "
                    f"got {status!r}"
                ),
            }), 400

        issues = self.reconciliation_service.list_trade_issues(
            user_id, status=status,
        )
        return jsonify({
            "issues": issues,
            "count": len(issues),
            "status": status,
        }), 200

    @Logger.standardLogger
    def resolveIssue(self, issue_id):
        """POST /reconciliation/issues/<issue_id>/resolve.

        Body (optional, JSON): ``{"resolution_note": "..."}`` capped
        at 500 chars at the service boundary.

        Response:
          * 200 ``{id, resolved_at, resolution_note, idempotent}`` —
            freshly resolved OR idempotent repeat. ``idempotent=True``
            when the row was already resolved; ``resolved_at`` on a
            repeat is the ORIGINAL timestamp (not now()).
          * 400 — issue_id not an integer.
          * 404 — issue not found OR belongs to a different user.
          * 500 — DB commit failed (rolled back; next retry is safe).
        """
        user_id = g.get("firebase_id")
        try:
            issue_id_int = int(issue_id)
        except (TypeError, ValueError):
            return jsonify({
                "error": f"issue_id must be an integer; got {issue_id!r}",
            }), 400

        # Note body is OPTIONAL — a bare POST with no body is valid.
        # request.get_json(silent=True) returns None rather than
        # raising on missing / malformed body (strict=True would 400
        # on an empty body which is a reasonable resolve-without-note
        # shape).
        note = None
        if request.content_length and request.is_json:
            data = request.get_json(silent=True) or {}
            if isinstance(data, dict):
                raw_note = data.get("resolution_note")
                if isinstance(raw_note, str) and raw_note.strip():
                    note = raw_note

        result, status_code = self.reconciliation_service.resolve_trade_issue(
            user_id, issue_id_int, resolution_note=note,
        )
        return jsonify(result), status_code
