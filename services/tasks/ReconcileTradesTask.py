"""ak-zai2 Kite Bundle 2: daily broker-statement reconciliation.

Cross-checks Kite's reported trades (via ``KiteService.get_trades``)
against locally-imported trades (``TradeAssociation`` table). Flags
mismatches as ``ReconciliationIssue`` rows for FE dashboard surfacing.

## Cadence

Scheduled self-reschedule interval = 24h. Daily post-market-close is
the natural rhythm: Kite's ``get_trades()`` returns TODAY's executed
trades (not historical), so one run per day is enough. Running sub-
daily duplicates work; running weekly misses the window.

If a run is missed (scheduler gap), the NEXT run catches any
same-day gap — but Kite doesn't return historical trades through
``get_trades()`` (it's today-only), so gap-day discrepancies are
unrecoverable through this task. Acceptable trade-off: statement
imports are batch events, not real-time, so a one-day miss doesn't
lose real signal.

## Discrepancy class (Phase 2 initial ship)

ONE class in this commit — ``missing_from_statement``:
  * Kite reports a trade_id that is NOT in local TradeAssociation
  * Likely cause: statement import hasn't caught up, statement
    parser missed a row, or Overseer hasn't uploaded today's
    tradebook yet

Future classes (qty_mismatch / price_mismatch / missing_from_kite)
will land as follow-up beads with their own comparison paths.

## Idempotency

ReconciliationIssue has UK ``(user_id, kite_trade_id,
discrepancy_type)``. Re-flagging the same trade on the next cycle
UPDATEs the detected_at timestamp instead of inserting a duplicate.
Cleared by explicit resolution (operator UI or future auto-resolve
when statement catches up).

## Degradation

If Kite is unauthenticated / unreachable, the task returns Failed
and does NOT touch the ReconciliationIssue table. Existing flagged
issues stay flagged (no spurious resolution); next successful run
re-evaluates.
"""

from __future__ import annotations

import json
from datetime import datetime, timedelta, timezone

from models.googleTokens import UserToken
from models.reconciliationIssue import (
    ReconciliationIssue,
    RECONCILIATION_DISCREPANCY_TYPES,
)
from models.stockTrade import TradeAssociation
from services.KiteService import KiteService, KiteAuthError, KiteTokenExpired
from services.tasks.baseTask import BaseTask
from utils.logger import Logger


IST = timezone(timedelta(hours=5, minutes=30))

# Self-reschedule cadence (minutes). Daily.
_RUN_INTERVAL_MINUTES = 60 * 24

# Discrepancy class shipped in this commit. Future classes extend
# RECONCILIATION_DISCREPANCY_TYPES + a corresponding comparison path.
_DISCREPANCY_MISSING_FROM_STATEMENT = "missing_from_statement"


class ReconcileTradesTask(BaseTask):
    """Daily Kite-vs-local trade reconciliation (ak-zai2 Bundle 2)."""

    _instance = None

    def __new__(cls, *args, **kwargs):
        if not cls._instance:
            cls._instance = super().__new__(cls)
        return cls._instance

    def __init__(self, title, priority):
        if not hasattr(self, "initialized"):
            super().__init__(title, priority)
            self.logger = Logger(__name__).get_logger()
            self.kite_service = KiteService()
            self.interval = _RUN_INTERVAL_MINUTES
            self.initialized = True

    # ---------------- BaseTask.run contract ----------------

    def run(self):
        """Fetch Kite trades + flag discrepancies vs local
        TradeAssociation. Returns the ``(message, status, interval)``
        3-tuple matching every scheduled rate task.
        """
        try:
            user_id = self._get_first_kite_user()
            if not user_id:
                return (
                    "No user with Kite access token — skipping reconciliation",
                    "Failed",
                    self.interval,
                )

            # Cheapest probe before enumerating state (same pattern as
            # BackfillHistoricalCandlesTask + SetKiteStockDetails).
            if not self.kite_service.check_token_liveness(user_id):
                return (
                    "Kite token expired — reconnect required",
                    "Failed",
                    self.interval,
                )

            try:
                kite_trades = self.kite_service.get_trades(user_id)
            except KiteTokenExpired:
                return (
                    "Kite token expired mid-run — reconnect required",
                    "Failed",
                    self.interval,
                )
            except KiteAuthError as exc:
                return (
                    f"Kite auth error: {exc}",
                    "Failed",
                    self.interval,
                )

            if not kite_trades:
                return (
                    "No Kite trades today — nothing to reconcile",
                    "Completed",
                    self.interval,
                )

            flagged = self._flag_missing_from_statement(user_id, kite_trades)

            try:
                self.db.session.commit()
            except Exception as exc:
                self.db.session.rollback()
                return (
                    f"Reconciliation commit failed: {exc}"[:800],
                    "Failed",
                    self.interval,
                )

            msg = (
                f"Reconciled {len(kite_trades)} Kite trades for "
                f"user={user_id[:8]}...; flagged {flagged} "
                f"missing_from_statement"
            )
            self.logger.info(f"{self.title}: {msg}")
            return msg, "Completed", self.interval

        except Exception as exc:
            return str(exc)[:800], "Failed", self.interval

    # ---------------- comparison logic ----------------

    def _flag_missing_from_statement(self, user_id, kite_trades):
        """UPSERT ReconciliationIssue rows for Kite trades absent from
        local TradeAssociation. Returns count of flagged trades.

        Kite's trade_id is the join key — ``TradeAssociation.tradeID``
        is populated at statement-import time with the same canonical
        Zerodha trade ID that Kite reports here. Simple set-
        difference match.
        """
        # Enumerate local trade IDs ONCE per run. Small table
        # (one row per imported trade) so full-scan is fine.
        local_trade_ids = {
            row.tradeID
            for row in self.db.session.query(TradeAssociation.tradeID).all()
        }

        flagged = 0
        from sqlalchemy.dialects.mysql import insert as mysql_insert

        for trade in kite_trades:
            kite_trade_id = str(trade.get("trade_id") or "").strip()
            if not kite_trade_id:
                continue
            if kite_trade_id in local_trade_ids:
                continue

            # Build the detail payload. Kite's trade shape varies by
            # version (tradingsymbol vs symbol, trade_date formats etc.)
            # — defensive .get everywhere; detail is advisory for the
            # operator surface, not a strict schema.
            detail_json = json.dumps(
                {
                    "symbol": trade.get("tradingsymbol"),
                    "exchange": trade.get("exchange"),
                    "transaction_type": trade.get("transaction_type"),
                    "quantity": trade.get("quantity"),
                    "price": _json_default(trade.get("price")),
                    "trade_date": _json_default(trade.get("trade_date")),
                    "order_id": trade.get("order_id"),
                    "instrument_token": trade.get("instrument_token"),
                },
                default=_json_default,
            )

            stmt = mysql_insert(ReconciliationIssue).values(
                user_id=user_id,
                kite_trade_id=kite_trade_id,
                discrepancy_type=_DISCREPANCY_MISSING_FROM_STATEMENT,
                discrepancy_detail=detail_json,
            )
            # Re-detection refreshes detected_at (via NOW() default
            # value through inserted.detected_at ref would require
            # setting detected_at explicitly; SQLAlchemy's server_default
            # kicks in on INSERT, not ON DUPLICATE KEY UPDATE, so we
            # refresh detail here. detected_at stays at first-detection
            # by design — "when did we first see this" is more useful
            # for operator triage than "most recent sighting").
            upsert = stmt.on_duplicate_key_update(
                discrepancy_detail=stmt.inserted.discrepancy_detail,
            )
            self.db.session.execute(upsert)
            flagged += 1

        return flagged

    # ---------------- helpers ----------------

    def _get_first_kite_user(self):
        """First user with a Kite access token on file.

        Single-user app today; same pattern as
        SetKiteStockDetails / BackfillHistoricalCandlesTask.
        """
        try:
            user_token = (
                self.db.session.query(UserToken)
                .filter_by(service_type="kite")
                .first()
            )
            if user_token and user_token.access_token:
                return user_token.user_id
            return None
        except Exception as exc:
            self.logger.error(f"Error getting Kite user: {exc}")
            return None


def _json_default(obj):
    """ak-zai2: json.dumps default for Kite's native types.

    Handles datetime / date / Decimal from Kite SDK responses. Falls
    back to ``str(obj)`` for anything else so the discrepancy_detail
    column never fails serialization.
    """
    from datetime import date
    from decimal import Decimal
    if isinstance(obj, (datetime, date)):
        return obj.isoformat()
    if isinstance(obj, Decimal):
        return str(obj)
    if obj is None:
        return None
    return str(obj)
