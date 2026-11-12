"""ak-zai2 Kite Bundle 2: broker-statement reconciliation log.

Per-trade discrepancy records produced by
``ReconcileTradesTask`` when it compares Kite-reported trades
against local ``TradeAssociation`` rows. Surfaces mismatches the FE
dashboard flags for Overseer review ("3 reconciliation discrepancies
on recent imports").

## Scope

Phase 2 of Bundle 2 ships ONE discrepancy class:

  * ``missing_from_statement`` — Kite reports a ``trade_id`` that is
    NOT in the local ``TradeAssociation`` table. Likely meaning: the
    Zerodha tradebook statement import either hasn't happened yet or
    missed this trade (OCR drift / parse regression / pre-upload
    backfill gap).

Future discrepancy classes (per bead's Phase 2 wording "price/qty
mismatch"):

  * ``qty_mismatch``   — tradeID matches but Kite's quantity differs
                         from the imported ``buyQuant`` / ``sellQuant``.
  * ``price_mismatch`` — tradeID matches but Kite's price differs.
  * ``missing_from_kite`` — a local TradeAssociation row has no
                            matching Kite trade (less common; implies
                            the statement contains a trade Kite didn't
                            record — a parse-side false positive).

Shape designed so adding a new class is a one-line enum extension +
corresponding comparison path in ReconcileTradesTask. No schema
change required.

## Idempotency

Unique constraint ``(user_id, kite_trade_id, discrepancy_type)``. The
scheduled task re-runs every cycle — an already-flagged discrepancy
must not spawn duplicate rows. ``INSERT ... ON DUPLICATE KEY UPDATE
detected_at = NOW()`` semantics on re-detection; resolution cleared
via explicit mutation (operator UI or a resolved-sweep follow-up).
"""
from sqlalchemy import (
    Column, DateTime, Integer, String, Text, ForeignKey, UniqueConstraint, Index
)
from sqlalchemy.sql import func

from models.Base import Base


#: Allowed values for ``discrepancy_type``. Enforced application-side
#: (not as a DB-level ENUM) so a future class addition doesn't need a
#: migration. Validation happens at ReconcileTradesTask upsert time.
RECONCILIATION_DISCREPANCY_TYPES = frozenset({
    "missing_from_statement",  # Phase 2 initial class (Bundle 2 ship)
    "missing_from_kite",       # Future: local-only trade
    "qty_mismatch",            # Future: tradeID match, qty diverges
    "price_mismatch",          # Future: tradeID match, price diverges
})


class ReconciliationIssue(Base):
    __tablename__ = 'reconciliation_issues'

    id = Column(Integer, primary_key=True, autoincrement=True)
    user_id = Column(
        String(100),
        ForeignKey('users.userID', ondelete='CASCADE'),
        nullable=False,
    )
    #: The Kite trade_id that triggered the discrepancy. For
    #: ``missing_from_kite`` (future), this holds the local
    #: TradeAssociation.tradeID instead; the discrepancy_type field
    #: disambiguates. One of the two sides always owns the canonical id.
    kite_trade_id = Column(String(64), nullable=False)
    discrepancy_type = Column(String(32), nullable=False)
    #: Free-form JSON string carrying class-specific details
    #: (e.g. {"symbol": "RELIANCE", "qty_kite": 10, "qty_local": 5}).
    #: TEXT not JSON type so sqlite + MySQL both work without a dialect
    #: flag — the ReconcileTradesTask handles json.dumps/loads.
    discrepancy_detail = Column(Text, nullable=True)
    detected_at = Column(
        DateTime,
        nullable=False,
        server_default=func.now(),
    )
    #: Operator-set via future resolution UI. Nullable so a freshly-
    #: detected issue has no resolved_at until explicitly cleared.
    resolved_at = Column(DateTime, nullable=True)

    __table_args__ = (
        UniqueConstraint(
            'user_id', 'kite_trade_id', 'discrepancy_type',
            name='uq_reconciliation_user_tradeid_type',
        ),
        # Lookup by user + unresolved-only (FE dashboard badge) — most
        # queries filter resolved_at IS NULL + user_id = ?.
        Index('ix_recon_user_resolved', 'user_id', 'resolved_at'),
    )
