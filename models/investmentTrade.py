"""ak-7y6o (ak-8kp4 Phase 3a, 2026-11-23) — historical record of
equity DEMAT DELIVERIES reconstructed from the weekly-securities-
statement PDFs published by the broker.

**Grain (ak-4t7f, measured on the real PDFs):** one row per
SETTLED demat delivery, i.e. one row per (ISIN, settlement
number, direction) movement in or out of the demat account. It
is NOT one row per executed exchange trade: a single delivery
can net several trades in the same settlement. `trade_date`
holds the PDF's "Transaction Date", which on settled rows is the
DEMAT-MOVEMENT date (T+1 or later), not the exchange execution
date. Whether to change this grain or source a true trade date
(e.g. from the Phase 3b tradebook) is an open Overseer
data-model decision; the schema is unchanged here.

**NOT the same table as `investmentSnapshots`.** That table is
PER-PERIOD portfolio aggregate (one row per
`(date, user, investment_type)` with totals). This one is PER-
DELIVERY (one row per settled demat movement, see Grain). The
two coexist:
Phase 3c (future bead) will aggregate from THIS table into the
per-period snapshot.

Price-NULL semantic (Phase 3a → Phase 3b handoff):
    * Phase 3a (this parser): only `symbol / isin / qty /
      trade_date / transaction_type` are available in the
      weekly PDFs. `price` is inserted as NULL.
    * Phase 3b (future bead): Overseer uploads Kite Console
      tradebook CSVs; a follow-up script UPDATEs
      `price = ?, price_source = 'console_csv'` WHERE `price
      IS NULL` AND the (user, isin, trade_date, settlement_no,
      transaction_type) key matches. Also INSERTS trades older
      than the prod-data window (pre-2022-10).
    * Phase 3c (future bead): per-FY aggregator reads this
      table + current holdings to backfill
      `investmentSnapshots` rows.

Query helper `pending_price_fill` (SELECT ... WHERE price IS
NULL) will be added in a FE/BE view once Phase 3b lands — not
in this bead's scope.

Idempotency: the uniqueness tuple
`(user_id, isin, trade_date, settlement_no, transaction_type)`
is stable across re-runs of the ingest script; a second run
inserts zero new rows.

ak-7y6o reviewer CRITICAL (2026-11-23): `settlement_no` is
NOT NULL. SQL `UNIQUE` semantics treat NULL ≠ NULL on EVERY
major backend (MySQL, SQLite, PostgreSQL) — a NULL-settlement
row's uniqueness check never collides with another NULL-
settlement row, so duplicates would slip through and Phase 3c
aggregation would double-count. Pre-reviewer the model shipped
nullable=True with a false "dedup falls back to the four-
field key" claim in the docstring — reviewer demonstrated the
defect empirically on in-memory SQLite. The parser enforces
this invariant at parse time (rejects rows lacking a
settlement_no with a WARN log + rejections["missing_
settlement"] counter); the DB constraint is the belt-and-brace
in case a future caller bypasses the parser.

Phase 3b price-fill dependency: `UPDATE investmentTrades SET
price = ? WHERE settlement_no = ?` must match on a definite
value; NULL-settlement rows would silently never receive a
price fill. Same defect class; same fix.

Schema install: `db.create_all()` (app.py `_setup_database`)
picks up this table on next boot — no manual ALTER, no infra
coordination required. The existing `investmentSnapshots`
table is NOT touched.
"""
from sqlalchemy import (
    Column, DateTime, Date, ForeignKey, Index, Integer, Numeric,
    String, UniqueConstraint, func,
)
from sqlalchemy.orm import relationship

from models.Base import Base


class InvestmentTrade(Base):
    __tablename__ = 'investmentTrades'

    id = Column(Integer, primary_key=True, autoincrement=True)
    user_id = Column(
        String(100),
        ForeignKey('users.userID', ondelete='CASCADE'),
        nullable=False,
    )

    # ak-4t7f: `trade_date` = the PDF "Transaction Date", which
    # for these settled rows is the DEMAT-MOVEMENT date (T+1 or
    # later), NOT the exchange execution date. `settlement_date`
    # = the PDF "Execution Date" (identical to trade_date on
    # every settled row measured). True exchange trade dates
    # are pending the Overseer's data-model decision (see the
    # module docstring's Grain section).
    trade_date = Column(Date, nullable=False)
    settlement_date = Column(Date, nullable=True)

    # Security identification. ISIN is the canonical cross-
    # broker key; `symbol` (scrip name) is retained for human-
    # readable displays + is the only field in the Kite Console
    # CSV that carries over if an ISIN mapping drifts.
    isin = Column(String(20), nullable=False)
    symbol = Column(String(50), nullable=False)

    # Signed semantics: `quantity` is always POSITIVE;
    # `transaction_type` disambiguates buy/sell. This matches
    # the PDF's Qty Delivered (sell) / Qty Received (buy)
    # fields: the parser infers transaction_type from which
    # qty column is non-zero. It is the DELIVERED quantity for
    # the settlement, which may net several exchange trades.
    quantity = Column(Numeric(14, 4), nullable=False)
    transaction_type = Column(String(10), nullable=False)
    # 'BUY' | 'SELL'

    # Segment classification (EQ = equity cash, F&O, CDS, MCX).
    segment = Column(String(10), nullable=False)

    # Settlement number — required for correctness of dedup +
    # Phase 3b price-fill. ak-7y6o reviewer CRITICAL: SQL
    # UNIQUE treats NULL ≠ NULL on MySQL/SQLite/Postgres so a
    # NULL-settlement row would slip past the uniqueness
    # constraint; Phase 3c would double-count. Also Phase 3b
    # `UPDATE ... WHERE settlement_no = ?` would never match
    # NULL. Enforce NOT NULL at the DB layer as belt-and-brace
    # against any future caller that bypasses the parser's
    # parse-time reject-on-missing-settlement guard.
    settlement_no = Column(String(20), nullable=False)

    # Price fields — NULL during Phase 3a, populated in
    # Phase 3b from Kite Console tradebook CSV.
    price = Column(Numeric(14, 4), nullable=True)
    price_source = Column(String(20), nullable=True)
    # 'console_csv' | 'inferred' | NULL (pending Phase 3b)

    # Provenance for audit + per-source backfill-replay.
    source = Column(String(50), nullable=False)
    # 'zerodha_weekly_securities_pdf' | 'kite_console_tradebook'
    source_file = Column(String(500), nullable=True)
    # Path or content-hash of the source document.

    created_at = Column(DateTime, server_default=func.now())

    __table_args__ = (
        # Idempotency — a second ingest run on the same PDFs
        # produces zero new rows. settlement_no is NOT NULL
        # (ak-7y6o reviewer CRITICAL): SQL UNIQUE treats NULL
        # ≠ NULL so dedup would miss NULL-settlement rows;
        # the parser's reject-on-missing-settlement guard +
        # the NOT NULL DB constraint together guarantee the
        # uniqueness tuple is total.
        UniqueConstraint(
            'user_id', 'isin', 'trade_date',
            'settlement_no', 'transaction_type',
            name='uq_trade_idempotent',
        ),
        Index('idx_trade_user_date', 'user_id', 'trade_date'),
        Index('idx_trade_user_isin', 'user_id', 'isin'),
        # Phase 3b query helper — fast "which trades need
        # price fill?" scan.
        Index('idx_trade_price_null', 'user_id', 'price'),
    )

    user_relationship = relationship(
        'User', back_populates='investment_trades',
    )
