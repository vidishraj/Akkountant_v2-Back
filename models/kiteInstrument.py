"""ak-v55t: cached Kite instrument master for the search / autocomplete
surface.

Kite's ``instruments()`` endpoint returns the full exchange master
(~200k rows across NSE / BSE / NFO / BFO / MCX / CDS) as a CSV-shaped
list of dicts. Calling it live on every search keystroke would be
cost-prohibitive and would also burn through the Kite rate limits
very quickly. This table caches the master and is refreshed daily by
``InstrumentsCacheTask``.

## Shape rationale

* ``instrument_token`` is Kite's canonical 64-bit identifier (stable
  across symbol renames). Used as PK so a daily full-table re-sync
  can UPSERT without caring about exchange-side renames.
* ``exchange_token`` is a legacy field some endpoints still emit;
  kept nullable since not every segment populates it reliably.
* ``tradingsymbol`` + ``name`` are the two search targets — both
  indexed for LIKE prefix / substring queries.
* Numeric fields (``last_price``, ``strike``, ``tick_size``) stay
  nullable; equity rows typically carry only ``last_price`` while
  options rows carry ``strike`` + lot_size. Leaving them nullable
  avoids painful per-segment branching in the writer.
* ``expiry`` nullable; only F&O / currency segments emit it. Native
  Date column so filter-by-expiry queries don't need date parsing.
* No ``user_id`` — instruments are market-wide. Same precedent as
  ``stockHistoricalCandles`` (ak-4o41) + ``mfHistoricalNavs``
  (ak-v8z1).

## Indexes

* ``ix_kite_inst_tradingsymbol`` + ``ix_kite_inst_name`` support
  the LIKE-on-tradingsymbol / LIKE-on-name search shape. MySQL uses
  the left-prefix of a BTREE index for LIKE 'prefix%' queries.
* ``ix_kite_inst_exchange`` scopes the exchange-filter chip query.
  Cardinality is low (~6 distinct values) so this is a filter-first
  helper rather than a selectivity win.

## Idempotency

Daily UPSERT by ``InstrumentsCacheTask`` on ``instrument_token`` PK.
Rows absent from a fresh fetch are NOT deleted by default — a
future bead can wire a soft-delete sweep if tombstoned-symbol
hygiene becomes a problem. Pre-existing rows get refreshed
in-place.

## Related

* ak-zai2 Bundle 2 writes a very similar fetch via
  ``get_trades`` but that's the TRADE log, not the instrument
  master. No overlap.
* ``SetKiteStockDetails`` task writes to disk JSON (not a DB
  table). That path remains the source for the agent's
  symbol→token resolver on the holdings side; this bead's table
  is a DIFFERENT surface focused on search.
"""
from sqlalchemy import (
    BigInteger, Column, Date, Index, Integer, Numeric, String,
)

from models.Base import Base


class KiteInstrument(Base):
    __tablename__ = 'kite_instruments'

    # Kite's canonical 64-bit token (stable across renames).
    instrument_token = Column(BigInteger, primary_key=True)
    # Legacy field; some segments emit, others don't.
    exchange_token = Column(BigInteger, nullable=True)
    tradingsymbol = Column(String(80), nullable=False)
    name = Column(String(200), nullable=True)
    # F&O / derivative fields — nullable because equity rows don't
    # carry them.
    lot_size = Column(Integer, nullable=True)
    instrument_type = Column(String(10), nullable=True)
    segment = Column(String(20), nullable=True)
    exchange = Column(String(10), nullable=True)
    tick_size = Column(Numeric(14, 4), nullable=True)
    last_price = Column(Numeric(14, 4), nullable=True)
    expiry = Column(Date, nullable=True)
    strike = Column(Numeric(14, 4), nullable=True)

    __table_args__ = (
        # Primary search paths: LIKE on tradingsymbol + name.
        Index('ix_kite_inst_tradingsymbol', 'tradingsymbol'),
        Index('ix_kite_inst_name', 'name'),
        # Filter chip — exchange-scoped queries.
        Index('ix_kite_inst_exchange', 'exchange'),
    )
