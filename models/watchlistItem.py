"""ak-05af: persistent watchlist of symbols the Overseer wants to
monitor without holding them.

Category C #16 of the Kite capability roadmap. Backs the FE
"My Watchlist" surface — grid of symbol cards with live LTP +
day-change + a delete affordance. Prereq ``kite_instruments``
(ak-v55t) supplies the autocomplete for add-symbol; live LTP
polls ``/watchlist/ltp`` until the WebSocket bead (ak-vue6)
lands.

## Shape rationale

* ``user_id`` is PK-component (NOT absent as "single-user, who
  cares"). Column is cheap; keeping it means a future multi-
  user move isn't a security defect waiting to be scanned in.
  Same precedent as ``ReconciliationIssue`` (ak-cipj) +
  ``PurchasedSecurities``.
* ``instrument_token`` is Kite's canonical 64-bit identifier
  (stable across symbol renames) — same column type as
  ``KiteInstrument.instrument_token`` so FK integrity is
  possible if a future bead wants it. We DO NOT emit an FK
  today: the master is refreshed by a daily task and rare
  purges could cascade-delete watchlist rows — an orphaned
  watchlist entry is less bad than a silent delete.
* ``tradingsymbol`` + ``exchange`` are copied at insert time
  (not joined at read time) so a watchlist render doesn't need
  the kite_instruments cache to be alive / warm. These are
  effectively a point-in-time snapshot of what the user asked
  to track. Exchange rename / symbol delist is a rare enough
  case that the stale row is acceptable (user can delete +
  re-add).
* ``added_at`` NOT NULL server_default now() — sort key for
  the default list view.
* ``note`` nullable String(500) — optional operator-supplied
  annotation. Same shape + cap as ak-cipj ``resolution_note``
  (convenience field; truncated silently at the service
  boundary rather than 400'd). PUT /watchlist/<id>/note edits
  it.

## Uniqueness + indexes

* ``(user_id, instrument_token)`` unique — a symbol in the
  user's watchlist exactly once. Add-existing is idempotent
  (returns the existing row; see ``WatchlistService.add_item``).
* ``(user_id, added_at)`` index — list query orders
  by ``added_at DESC`` scoped by ``user_id``; this index covers
  both filter + sort without a tablescan.

## Related

* ak-v55t ``KiteInstrument`` — the autocomplete source feeding
  add-symbol. Not FK'd (see above).
* ak-vue6 WebSocket bead (deferred) — when it lands, FE drops
  the ``/watchlist/ltp`` polling fallback for live LTP pushes.
"""
from sqlalchemy import (
    BigInteger, Column, DateTime, ForeignKey, Index, Integer,
    String, UniqueConstraint,
)
from sqlalchemy.sql import func

from models.Base import Base


class WatchlistItem(Base):
    __tablename__ = 'watchlist_items'

    id = Column(Integer, primary_key=True, autoincrement=True)
    user_id = Column(
        String(100),
        ForeignKey('users.userID', ondelete='CASCADE'),
        nullable=False,
    )
    # Kite's 64-bit canonical token (matches KiteInstrument PK
    # column type). No FK — see module docstring rationale.
    instrument_token = Column(BigInteger, nullable=False)
    # Snapshot at add time; render surface doesn't join
    # kite_instruments to display these.
    tradingsymbol = Column(String(80), nullable=False)
    exchange = Column(String(10), nullable=False)
    added_at = Column(
        DateTime,
        nullable=False,
        server_default=func.now(),
    )
    # ak-05af: optional operator note capped at the column width
    # (500) at the service boundary. Longer input is truncated
    # silently. See WatchlistService.update_note.
    note = Column(String(500), nullable=True)

    __table_args__ = (
        # One symbol per user — add-existing is idempotent.
        UniqueConstraint(
            'user_id', 'instrument_token',
            name='uq_watchlist_user_instrument',
        ),
        # Covers the primary list query:
        # WHERE user_id = ? ORDER BY added_at DESC.
        Index('ix_watchlist_user_added', 'user_id', 'added_at'),
    )
