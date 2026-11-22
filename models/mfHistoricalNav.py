"""ak-v8z1 (ak-ofir Phase 2A): historical MF NAV cache.

Per-scheme-per-date NAV rows, populated by
``BackfillAMFIHistoricalNAVsTask`` from AMFI's bulk historical NAV
endpoint. Feeds the Phase 2B ``SynthesizeHistoricalSnapshotsTask``
so it can reconstruct portfolio values on past dates by combining
held quantity × NAV(target_date).

## Scope

Daily (EOD) NAVs only. AMFI publishes one NAV per scheme per
trading day — intraday NAV doesn't exist in Indian MF regulation.
Target coverage is 365 days back from today (matches
``BackfillHistoricalCandlesTask``'s stock-cache depth).

## Shape rationale

* ``scheme_code`` is the AMFI 6-digit canonical scheme code (same
  identifier mfHistoricalNav's writer parses from AMFI bulk NAV
  rows, and the same key ``MFRate.json`` already uses for
  ``scheme_id``). Varchar(20) gives margin for the ISIN-like
  identifiers some AMCs emit for direct-plan variants.
* No ``user_id`` column: MF NAVs are market-wide. Every user reads
  the same NAV. Matches the ``stockHistoricalCandles`` precedent
  (ak-4o41 Bundle 1) and keeps a single-write / multi-read shape
  so the Phase 2B synthesis script needs only one dedupe pass.
* Unique constraint ``(scheme_code, nav_date)`` enables idempotent
  upsert — re-running the backfill task over an overlap window
  does not produce dupes. Matches the
  ``stockHistoricalCandles.uq_candle_token_date`` precedent.
* ``Numeric(10, 4)`` matches the AMFI NAVAll.txt publication
  precision (4 decimal places; most schemes publish 4dp NAVs,
  a few publish 3dp — the extra place is lossless-coerce-friendly).

## Related

* ak-ofir Phase 1 (closed): feasibility note identifying AMFI's
  ``DownloadNAVHistoryReport_Po.aspx`` as the historical source +
  the 90-day chunk / 4-GET cold-start pattern this bead's writer
  implements.
* ak-ofir Phase 2B (next): synthesis script consumer.
* ak-nutd (2026-11-11): the SetAMFIMFRate / NAVAll.txt writer for
  TODAY's NAVs — sibling shape, different endpoint.
"""
from sqlalchemy import Column, Date, Integer, Numeric, String, UniqueConstraint, Index

from models.Base import Base


class MFHistoricalNav(Base):
    __tablename__ = 'mfHistoricalNavs'

    id = Column(Integer, primary_key=True, autoincrement=True)
    # AMFI 6-digit scheme code (same identifier as MFRate.json's
    # scheme_id field). Varchar(20) gives margin for ISIN-like direct-
    # plan variants that some AMCs emit alongside the numeric code.
    scheme_code = Column(String(20), nullable=False)
    nav_date = Column(Date, nullable=False)
    # 4 decimal places to match AMFI publication precision. Some
    # schemes publish 3dp; coercing a 3dp number into a 4dp column
    # is lossless so this is always safe.
    nav = Column(Numeric(10, 4), nullable=False)

    __table_args__ = (
        # Idempotent-upsert key — repeated backfill over the same
        # (scheme_code, nav_date) must not produce duplicate rows.
        # ``ON DUPLICATE KEY UPDATE`` against this UK is how the
        # writer preserves at-most-one row per (scheme, date).
        UniqueConstraint(
            'scheme_code', 'nav_date',
            name='uq_mf_hist_nav_scheme_date',
        ),
        # Lookup shape for the Phase 2B synthesis script: given a
        # target_date, enumerate (scheme_code, nav) for the user's
        # held MF schemes. The index supports both single-scheme-
        # window and date-cross-scheme queries.
        Index(
            'ix_mf_hist_nav_scheme_date',
            'scheme_code', 'nav_date',
        ),
    )
