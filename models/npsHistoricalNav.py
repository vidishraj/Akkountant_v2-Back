"""ak-v8z1 (ak-ofir Phase 2A): historical NPS NAV cache.

Per-scheme-per-date NAV rows persisted FROM the existing
``SetNPSRate`` 12-month Excel pull. The upstream URL
(``scheme-wise-nav-report-excel?...navyearselxls=12...``) already
returns 12 months of history every cycle; pre-ak-v8z1
``extractRatesFromExcel`` kept only ``latest_row.iloc[0]`` + a
handful of discrete points (yesterday / lastWeek / sixMonthsAgo)
and DROPPED the rest. This table captures the dropped rows so the
Phase 2B synthesis script can reconstruct NPS values on past
dates by combining held quantity × NAV(target_date).

## Scope

Daily (EOD) NAVs for all PFM × scheme combinations the existing
SetNPSRate task iterates (14 PFMs × 16 schemes = 224 candidate
combos). Coverage is bound by what npstrust.org.in publishes —
historically 12 months via ``navyearselxls=12``. Longer windows
would need a URL-parameter change (unverified; out of scope for
this bead).

## Shape rationale

* ``scheme_id`` is the NPS canonical scheme identifier (same shape
  as ``NPSRATE.json``'s ``scheme_id`` field — e.g. ``SM001001``).
  Varchar(20) covers the 8-char canonical form with margin for
  future NPS Trust renaming.
* No ``user_id`` column: NPS NAVs are market-wide — every
  subscriber of the same scheme sees the same NAV. Matches
  ``mfHistoricalNavs`` + ``stockHistoricalCandles`` precedents.
* Unique constraint ``(scheme_id, nav_date)`` enables idempotent
  upsert — the SetNPSRate task runs every 60min today (interval =
  60 per SetNPSRate.py:49); re-running within a trading day must
  not spawn duplicates when the same 12-month window lands
  repeatedly.
* ``Numeric(10, 4)`` matches NPS Trust publication precision
  (4dp NAV values in the Excel/TSV response).

## Related

* ak-ofir Phase 1 (closed): feasibility note — "NPS historical
  NAV already downloaded, just discarded". This bead captures it.
* ak-ofir Phase 2B (next): synthesis script consumer.
* services/tasks/SetNPSRate.py (extended by this bead): writer
  side — the Excel-parse loop now persists every row instead of
  dropping all-but-one.
"""
from sqlalchemy import Column, Date, Integer, Numeric, String, UniqueConstraint, Index

from models.Base import Base


class NPSHistoricalNav(Base):
    __tablename__ = 'npsHistoricalNavs'

    id = Column(Integer, primary_key=True, autoincrement=True)
    # NPS canonical scheme identifier (e.g. ``SM001001``). Same key
    # NPSRATE.json's scheme_id uses so the Phase 2B synthesis script
    # can join seamlessly across current + historical rows.
    scheme_id = Column(String(20), nullable=False)
    nav_date = Column(Date, nullable=False)
    nav = Column(Numeric(10, 4), nullable=False)

    __table_args__ = (
        UniqueConstraint(
            'scheme_id', 'nav_date',
            name='uq_nps_hist_nav_scheme_date',
        ),
        Index(
            'ix_nps_hist_nav_scheme_date',
            'scheme_id', 'nav_date',
        ),
    )
