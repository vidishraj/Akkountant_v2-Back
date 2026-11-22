"""ak-v8z1 (ak-ofir Phase 2A): historical gold rate cache.

Per-date gold spot-price rows. Unlike MF / NPS / Stocks, there is
NO auto-fetcher for historical gold data — per the Overseer's
2026-11-22 scope decision (ak-ofir Phase 1 §5), IBJA's scraper is
TODAY-only and historical IBJA data isn't published. Options to
re-add historical coverage (metalpriceapi.com, goldapi.io) were
deferred to a separate bead; this table exists so a one-shot
loader script can pull overseer-provided CSV/JSON history into
the backfill-synthesis stream if needed.

## Scope

Per-date single-rate rows at a single purity (24-carat spot per
gram, the canonical IBJA headline figure). The producing
loader is ``scripts/load_gold_historical_rates.py`` (this bead);
Phase 2B's synthesis script treats gold like other
asset classes but skips dates with no row rather than inventing
values.

## Shape rationale

* ``date`` as PRIMARY KEY: only one spot rate per date (no
  per-purity variant stored today). If a future bead wants to
  store 24/22/18 separately, the shape would migrate to
  ``(date, carat)`` composite PK — not blocked by this
  single-purity choice.
* ``rate_per_gram Numeric(10, 2)``: paise precision; matches
  IBJA headline format.
* ``source Varchar(50)``: operator-provided label — e.g.
  ``"goodreturns"``, ``"metalpriceapi"``, ``"manual-entry"``.
  Captures provenance so a future operator can diff the loaded
  rows against a fresh download without ambiguity about where
  numbers came from.

## Related

* ak-ofir Phase 1 (closed): feasibility note §5 — "LOW
  confidence: skip historical gold OR use metalpriceapi".
  Overseer chose to defer the vendor-dependency decision;
  this bead provides the TABLE + LOADER so a later bead can
  flip on auto-fetch without a schema change.
* ak-ofir Phase 2B (next): synthesis script consumer — treats
  missing dates as "gold slice skipped for this snapshot"
  rather than inventing values.
* scripts/load_gold_historical_rates.py (this bead): one-shot
  CSV/JSON loader driven by operator-provided files.
"""
from sqlalchemy import Column, Date, Numeric, String

from models.Base import Base


class GoldHistoricalRate(Base):
    __tablename__ = 'goldHistoricalRates'

    # Date PK — single rate per date (single-purity scope).
    date = Column(Date, primary_key=True)
    # Paise precision; matches IBJA headline format.
    rate_per_gram = Column(Numeric(10, 2), nullable=False)
    # Operator-provided provenance label (e.g. 'goodreturns',
    # 'metalpriceapi', 'manual-entry'). Captured so a future diff
    # against a fresh download is traceable.
    source = Column(String(50), nullable=False)
