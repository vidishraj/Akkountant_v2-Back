"""ak-v55t: daily refresh of the Kite instrument master cache.

Kite's ``instruments()`` endpoint returns ~200k rows (full exchange
master across NSE / BSE / NFO / BFO / MCX / CDS). Calling it live
on every search keystroke would be cost-prohibitive AND would burn
Kite rate limits very quickly. This task runs once per day, pulls
the master, and UPSERTs into ``kite_instruments``. The autocomplete
surface (``GET /kite/instruments/search``) then serves DB-backed
LIKE queries with no Kite dependency.

## Cadence

Scheduled self-reschedule interval = 24h. Exchange master changes
slowly — new listings, delistings, F&O expiries. Daily refresh is
plenty for an autocomplete use case. Same-day search misses for a
freshly-listed symbol are an acceptable trade-off; the operator
can trigger-rate-refresh on demand if needed.

## Idempotency

UPSERT by ``instrument_token`` (BigInteger PK on the table). Re-
running over the same day's instrument set is safe — identical
rows update in-place, no dupes. Chunked INSERT so a 200k-row
dump doesn't blow the MySQL max_allowed_packet.

Stale-row handling: rows absent from a fresh fetch are NOT deleted
by default. Rationale: a KITE blip that returns a truncated list
shouldn't nuke every instrument that didn't appear in that one
truncated response. A separate housekeeping bead can add an
optional tombstone-sweep later when the shape of "legitimately
delisted" vs "transient absent" is understood.

## Dependencies

* ``KiteService.get_all_instruments`` (line 318) is the fetch.
  Routes through ``_call`` so token-expired failures surface as
  ``KiteTokenExpired`` and the task returns Failed → next cycle
  retries after the user reconnects.
* ``kite_instruments`` model (ak-v55t) is the UPSERT target.

## Blast radius

Zero. Separate table, separate task, separate scheduler entry.
The search endpoint degrades cleanly to empty result when the
table is empty (pre-first-run / post-ALTER-pre-backfill).
"""

from __future__ import annotations

from decimal import Decimal, InvalidOperation
from datetime import date as date_cls, datetime

from models.googleTokens import UserToken
from models.kiteInstrument import KiteInstrument
from services.KiteService import KiteService, KiteAuthError, KiteTokenExpired
from services.tasks.baseTask import BaseTask
from utils.logger import Logger


# How many rows to INSERT per statement. 200k rows at once blows
# past MySQL's ~16MB max_allowed_packet default; 2000-row chunks
# fit comfortably under 1MB each.
_UPSERT_CHUNK_SIZE = 2000

# Self-reschedule cadence (minutes). Once daily.
_RUN_INTERVAL_MINUTES = 60 * 24


class InstrumentsCacheTask(BaseTask):
    """Daily Kite instrument-master refresh (ak-v55t)."""

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

    def run(self):
        """Fetch + UPSERT the full Kite instrument master.

        Returns the 3-tuple ``(message, status, interval)``:
          * ``Completed`` on at-least-one-row-upserted.
          * ``Failed`` on no-kite-user, token-expired, auth-error,
            or empty-fetch (unexpected; Kite should always return
            something).
        """
        try:
            user_id = self._get_first_kite_user()
            if not user_id:
                return (
                    "No user with Kite access token — "
                    "skipping instruments refresh",
                    "Failed",
                    self.interval,
                )

            # Cheapest probe BEFORE fetching 200k rows — fail fast
            # on the Kite 6 AM IST expiry window without wasting
            # the big GET.
            if not self.kite_service.check_token_liveness(user_id):
                return (
                    "Kite token expired — reconnect required",
                    "Failed",
                    self.interval,
                )

            try:
                instruments = self.kite_service.get_all_instruments(
                    user_id,
                )
            except KiteTokenExpired:
                return (
                    "Kite token expired mid-fetch — reconnect required",
                    "Failed",
                    self.interval,
                )
            except KiteAuthError as exc:
                return (
                    f"Kite auth error: {exc}"[:800],
                    "Failed",
                    self.interval,
                )

            if not instruments:
                return (
                    "Kite returned empty instrument list — unexpected",
                    "Failed",
                    self.interval,
                )

            upserted = self._upsert_instruments(instruments)

            try:
                self.db.session.commit()
            except Exception as exc:
                self.db.session.rollback()
                return (
                    f"Instruments commit failed: {exc}"[:800],
                    "Failed",
                    self.interval,
                )

            msg = (
                f"Completed: {upserted} instruments upserted "
                f"(fetched {len(instruments)} rows)"
            )
            self.logger.info(f"{self.title}: {msg}")
            return msg, "Completed", self.interval

        except Exception as exc:
            return str(exc)[:800], "Failed", self.interval

    # ---------------- helpers ----------------

    def _upsert_instruments(self, instruments):
        """Chunked UPSERT of Kite instrument dicts into
        ``kite_instruments``. Returns count of rows attempted.

        Kite returns a list of dicts; each dict carries the fields
        the model stores (instrument_token / tradingsymbol / name
        / lot_size / instrument_type / segment / exchange / tick_size
        / last_price / expiry / strike / exchange_token). Nullable
        fields are coerced via ``_or_none`` so missing / empty-string
        / NaN values land as SQL NULL rather than quote-string-of-
        empty.
        """
        from sqlalchemy.dialects.mysql import insert as mysql_insert

        total_upserted = 0
        for i in range(0, len(instruments), _UPSERT_CHUNK_SIZE):
            chunk = instruments[i:i + _UPSERT_CHUNK_SIZE]
            rows = []
            for inst in chunk:
                token = inst.get("instrument_token")
                if token is None:
                    continue
                try:
                    token_int = int(token)
                except (TypeError, ValueError):
                    continue
                rows.append(
                    {
                        "instrument_token": token_int,
                        "exchange_token": _int_or_none(
                            inst.get("exchange_token")
                        ),
                        "tradingsymbol": (
                            str(inst.get("tradingsymbol") or "")[:80]
                        ),
                        "name": _str_or_none(inst.get("name"), 200),
                        "lot_size": _int_or_none(inst.get("lot_size")),
                        "instrument_type": _str_or_none(
                            inst.get("instrument_type"), 10,
                        ),
                        "segment": _str_or_none(inst.get("segment"), 20),
                        "exchange": _str_or_none(
                            inst.get("exchange"), 10,
                        ),
                        "tick_size": _dec_or_none(inst.get("tick_size")),
                        "last_price": _dec_or_none(inst.get("last_price")),
                        "expiry": _date_or_none(inst.get("expiry")),
                        "strike": _dec_or_none(inst.get("strike")),
                    }
                )
            if not rows:
                continue
            stmt = mysql_insert(KiteInstrument).values(rows)
            upsert = stmt.on_duplicate_key_update(
                exchange_token=stmt.inserted.exchange_token,
                tradingsymbol=stmt.inserted.tradingsymbol,
                name=stmt.inserted.name,
                lot_size=stmt.inserted.lot_size,
                instrument_type=stmt.inserted.instrument_type,
                segment=stmt.inserted.segment,
                exchange=stmt.inserted.exchange,
                tick_size=stmt.inserted.tick_size,
                last_price=stmt.inserted.last_price,
                expiry=stmt.inserted.expiry,
                strike=stmt.inserted.strike,
            )
            self.db.session.execute(upsert)
            total_upserted += len(rows)
        return total_upserted

    def _get_first_kite_user(self):
        """First user with a Kite access token on file.

        Single-user app today; same pattern as
        BackfillHistoricalCandlesTask / ReconcileTradesTask.
        """
        from sqlalchemy.exc import SQLAlchemyError
        try:
            user_token = (
                self.db.session.query(UserToken)
                .filter_by(service_type="kite")
                .first()
            )
            if user_token and user_token.access_token:
                return user_token.user_id
            return None
        except (AttributeError, NameError, TypeError):
            raise
        except SQLAlchemyError as exc:
            self.logger.error(f"Error getting Kite user (DB): {exc}")
            return None
        except Exception as exc:
            self.logger.error(
                f"Error getting Kite user (unexpected): {exc}",
                exc_info=True,
            )
            return None


# ---------------- module-level coercion helpers ----------------
#
# Kite's dict values are heterogeneous — some fields are
# empty-string-when-missing, some are pandas-NaN-when-missing
# (depending on how kiteconnect SDK parses the response CSV).
# Centralized coercion keeps _upsert_instruments readable.


def _int_or_none(v):
    """Coerce to int; empty / None / NaN / unparseable → None."""
    if v is None:
        return None
    if isinstance(v, str) and not v.strip():
        return None
    try:
        return int(v)
    except (TypeError, ValueError):
        return None


def _dec_or_none(v):
    """Coerce to Decimal; empty / None / unparseable → None."""
    if v is None:
        return None
    if isinstance(v, str) and not v.strip():
        return None
    try:
        return Decimal(str(v))
    except (TypeError, ValueError, InvalidOperation):
        return None


def _str_or_none(v, max_len):
    """Coerce to length-bounded str; empty / None → None."""
    if v is None:
        return None
    s = str(v).strip()
    if not s:
        return None
    return s[:max_len]


def _date_or_none(v):
    """Coerce to date; empty / None / unparseable → None.

    Kite's SDK returns either a ``datetime.date``, a ``datetime.
    datetime``, or an empty string for expiry fields depending on
    segment (equity has none; F&O / currency carry values). Be
    defensive across all three.
    """
    if v is None:
        return None
    if isinstance(v, date_cls) and not isinstance(v, datetime):
        return v
    if isinstance(v, datetime):
        return v.date()
    if isinstance(v, str):
        s = v.strip()
        if not s:
            return None
        # Observed formats: 'YYYY-MM-DD' or ISO datetime.
        try:
            return datetime.fromisoformat(s).date()
        except ValueError:
            try:
                return datetime.strptime(s[:10], "%Y-%m-%d").date()
            except ValueError:
                return None
    return None
