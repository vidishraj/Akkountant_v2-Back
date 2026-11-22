"""ak-99gt (2026-11-22): runtime 301-WARN probe for NSE host moves.

Complements the ak-3hkb CI drift guard. The CI guard catches a STALE
LITERAL in ``utils.nse_patch._BASE_URL`` (someone reverts to .com
or lets the constant drift). This runtime probe catches a FUTURE
host move — NSE's next migration from .in to somewhere else — at
the FIRST scheduler restart after the move, instead of letting
the fleet silently render ₹0.00 / -100% stocks until an operator
notices.

Mechanism: a single HTTP HEAD against ``_BASE_URL + "BHEL"`` with
``allow_redirects=False``, timeout 5s. A 3xx response means NSE
moved hosts and nsefetch's non-redirect-following behavior will
produce empty equityResponse → silent-zero-stocks (the ak-3hkb
prod symptom). Log WARN with the current base URL AND the
Location header so an operator can grep for the exact migration
target.

Cost: one HEAD request per ``scheduler.start_scheduler()`` call.
The scheduler typically runs for days between restarts; at steady
state this is zero traffic. Deliberately HEAD not GET — half the
bytes, same 3xx signal.

Fail-soft contract: ANY exception (network error, DNS fail, timeout,
TLS error) is swallowed + logged at INFO. The probe is diagnostic;
a flaky NSE at boot must NOT prevent the scheduler from starting.
See ``probe()`` for the error-class table.
"""
from __future__ import annotations

import logging

# Symbol to probe. Chosen because it's a mid-cap always-listed equity
# the Overseer actually holds — NSE is guaranteed to return either a
# 200 (if the URL is current) or a redirect (if the host moved). An
# unlisted symbol could return 404 even on the current host, which
# would mask a redirect signal.
_PROBE_SYMBOL = "BHEL"

# HTTP-layer timeout. 5s is well above the sub-second NSE response
# time at steady state and short enough that a dead host doesn't
# stretch scheduler boot by more than a few seconds total (we add
# the probe to start_scheduler which runs before daemon threads
# kick off).
_PROBE_TIMEOUT_SECONDS = 5

# User-Agent header — nsepython's rahu attaches a browser-like UA
# to its fetches so NSE's edge doesn't 403 the request. We mirror
# a minimal UA here. Stays a plain constant (not imported from
# nsepython) so the probe is independent of library internals.
_PROBE_UA = (
    "Mozilla/5.0 (akkountant/ak-99gt nse_health probe)"
)


def probe(logger: logging.Logger | None = None) -> None:
    """Fire the one-shot runtime 301-WARN probe.

    Called once by ``TaskScheduler.start_scheduler()`` on scheduler
    boot. Expected steady-state: HEAD returns 200 against the
    current ``utils.nse_patch._BASE_URL`` + BHEL → silent (no log
    noise). On migration-moved: HEAD returns 3xx with a Location
    header pointing at the new host → WARN log tagged for grep,
    scheduler keeps booting.

    Any exception (requests not installed, DNS error, timeout, TLS
    error, nse_patch not importable) is caught and logged at INFO.
    Probe failures must NEVER prevent the scheduler from starting.

    Args:
        logger: optional logger to receive the WARN. Defaults to
            the module logger so journalctl -u schedular.service
            picks it up alongside the other utils.nse_* lines.
    """
    log = logger or logging.getLogger(__name__)
    try:
        # Late imports — keeps the module import-cheap and avoids
        # tangling with nse_patch's monkey-patch at import time.
        import requests
        from utils import nse_patch

        base_url = getattr(nse_patch, "_BASE_URL", None)
        if not base_url:
            log.info(
                "nse_health: utils.nse_patch._BASE_URL not set; "
                "skipping redirect probe"
            )
            return

        probe_url = base_url + _PROBE_SYMBOL
        response = requests.head(
            probe_url,
            allow_redirects=False,
            timeout=_PROBE_TIMEOUT_SECONDS,
            headers={"User-Agent": _PROBE_UA},
        )
        status = response.status_code
        location = response.headers.get("Location", "")

        if 300 <= status < 400:
            # Migration signal. Loud enough an operator grepping
            # journalctl for "NSE may have moved" lands directly
            # on the message with enough context (old URL + new
            # Location) to patch utils.nse_patch._BASE_URL.
            log.warning(
                "nse_health: NSE may have moved hosts — HEAD %s "
                "returned %d; Location=%s. "
                "nsefetch does NOT follow cross-TLD redirects; "
                "stocks will render as 0 until "
                "utils.nse_patch._BASE_URL is updated. See bead "
                "ak-3hkb for the last migration (prod symptom: "
                "₹0.00 / -100%% stocks for every user).",
                probe_url, status, location,
            )
        else:
            # Any non-3xx — 200 (healthy), 4xx (API-shape change
            # that nsefetch might still handle), 5xx (NSE flapping).
            # Log at INFO so an operator sees "probe ran and found
            # these" on scheduler restart, but no alarm.
            log.info(
                "nse_health: HEAD %s → %d (no redirect; "
                "_BASE_URL appears current)",
                probe_url, status,
            )
    except Exception as exc:
        # Fail-soft per contract. ``probe`` must not crash the
        # scheduler. INFO level so a steady NSE outage at boot
        # doesn't fill the WARN stream, but the operator still
        # sees it on grep.
        log.info(
            "nse_health: redirect probe skipped due to error "
            "(%s: %s). This is diagnostic-only; scheduler continues.",
            type(exc).__name__, exc,
        )
