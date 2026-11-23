"""ak-vue6 Phase 1: entrypoint systemd invokes for the Kite WS
daemon.

Lifecycle:
  1. Import the configured Akkountant Flask app (same precedent
     as ``services/tasks/scheduler.py:398-408`` for the ak-pib
     fleet-down guard — the configured app is the SINGLE source
     of truth for DB engine + env config).
  2. Enter app_context so BaseService.db + SQLAlchemy work.
  3. Build the daemon with:
       - access_token_provider → reads UserToken on each call
       - subscription_provider → reads holdings + resolves
         securityCode → instrument_token via kite_instruments
         (ak-v55t daily-refreshed cache)
  4. Start the IPC server on ``/run/akkountant/kite-ltp.sock``.
  5. Run the WS loop (blocks).
  6. On exit, propagate the exit code to systemd.

systemd unit expectations:
  - Type=simple
  - Restart=always + RestartSec=5
  - StartLimitIntervalSec=0 (indefinite retry on token-expired)
  - User=opc / Group=opc
  - RuntimeDirectory=akkountant (gives /run/akkountant 0755 opc:opc;
    socket inside gets 0600 via chmod from daemon)

Operator notes:
  - Logs land in journalctl -u ak-kite-ws (stdout/stderr).
  - On token-expired → daemon exits 1 → systemd restarts
    within 5s → re-reads token from DB → if still expired,
    daemon logs `state=reconnect_required` + sleeps 30s per
    cycle (no restart loop spam; bounded by the 30s wait).
"""
from __future__ import annotations

import logging
import os
import sys


# Make sure the Flask app's modules are importable. systemd invokes
# this from the repo root (WorkingDirectory in the unit file).
_REPO_ROOT = os.path.dirname(os.path.dirname(os.path.abspath(__file__)))
if _REPO_ROOT not in sys.path:
    sys.path.insert(0, _REPO_ROOT)


def _build_access_token_provider(flask_app):
    """Return a zero-arg callable that fetches the latest Kite
    access_token from the DB on each call — so a post-expiry
    re-login lands without a daemon bounce.
    """
    from models.googleTokens import UserToken

    def _provider():
        with flask_app.app_context():
            row = (
                flask_app.db.session.query(UserToken)
                .filter_by(service_type="kite")
                .first()
            )
            if row is None or not row.access_token:
                return None
            return row.access_token

    return _provider


def _build_subscription_provider(flask_app):
    """Zero-arg callable returning
    ``{"<EXCHANGE:SYMBOL>": instrument_token}`` for everything
    currently in the user's holdings.

    Resolution path:
      1. Load ``PurchasedSecurities`` where ``securityType =
         'Stocks'`` and ``buyQuant > 0``.
      2. Join to ``kite_instruments`` (ak-v55t) by
         tradingsymbol → instrument_token.
      3. Pick exchange from the ``kite_instruments`` row
         (equity rows carry NSE or BSE).

    Unresolved symbols are logged + skipped — they most likely
    indicate a stale instrument cache (SetKiteStockDetails or
    InstrumentsCacheTask hasn't refreshed). The daemon
    continues with the resolvable subset.
    """
    from models.kiteInstrument import KiteInstrument
    from models.purchasedSecurities import PurchasedSecurities

    def _provider():
        with flask_app.app_context():
            held = (
                flask_app.db.session.query(PurchasedSecurities)
                .filter(
                    PurchasedSecurities.securityType == "Stocks",
                    PurchasedSecurities.buyQuant > 0,
                )
                .all()
            )
            if not held:
                return {}
            codes = {h.securityCode for h in held if h.securityCode}
            if not codes:
                return {}
            rows = (
                flask_app.db.session.query(KiteInstrument)
                .filter(KiteInstrument.tradingsymbol.in_(codes))
                .all()
            )
            out = {}
            for r in rows:
                if r.exchange and r.tradingsymbol and r.instrument_token:
                    key = f"{r.exchange}:{r.tradingsymbol}"
                    out[key] = int(r.instrument_token)
            return out

    return _provider


def _configure_logging():
    """systemd captures stdout+stderr → journal. Keep format
    terse (timestamps come from the journal).
    """
    logging.basicConfig(
        level=logging.INFO,
        format="%(levelname)s %(name)s %(message)s",
        stream=sys.stderr,
    )
    return logging.getLogger("kite_ws_daemon")


def main() -> int:
    logger = _configure_logging()

    api_key = os.getenv("KITE_API_KEY")
    if not api_key:
        logger.error(
            "KITE_API_KEY env var missing — infra must set this on "
            "ak-kite-ws.service (same as app.service / schedular."
            "service)"
        )
        return 2

    # Socket path overridable via env var so dev-run on non-infra
    # boxes can point at $XDG_RUNTIME_DIR. Prod systemd unit sets
    # this to /run/akkountant/kite-ltp.sock.
    socket_path = os.getenv(
        "AKKOUNTANT_KITE_LTP_SOCKET",
        "/run/akkountant/kite-ltp.sock",
    )

    # Import the configured app lazily — pulls the full dep chain
    # (SQLAlchemy / Firebase / etc). Done inside main() so the
    # module is importable in tests that don't need the app.
    from app import app as flask_app  # noqa: E402
    from services.kite_ws_daemon import KiteWsDaemon

    daemon = KiteWsDaemon(
        api_key=api_key,
        access_token_provider=_build_access_token_provider(flask_app),
        subscription_provider=_build_subscription_provider(flask_app),
        logger=logger,
    )
    daemon.start_ipc_server(socket_path=socket_path)
    try:
        return daemon.run_ws_loop()
    finally:
        daemon.stop()


if __name__ == "__main__":  # pragma: no cover — entrypoint
    sys.exit(main())
