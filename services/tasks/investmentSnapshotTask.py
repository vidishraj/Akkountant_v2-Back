"""
Daily investment snapshot daemon.
Runs once per day at ~11PM IST, captures portfolio state for all users
into the investmentSnapshots table for historical growth tracking.
"""

import threading
from datetime import datetime, timedelta, timezone
from decimal import Decimal, ROUND_DOWN

from flask import g

from utils.logger import Logger

IST = timezone(timedelta(hours=5, minutes=30))
TARGET_HOUR = 23
TARGET_MINUTE = 0

TYPE_LABELS = {
    'Stocks': 'Stocks',
    'Mutual_Funds': 'MF',
    'NPS': 'NPS',
}


class InvestmentSnapshotTask:

    def __init__(self, flask_app, investment_service):
        self.logger = Logger(__name__).get_logger()
        self.flask_app = flask_app
        self.investment_service = investment_service
        self._stop_event = threading.Event()
        self._thread = None

    def start(self):
        self._thread = threading.Thread(target=self._run_loop, daemon=True)
        self._thread.start()
        self.logger.info("InvestmentSnapshotTask daemon started.")

    def stop(self):
        self._stop_event.set()

    # ── Scheduling ──────────────────────────────────────────────────────

    def _seconds_until_target(self):
        """Calculate seconds until next 11:00 PM IST."""
        now = datetime.now(IST)
        target = now.replace(hour=TARGET_HOUR, minute=TARGET_MINUTE, second=0, microsecond=0)
        if now >= target:
            target += timedelta(days=1)
        return (target - now).total_seconds()

    def _run_loop(self):
        # Startup delay — let Flask fully boot
        if self._stop_event.wait(timeout=60):
            return

        while not self._stop_event.is_set():
            wait_seconds = self._seconds_until_target()
            self.logger.info(
                f"InvestmentSnapshotTask: next run in {wait_seconds / 3600:.1f}h"
            )

            if self._stop_event.wait(timeout=wait_seconds):
                break

            try:
                self._run_once()
            except Exception as e:
                self.logger.error(f"InvestmentSnapshotTask cycle error: {e}", exc_info=True)

    # ── Core logic ──────────────────────────────────────────────────────

    def _run_once(self):
        with self.flask_app.app_context():
            from models.users import User
            from models.investmentSnapshot import InvestmentSnapshot

            g.db = self.flask_app.extensions.get("sqlalchemy")

            try:
                all_users = g.db.session.query(User.userID).all()
            except Exception as e:
                self.logger.error(f"Failed to query users: {e}")
                return

            today = datetime.now(IST).date()
            self.logger.info(
                f"Running investment snapshots for {len(all_users)} user(s) on {today}"
            )

            for (user_id,) in all_users:
                try:
                    self._snapshot_user(user_id, today, InvestmentSnapshot)
                except Exception as e:
                    self.logger.error(
                        f"Snapshot failed for user {user_id}: {e}", exc_info=True
                    )

    def _snapshot_user(self, user_id, snapshot_date, SnapshotModel):
        g.firebase_id = user_id
        svc = self.investment_service
        snapshots = []

        # ── Stocks, MF, NPS ────────────────────────────────────────────
        for stype, label in TYPE_LABELS.items():
            try:
                total_invested = svc.StockService.getActiveMoneyInvested(stype, user_id)
                if total_invested == 0:
                    snapshots.append(self._make_row(
                        snapshot_date, user_id, label, 0, 0, 0, 0
                    ))
                    continue
                profit_data = svc.StockService.calculateProfitAndCurrentValue(stype, user_id)
                total_profit = sum(v['profit'] for v in profit_data.values())
                current_value = total_invested + total_profit
                pct = (total_profit / total_invested) * 100 if total_invested else 0
                snapshots.append(self._make_row(
                    snapshot_date, user_id, label,
                    total_invested, current_value, total_profit, pct
                ))
            except Exception as e:
                self.logger.error(f"Error computing {stype} for {user_id}: {e}")

        # ── PPF ────────────────────────────────────────────────────────
        try:
            ppf = svc.PPFService.fetchComplete(user_id)
            if ppf and ppf.get('net'):
                net = float(ppf['net'])
                profit = float(ppf.get('netProfit', 0))
                invested = net - profit
                pct = (profit / invested * 100) if invested else 0
                snapshots.append(self._make_row(
                    snapshot_date, user_id, 'PPF', invested, net, profit, pct
                ))
            else:
                snapshots.append(self._make_row(
                    snapshot_date, user_id, 'PPF', 0, 0, 0, 0
                ))
        except Exception as e:
            self.logger.error(f"Error computing PPF for {user_id}: {e}")

        # ── EPF ────────────────────────────────────────────────────────
        try:
            epf = svc.EPFService.fetchComplete(user_id)
            if epf and epf.get('net'):
                net = float(epf['net'])
                profit = float(epf.get('netProfit', 0))
                invested = net - profit
                pct = (profit / invested * 100) if invested else 0
                snapshots.append(self._make_row(
                    snapshot_date, user_id, 'EPF', invested, net, profit, pct
                ))
            else:
                snapshots.append(self._make_row(
                    snapshot_date, user_id, 'EPF', 0, 0, 0, 0
                ))
        except Exception as e:
            self.logger.error(f"Error computing EPF for {user_id}: {e}")

        # ── Gold ───────────────────────────────────────────────────────
        try:
            gold = svc.GoldService.fetchComplete(user_id)
            if gold and gold.get('net'):
                net = float(gold['net'])
                profit = float(gold.get('netProfit', 0))
                invested = net - profit
                pct = (profit / invested * 100) if invested else 0
                snapshots.append(self._make_row(
                    snapshot_date, user_id, 'Gold', invested, net, profit, pct
                ))
            else:
                snapshots.append(self._make_row(
                    snapshot_date, user_id, 'Gold', 0, 0, 0, 0
                ))
        except Exception as e:
            self.logger.error(f"Error computing Gold for {user_id}: {e}")

        # ── Upsert ────────────────────────────────────────────────────
        db = g.db
        db.session.query(SnapshotModel).filter(
            SnapshotModel.date == snapshot_date,
            SnapshotModel.user == user_id,
        ).delete()
        for snap in snapshots:
            db.session.add(snap)
        db.session.commit()
        self.logger.info(
            f"Saved {len(snapshots)} snapshots for user {user_id} on {snapshot_date}"
        )

    @staticmethod
    def _make_row(date, user_id, inv_type, invested, current, profit, pct):
        from models.investmentSnapshot import InvestmentSnapshot
        return InvestmentSnapshot(
            date=date,
            user=user_id,
            investment_type=inv_type,
            total_invested=Decimal(str(invested)).quantize(Decimal('0.01'), rounding=ROUND_DOWN),
            current_value=Decimal(str(current)).quantize(Decimal('0.01'), rounding=ROUND_DOWN),
            profit=Decimal(str(profit)).quantize(Decimal('0.01'), rounding=ROUND_DOWN),
            profit_percent=Decimal(str(pct)).quantize(Decimal('0.0001'), rounding=ROUND_DOWN),
        )
