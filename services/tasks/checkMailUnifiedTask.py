"""
Cron task for scheduled unified mail processing.
Follows the CronAgent daemon pattern — runs as a background thread.
"""

import threading
from datetime import datetime, timedelta

from flask import g

from utils.logger import Logger


class CheckMailUnifiedTask:
    """
    Background daemon that periodically runs the unified mail processing pipeline.
    Unlike CronAgent which uses AI for scheduling decisions, this is a simple
    interval-based runner.
    """

    def __init__(self, flask_app, mail_processor, interval_seconds=3600):
        self.logger = Logger(__name__).get_logger()
        self.flask_app = flask_app
        self.mail_processor = mail_processor
        self.interval_seconds = interval_seconds
        self._stop_event = threading.Event()
        self._thread = None

    def start(self):
        """Launch the daemon thread."""
        self._thread = threading.Thread(target=self._run_loop, daemon=True)
        self._thread.start()
        self.logger.info(
            f"CheckMailUnifiedTask started (interval: {self.interval_seconds}s)"
        )

    def stop(self):
        """Signal the daemon to shut down."""
        self._stop_event.set()

    def _run_loop(self):
        """Wait for Flask to initialize, then run on interval."""
        # Startup delay — let Flask fully boot
        if self._stop_event.wait(timeout=60):
            return

        while not self._stop_event.is_set():
            try:
                self._run_once()
            except Exception as e:
                self.logger.error(f"CheckMailUnifiedTask cycle error: {e}", exc_info=True)

            # Interruptible sleep
            if self._stop_event.wait(timeout=self.interval_seconds):
                break

    def _run_once(self):
        """Execute a single email processing cycle inside Flask app context."""
        with self.flask_app.app_context():
            # Get all users with Gmail tokens
            from models import UserToken
            from enums.ServiceTypeEnum import ServiceTypeEnum

            g.db = self.flask_app.extensions.get("sqlalchemy")

            try:
                users_with_gmail = (
                    g.db.session.query(UserToken.user_id)
                    .filter_by(service_type=ServiceTypeEnum.Gmail.value)
                    .distinct()
                    .all()
                )
            except Exception as e:
                self.logger.error(f"Failed to query users with Gmail tokens: {e}")
                return

            if not users_with_gmail:
                self.logger.info("No users with Gmail tokens found, skipping cycle")
                return

            # Process last 24 hours for each user
            now = datetime.now()
            yesterday = now - timedelta(days=1)
            date_from = yesterday.strftime("%Y/%m/%d")
            date_to = now.strftime("%Y/%m/%d")

            for (user_id,) in users_with_gmail:
                try:
                    g.firebase_id = user_id
                    self.logger.info(f"Running unified mail processing for user {user_id}")
                    result = self.mail_processor.process_emails(user_id, date_from, date_to)
                    self.logger.info(
                        f"User {user_id} mail processing complete: "
                        f"fetched={result.get('total_emails_fetched', 0)}, "
                        f"financial={result.get('financial_emails', 0)}, "
                        f"text={result.get('text_emails_processed', 0)}, "
                        f"pdf={result.get('pdf_emails_processed', 0)}"
                    )
                except Exception as e:
                    self.logger.error(
                        f"Mail processing failed for user {user_id}: {e}",
                        exc_info=True,
                    )
