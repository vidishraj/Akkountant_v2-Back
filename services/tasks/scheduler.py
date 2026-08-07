import os
import threading
import time
from datetime import datetime, timedelta

from sqlalchemy.orm import sessionmaker
from sqlalchemy import create_engine
from models.Jobs import Job
from enums.TaskStatusEnum import JobStatus
from services.tasks.InvestmentHistoryTask import InvestmentHistoryTask
from services.tasks.SetNPSRate import SetNPSRate
from services.tasks.SetNPSDetails import SetNPSDetails
from services.tasks.SetKiteStockDetails import SetKiteStockDetails
from services.tasks.SetMfRate import SetMFRate
from services.tasks.SetMfDetails import SetMFDetails
from services.tasks.SetIBJAGoldRate import SetIBJAGoldRate
from services.tasks.SetPpfRate import SetPPFRate
from services.tasks.SetEPFRate import SetEPFRate
from services.tasks.checkMailTask import CheckMailTask
from services.tasks.checkStatementsTask import CheckStatementTask
from services.tasks.SetStockOldCodes import SetStockOldCodes
from services.tasks.WealthDigestTask import WealthDigestTask
from services.tasks.wealth_digest_constants import WEALTH_DIGEST_JOB_TITLE
from utils.logger import Logger


TASK_MAPPING = {
    "SetNPSRate": SetNPSRate,
    "SetNPSDetails": SetNPSDetails,
    "SetStocksDetails": SetKiteStockDetails,
    "SetStocksOldDetails": SetStockOldCodes,
    "SetMFRate": SetMFRate,
    "SetMFDetails": SetMFDetails,
    "SetGoldRate": SetIBJAGoldRate,
    "SetPPFRate": SetPPFRate,
    "SetEPFRate": SetEPFRate,
    "CheckMail": CheckMailTask,
    "CheckStatement": CheckStatementTask,
    "InvestmentHistoryTask": InvestmentHistoryTask,
    # ak-ran Phase 1 + v2 #5: daily proactive wealth-management digest
    # at 06:00 IST. Bootstrap script (bootstrap_wealth_digest.py)
    # seeds the first Pending Job; subsequent runs self-reschedule
    # at 24h cadence per WealthDigestTask.interval. Routing key is
    # WEALTH_DIGEST_JOB_TITLE — shared across scheduler / jobsObject
    # / bootstrap so a typo becomes a compile-time NameError instead
    # of a silent routing miss.
    WEALTH_DIGEST_JOB_TITLE: WealthDigestTask,
}


def get_task_class(title):
    """Get the task class for a given job title."""
    return TASK_MAPPING.get(title)


class TaskScheduler:
    def __init__(self, db_url, flask_app=None):
        self.logger = Logger(__name__).get_logger()
        self.engine = create_engine(db_url)
        self.Session = sessionmaker(bind=self.engine)
        self.flask_app = flask_app
        self.threads_initialized = False
        self.thread_locks = threading.Lock()

    def _update_overdue_jobs(self):
        session = self.Session()
        try:
            self.logger.info("Updating overdue jobs...")
            current_time = datetime.now()
            pending_jobs = session.query(Job).filter(Job.status == JobStatus.PENDING.value).all()

            for job in pending_jobs:
                if job.due_date < current_time:
                    job.status = JobStatus.OVERDUE.value
                    # session.add(job)
            session.commit()
        except Exception as e:
            self.logger.error(f"Error updating overdue jobs: {e}")
            session.rollback()
        finally:
            session.close()

    def _process_pending_and_overdue_jobs(self):
        from utils.DateTimeUtil import clamp_to_allowed_window
        session = self.Session()
        try:
            self.logger.info("Processing pending and overdue jobs...")
            jobs_to_process = session.query(Job).filter(
                Job.status.in_([JobStatus.OVERDUE.value])
            ).all()

            # Deduplicate: only process one job per title (pick the oldest)
            seen_titles = set()
            deduplicated = []
            duplicates = []
            for job in jobs_to_process:
                if job.title in seen_titles:
                    duplicates.append(job)
                else:
                    seen_titles.add(job.title)
                    deduplicated.append(job)

            # Mark duplicate overdue jobs as completed to prevent re-processing
            for dup in duplicates:
                dup.status = JobStatus.COMPLETED.value
                dup.result = "Skipped (duplicate)"

            for job in deduplicated:
                self.logger.info(f"Processing job: {job.title} (ID: {job.id})")

                task_class = self._get_task_class(job.title)
                if not task_class:
                    self.logger.warning(f"No task class found for title: {job.title}")
                    continue

                task_instance = task_class(job.title, job.priority)
                task_instance.init_runner(job)
                try:
                    result, status, interval = task_instance.startTask()
                finally:
                    # ak-ojd v2 MAJOR: reset the app's scoped_session
                    # between tasks. Post-Path-B, the scheduler holds
                    # ONE long-lived app_context for the entire
                    # while-True lifetime → Flask-SQLAlchemy's per-
                    # request teardown-session-remove never fires →
                    # the app scoped_session is a SINGLE long-lived
                    # session shared by every task. Pre-Path-B, the
                    # fresh-engine-per-access branch gave accidental
                    # isolation; without this remove(), a task that
                    # leaves the session in pending-rollback state
                    # (raised mid-txn without rolling back) will
                    # cascade a PendingRollbackError into the next
                    # task's first query. session.remove() bounds
                    # error-cascade + identity-map growth to a single
                    # task boundary — standard Flask-SQLAlchemy
                    # long-running-loop hygiene.
                    #
                    # Best-effort: never let cleanup crash the loop.
                    try:
                        self.flask_app.db.session.remove()
                    except Exception as cleanup_exc:
                        self.logger.warning(
                            f"scheduler: session.remove() after "
                            f"{job.title} failed (non-fatal): "
                            f"{cleanup_exc}"
                        )

                self.logger.info(f"Job result: {result}, status: {status}")
                job.result = result
                job.status = JobStatus[status.upper()].value

                # Track failures: increment on failure, reset on success
                if status == JobStatus.FAILED.value:
                    job.failures = min(job.failures + 1, 10)
                    if job.failures == 10:
                        self.logger.error(f"Reached failure limit for job {job.title}, will not reschedule")
                else:
                    job.failures = 0

                if interval and job.failures < 10:
                    # Only create next job if no Pending one already exists for this title
                    existing_pending = session.query(Job).filter(
                        Job.title == job.title,
                        Job.status == JobStatus.PENDING.value
                    ).first()
                    if not existing_pending:
                        raw_due = datetime.now() + timedelta(minutes=interval)
                        clamped_due = clamp_to_allowed_window(raw_due)
                        new_job = Job(
                            title=job.title,
                            priority=job.priority,
                            status=JobStatus.PENDING.value,
                            due_date=clamped_due,
                            user_id=job.user_id,
                            failures=0 if status != JobStatus.FAILED.value else job.failures
                        )
                        session.add(new_job)

            session.commit()
            if duplicates:
                self.logger.info(f"Cleaned up {len(duplicates)} duplicate overdue jobs")
        except Exception as e:
            self.logger.error(f"Error processing jobs: {e}")
            session.rollback()
        finally:
            session.close()

    def _get_task_class(self, title):
        return get_task_class(title)

    def start_scheduler(self):
        with self.thread_locks:
            if self.threads_initialized:
                self.logger.info("Scheduler threads are already running.")
                return

            self.logger.info("Starting scheduler threads...")
            threading.Thread(target=self._run_overdue_scheduler, daemon=True).start()
            threading.Thread(target=self._run_job_processor, daemon=True).start()
            self.threads_initialized = True

    def _run_overdue_scheduler(self):
        with self.flask_app.app_context():
            # ak-ojd Path B defense-in-depth: set g.db explicitly at
            # the task-invocation boundary (mirrors reprocess_*.py
            # pattern). Redundant with the Base_Service.db
            # current_app.db fallback (which handles the case where
            # g.db is not set), but explicit-at-boundary is clearer
            # for anyone reading scheduler.py — makes the "background
            # task must share the app's stable engine" contract
            # visible AT the task-runner entry point instead of
            # implicit via the primitive property.
            from flask import g
            g.db = self.flask_app.db
            while True:
                try:
                    self._update_overdue_jobs()
                    time.sleep(120)
                except Exception as e:
                    self.logger.error(f"Error in overdue scheduler: {e}")
                finally:
                    # ak-ojd v2 MAJOR (symmetric with _process loop):
                    # reset the app scoped_session per tick even
                    # though this loop's payload uses a scheduler-
                    # local session for its writes. Defense-in-depth
                    # against any future _update_overdue_jobs
                    # refactor that reaches self.flask_app.db.session
                    # for reads, and cheap symmetry with the per-task
                    # remove() below.
                    try:
                        self.flask_app.db.session.remove()
                    except Exception:
                        pass

    def _run_job_processor(self):
        from utils.DateTimeUtil import is_within_allowed_window, seconds_until_allowed_window
        with self.flask_app.app_context():
            # ak-ojd Path B defense-in-depth: see _run_overdue_scheduler
            # comment above.
            from flask import g
            g.db = self.flask_app.db
            while True:
                try:
                    if is_within_allowed_window():
                        self._process_pending_and_overdue_jobs()
                        time.sleep(300)
                    else:
                        wait = seconds_until_allowed_window()
                        self.logger.info(f"Outside 1-7AM IST window, sleeping {wait/3600:.1f}h")
                        time.sleep(wait)
                except Exception as e:
                    self.logger.error(f"Error in job processor: {e}")


if __name__ == "__main__":
    from flask import Flask

    app = Flask(__name__)
    # Replace with your database URL
    DATABASE_URL = os.getenv('DATABASE_URL')

    scheduler = TaskScheduler(DATABASE_URL, flask_app=app)
    scheduler.start_scheduler()

    app.run(port=5000, debug=False)
