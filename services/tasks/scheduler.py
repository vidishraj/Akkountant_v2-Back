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
from utils.logger import Logger


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
        session = self.Session()
        try:
            self.logger.info("Processing pending and overdue jobs...")
            jobs_to_process = session.query(Job).filter(
                Job.status.in_([JobStatus.OVERDUE.value])
            ).all()

            for job in jobs_to_process:
                self.logger.info(f"Processing job: {job.title} (ID: {job.id})")

                task_class = self._get_task_class(job.title)
                if not task_class:
                    self.logger.warning(f"No task class found for title: {job.title}")
                    continue

                task_instance = task_class(job.title, job.priority)
                task_instance.init_runner(job)
                result, status, interval = task_instance.startTask()

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
                    new_job = Job(
                        title=job.title,
                        priority=job.priority,
                        status=JobStatus.PENDING.value,
                        due_date=datetime.now() + timedelta(minutes=interval),
                        user_id=job.user_id,
                        failures=0 if status != JobStatus.FAILED.value else job.failures
                    )
                    session.add(new_job)
                session.commit()
        except Exception as e:
            self.logger.error(f"Error processing jobs: {e}")
            session.rollback()
        finally:
            session.close()

    def _get_task_class(self, title):
        task_mapping = {
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
            "InvestmentHistoryTask": InvestmentHistoryTask
        }
        return task_mapping.get(title)

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
            while True:
                try:
                    self._update_overdue_jobs()
                    time.sleep(120)
                except Exception as e:
                    self.logger.error(f"Error in overdue scheduler: {e}")

    def _run_job_processor(self):
        with self.flask_app.app_context():
            while True:
                try:
                    self._process_pending_and_overdue_jobs()
                    time.sleep(300)
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
