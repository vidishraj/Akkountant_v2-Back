import os
import threading
import time
from datetime import datetime, timedelta

from flask import Flask
from sqlalchemy.orm import sessionmaker
from sqlalchemy import create_engine
from models.Jobs import Job
from enums.TaskStatusEnum import JobStatus
from services.tasks.SetNPSRate import SetNPSRate
from services.tasks.SetNPSDetails import SetNPSDetails
from services.tasks.SetStockDetails import SetStockDetails
from services.tasks.SetStocksOldData import SetStocksOldDetails
from services.tasks.SetMfRate import SetMFRate
from services.tasks.SetMfDetails import SetMFDetails
from services.tasks.SetGoldRate import SetGoldRate
from services.tasks.SetPpfRate import SetPPFRate
from services.tasks.checkMailTask import CheckMailTask
from services.tasks.checkStatementsTask import CheckStatementTask
from utils.logger import Logger


class TaskScheduler:
    def __init__(self, db_url):
        self.logger = Logger(__name__).get_logger()
        self.engine = create_engine(db_url)
        self.Session = sessionmaker(bind=self.engine)
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
                    session.add(job)
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
                session.add(job)

                if interval and status != JobStatus.FAILED.value:
                    new_job = Job(
                        title=job.title,
                        priority=job.priority,
                        status=JobStatus.PENDING.value,
                        due_date=datetime.now() + timedelta(minutes=interval),
                        user_id=job.user_id,
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
            "SetStocksOldDetails": SetStocksOldDetails,
            "SetStocksDetails": SetStockDetails,
            "SetMFRate": SetMFRate,
            "SetMFDetails": SetMFDetails,
            "SetGoldRate": SetGoldRate,
            "SetPPFRate": SetPPFRate,
            "CheckMail": CheckMailTask,
            "CheckStatement": CheckStatementTask
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
        with app.app_context():  # Push the app context for this thread
            while True:
                try:
                    self._update_overdue_jobs()
                    time.sleep(120)
                except Exception as e:
                    self.logger.error(f"Error in overdue scheduler: {e}")

    def _run_job_processor(self):
        with app.app_context():  # Push the app context for this thread
            while True:
                try:
                    self._process_pending_and_overdue_jobs()
                    time.sleep(300)
                except Exception as e:
                    self.logger.error(f"Error in job processor: {e}")


app = Flask(__name__)
if __name__ == "__main__":
    # Replace with your database URL
    DATABASE_URL = os.getenv('DATABASE_URL')

    scheduler = TaskScheduler(DATABASE_URL)
    scheduler.start_scheduler()

    app.run(port=5000, debug=False)
