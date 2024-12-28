import threading
import time
from datetime import datetime, timedelta

from flask import Flask

from enums.TaskStatusEnum import JobStatus
from models.Jobs import Job
from services.tasks.SetNPSRate import SetNPSRate  # Add imports for specific task classes
from services.tasks.SetNPSDetails import SetNPSDetails  # Add imports for specific task classes
from services.tasks.SetStockDetails import SetStockDetails  # Add imports for specific task classes
from services.tasks.SetStocksOldData import SetStocksOldDetails # Add imports for specific task classes
from services.tasks.SetMfRate import SetMFRate  # Add imports for specific task classes
from services.tasks.SetMfDetails import SetMFDetails  # Add imports for specific task classes
from services.tasks.SetGoldRate import SetGoldRate  # Add imports for specific task classes
from services.tasks.SetPpfRate import SetPPFRate  # Add imports for specific task classes
from utils.logger import Logger


class TaskScheduler:
    """Scheduler to manage and run tasks at specific intervals."""

    def __init__(self, db, app: Flask):
        self.logger = Logger(__name__).get_logger()
        self.app = app
        self.db = db

    def _update_overdue_jobs(self):
        """Fetch all rows in 'Pending' state and update those beyond their due time to 'Overdue'."""
        session = self.db.session()
        try:
            self.logger.info("Updating pending rows to overdue")
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
        """
        Fetch rows in 'Pending' or 'Overdue' state, run the corresponding task,
        and update the job rows with results, status, and next interval.
        """
        session = self.db.session()
        try:
            jobs_to_process = session.query(Job).filter(
                Job.status.in_([JobStatus.OVERDUE.value])
            ).all()

            for job in jobs_to_process:
                self.logger.info(f"Processing job: {job.title} (ID: {job.id})")

                # Dynamically select the task class based on the job title
                task_class = self._get_task_class(job.title)
                if not task_class:
                    self.logger.warning(f"No task class found for title: {job.title}")
                    continue

                # Initialize and run the task
                task_instance = task_class(job.title, job.priority)
                task_instance.init_runner(job)

                result, status, interval = task_instance.startTask()
                self.logger.info(f"Finished job with result and status {result} {status}")
                # Update the current job
                job.result = result
                job.status = JobStatus[status.upper()].value
                session.add(job)

                # Create a new job for the next interval
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
        """Map job titles to their corresponding task classes."""
        task_mapping = {
            "SetNPSRate": SetNPSRate,
            "SetNPSDetails": SetNPSDetails,
            "SetStocksOldDetails": SetStocksOldDetails,
            "SetStocksDetails": SetStockDetails,
            "SetMFRate": SetMFRate,
            "SetMFDetails": SetMFDetails,
            "SetGoldRate": SetGoldRate,
            "SetPPFRate": SetPPFRate
        }
        return task_mapping.get(title)

    def start_scheduler(self):
        """Start the scheduler with two threads for overdue updates and job processing."""
        def run_scheduler():
            while True:
                with self.app.app_context():
                    try:
                        self._update_overdue_jobs()
                        time.sleep(120)  # Check for overdue jobs every 2 minutes
                    except Exception as e:
                        self.logger.error(f"Error in overdue job scheduler: {e}")
        def process_jobs():
            while True:
                with self.app.app_context():
                    try:
                        self._process_pending_and_overdue_jobs()
                        time.sleep(300)  # Process jobs every 5 minutes
                    except Exception as e:
                        self.logger.error(f"Error in job processor: {e}")

        threading.Thread(target=run_scheduler, daemon=True).start()
        threading.Thread(target=process_jobs, daemon=True).start()
