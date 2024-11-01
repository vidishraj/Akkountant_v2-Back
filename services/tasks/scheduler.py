import threading
import time

from services.tasks.baseTask import BaseTask
from utils.logger import Logger


class TaskScheduler:
    """Scheduler to manage and run tasks at specific intervals."""

    def __init__(self):
        self.tasks = []
        self.logger = Logger(__name__).get_logger()

    def add_task(self, task):
        """Add a task to the scheduler."""
        if isinstance(task, BaseTask):
            self.tasks.append(task)
            self.logger.info(f"Task '{task.name}' added to scheduler.")
        else:
            self.logger.error("Attempted to add non-BaseTask to scheduler.")

    def run_pending_tasks(self):
        """Run tasks that are due."""
        for task in self.tasks:
            if task.should_run():
                task.execute()

    def start(self, check_interval=60):
        """Start the scheduler, checking for tasks to run every `check_interval` seconds."""
        self.logger.info("Scheduler started.")
        try:
            while True:
                self.run_pending_tasks()
                time.sleep(check_interval)  # Sleep for `check_interval` seconds before checking again
        except KeyboardInterrupt:
            self.logger.info("Scheduler stopped.")

    def start_in_background(self, check_interval=60):
        """Start the scheduler in a separate thread."""
        scheduler_thread = threading.Thread(target=self.start, args=(check_interval,), daemon=True)
        scheduler_thread.start()
        self.logger.info("Scheduler started in background thread.")