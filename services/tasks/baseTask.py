from datetime import datetime, timedelta
from abc import ABC, abstractmethod
from utils.logger import Logger


class BaseTask(ABC):
    """Abstract base class for a scheduled task."""

    def __init__(self, name, interval_hours):
        self.name = name
        self.interval = timedelta(hours=interval_hours)
        self.last_run = None  # Stores the last time the task was run
        self.logger = Logger(__name__).get_logger()

    @abstractmethod
    def run(self):
        """Method containing the task logic. Must be overridden by subclasses."""
        pass

    def should_run(self):
        """Check if the task should be run based on the last run time."""
        if self.last_run is None:
            return True
        return datetime.now() - self.last_run >= self.interval

    def execute(self):
        """Run the task and update the last run time."""
        self.logger.info(f"Running task: {self.name}")
        self.run()
        self.last_run = datetime.now()
        self.logger.info(f"Task '{self.name}' completed.")
