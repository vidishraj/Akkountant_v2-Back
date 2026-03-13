from services.tasks.baseTask import BaseTask
from utils.logger import Logger


class CheckStatementTask(BaseTask):
    _instance = None

    def __new__(cls, *args, **kwargs):
        if not cls._instance:
            cls._instance = super(CheckStatementTask, cls).__new__(cls)
        return cls._instance

    def __init__(self, title, priority):
        if not hasattr(self, 'initialized'):  # Prevent multiple initializations
            super().__init__(title, priority)
            self.logger = Logger(__name__).get_logger()
            self.interval = 1440

    def run(self):
        """Deprecated — statement processing is now handled by CheckMailUnifiedTask via MailProcessorService."""
        self.logger.info("CheckStatementTask is deprecated. Statement processing is handled by the unified mail pipeline.")
        return "Deprecated — use unified mail pipeline (CheckMailUnifiedTask)", "Completed", self.interval
