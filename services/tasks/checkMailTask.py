from services.tasks.baseTask import BaseTask
from utils.DateTimeUtil import DateTimeUtil
from utils.logger import Logger
from utils.GoogleServiceSingleton import GoogleServiceSingleton


class CheckMailTask(BaseTask):
    _instance = None

    def __new__(cls, *args, **kwargs):
        if not cls._instance:
            cls._instance = super(CheckMailTask, cls).__new__(cls)
        return cls._instance

    def __init__(self, title, priority):
        if not hasattr(self, 'initialized'):  # Prevent multiple initializations
            super().__init__(title, priority)
            self.logger = Logger(__name__).get_logger()
            # 4 hours
            self.interval = 300

    def run(self):
        try:
            if not self.user_id:
                self.logger.error("User ID not found. Stopping task")
                return "No userid", "Failed", self.interval
            # Just scanning the entire fcking month
            read, conflicts = self.transactionService.readTransactionFromMail(None, None, self.user_id)
            return f"{read} emails read. {conflicts} conflicts", "Completed", self.interval
        except Exception as ex:
            return ex.__str__(), "Failed", self.interval