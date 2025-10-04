from services.tasks.baseTask import BaseTask
from utils.logger import Logger


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
            # 25 times per day (every ~58 minutes)
            self.interval = 58

    def run(self):
        try:
            if not self.user_id:
                self.logger.error("User ID not found. Stopping task")
                return "No userid", "Failed", self.interval
            # Match exactly what triggerEmailCheck endpoint does
            self.logger.info(f"Reading email for user {self.user_id} using claude algorithm")
            successCount, errorCount = self.transactionService.readTransactionFromMail(
                dateTo=None, 
                dateFrom=None, 
                userID=self.user_id, 
                algorithm='claude'
            )
            return f"{successCount} emails processed. {errorCount} conflicts (intelligent detection + Claude Code primary)", "Completed", self.interval
        except Exception as ex:
            return ex.__str__(), "Failed", self.interval