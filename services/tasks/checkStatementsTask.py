from services.tasks.baseTask import BaseTask
from utils.DateTimeUtil import DateTimeUtil
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
            # Once per day (1440 minutes)
            self.interval = 1440

    def run(self):
        try:
            if not self.user_id:
                self.logger.error("User ID not found. Stopping task")
                return "No userid", "Failed", self.interval
            # Match exactly what triggerStatementCheck endpoint does
            self.logger.info(f"Reading statements for user {self.user_id} using claude algorithm")
            successCount, errorCount = self.transactionService.readStatementsFromMail(
                dateTo=None, 
                dateFrom=None, 
                userID=self.user_id, 
                bank=None, 
                algorithm='claude'
            )
            return f"{successCount} transactions read in statements (Claude PDF analysis + intelligent detection). {errorCount} conflicts", "Completed", self.interval
        except Exception as ex:
            return ex.__str__(), "Failed", self.interval