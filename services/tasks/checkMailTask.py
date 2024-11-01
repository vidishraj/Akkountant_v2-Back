from services.tasks.baseTask import BaseTask
from utils.logger import Logger
from utils.GoogleServiceSingleton import GoogleServiceSingleton


class CheckMailTask(BaseTask):

    def __init__(self):
        super().__init__(name="Mail Check", interval_hours=0.5)
        self.logger = Logger(__name__).get_logger()
        self.googleService = GoogleServiceSingleton()

    def run(self):
        self.logger.info("-----Running mail check----")
        return
