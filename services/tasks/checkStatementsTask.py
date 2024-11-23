from services.tasks.baseTask import BaseTask
from utils.logger import Logger
from utils.GoogleServiceSingleton import GoogleServiceSingleton
from flask import g
from flask_sqlalchemy import SQLAlchemy


class CheckStatementTask(BaseTask):
    user: str
    db: SQLAlchemy

    def __init__(self, userID: str):
        super().__init__(name="Statement Check", interval_hours=0.5)
        self.logger = Logger(__name__).get_logger()
        self.googleService = GoogleServiceSingleton()
        self.user = userID
        self.db = g['db']

    def run(self, bankType, token):
        """
        :param bankType: Bank type refers to a type from BanksEnum
        :param token: The token to download file from gmail and save in temp
        :return: Nothing as of now.

        The idea is that we will use the token and userId to download the relevant file
        from gmail and save it in temp
        """
        self.logger.info("-----Running statement check----")

