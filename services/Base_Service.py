from utils.GDriveServiceUtils import GdriveServiceUtils
from utils.GenericUtils import GenericUtil
from utils.GmailServiceUtils import GmailServiceUtils
from flask import g
from utils.DateTimeUtil import DateTimeUtil

from flask_sqlalchemy import SQLAlchemy
from logging import Logger


class BaseService:
    gmailService: GmailServiceUtils
    genericUtil: GenericUtil
    db: SQLAlchemy
    logger: Logger

    @property
    def db(self):
        """Retrieve the database session from the Flask global `g`."""
        return g.db

    def __init__(self):
        self.gmailService = GmailServiceUtils()
        self.driveService = GdriveServiceUtils()
        self.genericUtil = GenericUtil()
        self.dateTimeUtil = DateTimeUtil()
