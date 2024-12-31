import os

from utils.DotDict import DotDict
from utils.GDriveServiceUtils import GdriveServiceUtils
from utils.GenericUtils import GenericUtil
from utils.GmailServiceUtils import GmailServiceUtils
from flask import g
from utils.DateTimeUtil import DateTimeUtil

from flask_sqlalchemy import SQLAlchemy
from logging import Logger

from sqlalchemy import create_engine
from sqlalchemy.orm import sessionmaker, scoped_session


class BaseService:
    gmailService: GmailServiceUtils
    genericUtil: GenericUtil
    db: SQLAlchemy
    logger: Logger

    @property
    def db(self):
        """Retrieve the database session from the Flask global `g`."""
        if g.get('db') is None:
            DATABASE_URL = os.getenv('DATABASE_URL')

            engine = create_engine(DATABASE_URL)
            db_session = scoped_session(sessionmaker(autocommit=False,
                                                     autoflush=False,
                                                     bind=engine))
            return DotDict({'session': db_session})
        return g.db

    def __init__(self):
        self.gmailService = GmailServiceUtils()
        self.driveService = GdriveServiceUtils()
        self.genericUtil = GenericUtil()
        self.dateTimeUtil = DateTimeUtil()
