"""
This file holds the Base class to manage Mutual funds, stocks and NPS data.

"""
from abc import abstractmethod

from werkzeug.routing import ValidationError

from services.JsonDownloadService import JSONDownloadService
from utils.DateTimeUtil import DateTimeUtil
from utils.GenericUtils import GenericUtil
from flask_sqlalchemy import SQLAlchemy
from logging import Logger
from flask import g

import os
import datetime
import json
import requests


class Base_MSN:
    baseAPIURL: str
    db: SQLAlchemy
    logger: Logger
    genericUtil: GenericUtil
    baseDirectory: str = f'{os.getcwd()}/services/'
    JsonDownloadService: JSONDownloadService

    @property
    def db(self):
        """Retrieve the database session from the Flask global `g`."""
        return g.db

    def __init__(self):
        """
        Each will be fetched from a base API url
        """
        self.genericUtil = GenericUtil()
        self.dateTimeUtil = DateTimeUtil()
        self.JsonDownloadService = JSONDownloadService(save_directory=f"{os.getcwd()}/services/assets/")

    @abstractmethod
    def fetchAllSecurities(self):
        """

        :return: List of securities without any pagiantion
        """
        return self.JsonDownloadService.getStockList()

    @abstractmethod
    def findSecurity(self, securityCode):
        """
        :return: Security without any pagiantion
        """
        pass

    @abstractmethod
    def readFromStatement(self, file_path: str, userId):
        """
        The file path is going to be the name of the file along with the
        temp directory path
        :return: Read security statement.
        """
        pass

    @abstractmethod
    def buySecurity(self, security_data, userId):
        """

        :param security_data: Code of security to buy
        :param userId: User_ID to add info for
        :return: Success or throws error
        """
        pass

    @abstractmethod
    def sellSecurity(self, securityCode, userId):
        """

        :param securityCode: Code of security to sell
        :param userId: User_ID to add info for
        :return: Success or throws error
        """
        pass
