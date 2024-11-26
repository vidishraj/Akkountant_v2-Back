"""
This file holds the Base class to manage Mutual funds, stocks and NPS data.

"""
from abc import abstractmethod

from sqlalchemy import func, case
from sqlalchemy.exc import SQLAlchemyError
from sqlalchemy.orm import joinedload

from models import PurchasedSecurities, SoldSecurities
from services.JsonDownloadService import JSONDownloadService
from utils.DateTimeUtil import DateTimeUtil
from utils.GenericUtils import GenericUtil
from flask_sqlalchemy import SQLAlchemy, session
from logging import Logger
from flask import g

import os


class Base_MSN:
    baseAPIURL: str
    db: session.Session
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

    def fetchActive(self, investment_type: str, user_id: int):
        """
        Fetch active securities of a specific type and format them into JSON.

        :param investment_type: Type of the security to filter.
        :param user_id: ID of the user to filter.
        :return: List of dictionaries (JSON-like structure).
        """
        try:
            active_securities = self.db.session.query(PurchasedSecurities).filter(
                PurchasedSecurities.buyQuant > 0,
                PurchasedSecurities.securityType == investment_type,
                PurchasedSecurities.userID == user_id  # Filter by userID
            ).all()

            result_json = [
                {
                    "buyID": sec.buyID,
                    "buyCode": sec.securityCode,
                    "buyQuant": sec.buyQuant,
                    "schemeCode": sec.securityType,
                    "serviceType": investment_type,
                    "date": sec.date.strftime("%Y-%m-%d"),
                }
                for sec in active_securities
            ]
            return result_json

        except SQLAlchemyError as e:
            print(f"Database error occurred: {e}")
            return []

    def getActiveMoneyInvested(self, service_type, user_id):
        """
        Get the total money invested in active securities based on service type.

        :param service_type: The service type to filter by.
        :param user_id: The userID to filter by.
        :return: Active money invested as a float.
        """
        try:
            active_invested = self.db.session.query(
                func.sum(
                    case(
                        [
                            (PurchasedSecurities.buyQuant > func.coalesce(func.sum(SoldSecurities.sellQuant), 0),
                             (PurchasedSecurities.buyQuant - func.coalesce(func.sum(SoldSecurities.sellQuant),
                                                                           0)) * PurchasedSecurities.buyPrice)
                        ],
                        else_=0
                    )
                )
            ).join(
                SoldSecurities, SoldSecurities.buyID == PurchasedSecurities.buyID, isouter=True
            ).filter(
                PurchasedSecurities.buyQuant > 0,
                PurchasedSecurities.serviceType == service_type,
                PurchasedSecurities.userID == user_id  # Filter by userID
            ).scalar()

            return float(active_invested) if active_invested else 0.0

        except SQLAlchemyError as e:
            print(f"Database error occurred: {e}")
            return 0.0

    def getTotalMoneyInvested(self, service_type, user_id):
        """
        Get the total money invested in all securities based on service type.

        :param service_type: The service type to filter by.
        :param user_id: The userID to filter by.
        :return: Total money invested as a float.
        """
        try:
            total_invested = self.db.session.query(
                func.sum(PurchasedSecurities.buyPrice * PurchasedSecurities.buyQuant)
            ).filter(
                PurchasedSecurities.serviceType == service_type,
                PurchasedSecurities.userID == user_id  # Filter by userID
            ).scalar()

            return float(total_invested) if total_invested else 0.0

        except SQLAlchemyError as e:
            print(f"Database error occurred: {e}")
            return 0.0

    def getTotalProfit(self, service_type, user_id):
        """
        Calculate the total profit from all securities based on service type.

        :param service_type: The service type to filter by.
        :param user_id: The userID to filter by.
        :return: Total profit from all securities as a float.
        """
        try:
            total_profit = self.db.session.query(
                func.sum(SoldSecurities.profit)
            ).join(
                PurchasedSecurities, SoldSecurities.buyID == PurchasedSecurities.buyID
            ).filter(
                PurchasedSecurities.serviceType == service_type,
                PurchasedSecurities.userID == user_id  # Filter by userID
            ).scalar()

            return float(total_profit) if total_profit else 0.0

        except SQLAlchemyError as e:
            print(f"Database error occurred: {e}")
            return 0.0

    def getActiveProfit(self, service_type, user_id):
        """
        Calculate the profit of active securities based on service type.

        :param service_type: The service type to filter by.
        :param user_id: The userID to filter by.
        :return: Total profit from active securities as a float.
        """
        try:
            active_profit = self.db.session.query(
                func.sum(
                    case(
                        [
                            (PurchasedSecurities.buyQuant > func.coalesce(func.sum(SoldSecurities.sellQuant), 0),
                             (PurchasedSecurities.buyQuant - func.coalesce(func.sum(SoldSecurities.sellQuant),
                                                                           0)) * SoldSecurities.profit)
                        ],
                        else_=0
                    )
                )
            ).join(
                SoldSecurities, SoldSecurities.buyID == PurchasedSecurities.buyID, isouter=True
            ).filter(
                PurchasedSecurities.buyQuant > 0,
                PurchasedSecurities.serviceType == service_type,
                PurchasedSecurities.userID == user_id  # Filter by userID
            ).scalar()

            return float(active_profit) if active_profit else 0.0

        except SQLAlchemyError as e:
            print(f"Database error occurred: {e}")
            return 0.0

    def getInvestmentHistory(self, service_type=None, user_id=None):
        """
        Fetch investment history with nested sold securities.
        :return: List of dictionaries (JSON-like structure).
        """
        try:
            investment_history = self.db.session.query(PurchasedSecurities).options(
                joinedload(PurchasedSecurities.sold_securities)
            ).filter(
                PurchasedSecurities.userID == user_id if user_id else True  # Filter by userID if provided
            ).all()

            if service_type:
                investment_history = [purchase for purchase in investment_history if
                                      purchase.securityType == service_type]

            result_json = [
                {
                    "buyID": purchase.buyID,
                    "buyCode": purchase.securityCode,
                    "buyQuant": purchase.buyQuant,
                    "schemeCode": purchase.securityType,
                    "date": purchase.date.strftime("%Y-%m-%d"),
                    "soldSecurities": [
                        {
                            "sellID": sold.sellID,
                            "sellQuant": sold.sellQuant,
                            "sellPrice": float(sold.sellPrice),
                            "profit": float(sold.profit) if sold.profit else None,
                            "date": sold.date.strftime("%Y-%m-%d"),
                        }
                        for sold in purchase.sold_securities
                    ]
                }
                for purchase in investment_history
            ]

            return result_json

        except SQLAlchemyError as e:
            print(f"Database error occurred: {e}")
            return []

    def findIdIfSecurityBought(self, userId, securityCode):
        result = self.db.session.query(PurchasedSecurities).filter(
            PurchasedSecurities.userID == userId,
            PurchasedSecurities.securityCode == securityCode
        ).first()

        # Check if a result is found
        if result:
            return result
        else:
            return None

    def updatePriceAndQuant(self, buyId, newPrice, newQuant):
        try:
            # Fetch the record with the given buyID
            security = self.db.session.query(PurchasedSecurities).filter(PurchasedSecurities.buyID == buyId).first()

            # Update fields with new values
            security.buyPrice = newPrice
            security.buyQuant = newQuant

            # Commit the changes to the database
            self.db.session.commit()
            self.logger.info(f"Record with buyID {buyId} successfully updated.")
        except Exception as e:
            self.db.session.rollback()  # Roll back in case of error
            self.logger.error(f"An error occurred while updating row {e}")
