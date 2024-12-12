from abc import abstractmethod
from concurrent.futures import ThreadPoolExecutor
from decimal import Decimal, ROUND_DOWN

from sqlalchemy import func
from sqlalchemy.exc import SQLAlchemyError
from sqlalchemy.orm import joinedload
from enums.MsnEnum import MSNENUM
from models import PurchasedSecurities, SoldSecurities
from models.securityTransactions import SecurityTransactions
from services.JsonDownloadService import JSONDownloadService
from utils.DateTimeUtil import DateTimeUtil
from utils.GenericUtils import GenericUtil
from flask_sqlalchemy import session
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
                    "buyPrice": sec.buyPrice,
                    "schemeCode": sec.securityType,
                    "serviceType": investment_type,
                    "date": sec.date.strftime("%Y-%m-%d"),
                }
                for sec in active_securities
            ]
            return result_json

        except SQLAlchemyError as e:
            self.logger.error(f"Database error occurred: {e}")
            return []

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
            self.logger.error(f"Database error occurred: {e}")
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
            self.logger.error(f"Database error occurred: {e}")
            return 0.0

    def getActiveMoneyInvested(self, service_type, user_id):
        """
        Get the total money invested in active securities based on service type.

        :param service_type: The service type to filter by.
        :param user_id: The userID to filter by.
        :return: Active money invested as a float.
        """
        try:
            """
                    Calculates the total active invested value for a specific user.

                    :param user_id: The ID of the user whose investment is calculated.
                    :param security_type: Optional filter for security type (e.g., 'Stocks').
                    :return: Total active invested value.
                    """
            query = self.db.session.query(
                func.sum(PurchasedSecurities.buyQuant * PurchasedSecurities.buyPrice).label("total_invested")
            ).filter(
                PurchasedSecurities.buyQuant > 0,  # Only consider active investments
                PurchasedSecurities.userID == user_id  # Filter by user
            )

            # Apply optional security type filter
            if service_type:
                query = query.filter(PurchasedSecurities.securityType == service_type)

            # Execute and fetch the scalar result
            total_invested = query.scalar()

            return float(total_invested) if total_invested else 0.0

        except SQLAlchemyError as e:
            self.logger.error(f"Database error occurred: {e}")
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
            self.logger.error(f"Database error occurred: {e}")
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

    def getSecurityCount(self, userId, investment_type):
        active_securities = self.db.session.query(PurchasedSecurities).filter(
            PurchasedSecurities.buyQuant > 0,
            PurchasedSecurities.securityType == investment_type,
            PurchasedSecurities.userID == userId  # Filter by userID
        ).all()

        # Check if a result is found
        if active_securities:
            return len(active_securities)
        else:
            return 0

    def findIfSameSecurityTransactionExists(self, userId, buyID):
        result = self.db.session.query(PurchasedSecurities).filter(
            PurchasedSecurities.buyID == buyID,
            PurchasedSecurities.userID == userId
        ).first()

        # Check if a result is found
        if result:
            return result
        else:
            return None

    def updatePriceAndQuant(self, newPrice, newQuant, buyId):
        try:
            # Fetch the record with the given buyID
            security = self.db.session.query(PurchasedSecurities).filter(PurchasedSecurities.buyID == buyId).first()

            # Update fields with new values
            security.buyPrice = newPrice
            security.buyQuant = newQuant

            # Commit the changes to the database
            self.logger.info(f"Record with buyID {buyId} successfully updated.")
        except Exception as e:
            self.logger.error(f"An error occurred while updating row {e}")

    def calculateStockRates(self, data_list):
        with ThreadPoolExecutor() as executor:
            # Submit tasks to threads and collect Future objects
            # @TODO Manage changed symbols and edge case for fucking SUZLON-BC
            futures = [
                executor.submit(self.findSecurity, data['buyCode'] if data['buyCode'] != "SUZLON-BE" else "SUZLON") for
                data in data_list]

            # Use results from completed tasks to call another method
            rateDictionary = {}
            for future in futures:
                result = future.result()
                # Waits for the thread to complete and gets the return value
                rate_card = self.genericUtil.fetchStockRates(result)
                rateDictionary[rate_card['symbol']] = rate_card
            return rateDictionary

    def calculateProfitAndCurrentValue(self, investment_type: str, user_id: int):
        """
        Calculate the profit and current value for each stock.

        :param investment_type: Type of the security to filter (e.g., Stocks, Mutual Funds, NPS).
        :param user_id: ID of the user to filter.
        :return: List of dictionaries with profit and current value for each stock.
        """
        try:
            # Fetch active securities
            active_securities = self.fetchActive(investment_type, user_id)

            if not active_securities:
                return []

            results = {}
            if investment_type == MSNENUM.Stocks.value:
                rate_data = self.calculateStockRates(active_securities)
                for sec in active_securities:
                    # Calculate current value and profit
                    current_value = Decimal(sec["buyQuant"]) * Decimal(rate_data["lastPrice"]).quantize(Decimal('0.01'),
                                                                                                        rounding=ROUND_DOWN)
                    buy_value = Decimal(sec["buyQuant"]) * Decimal(sec["buyPrice"]).quantize(Decimal('0.01'),
                                                                                             rounding=ROUND_DOWN)
                    profit = (current_value - buy_value).quantize(Decimal('0.01'), rounding=ROUND_DOWN)

                    # Append results
                    results[sec["buyID"]] = {
                        "currentValue": float(current_value),
                        "profit": float(profit),
                    }

            for sec in active_securities:
                # Fetch the current rate for the security
                rate_data = None
                if investment_type == MSNENUM.NPS.value:
                    rates = self.JsonDownloadService.getNPSRate(sec['buyCode'])
                    rate_data = {'lastPrice': rates['nav']}
                if "error" in rate_data:
                    self.logger.error(f"Error fetching rate data for {sec['buyCode']}: {rate_data['error']}")
                    continue

                # Calculate current value and profit
                current_value = Decimal(sec["buyQuant"]) * Decimal(rate_data["lastPrice"]).quantize(Decimal('0.01'),
                                                                                                    rounding=ROUND_DOWN)
                buy_value = Decimal(sec["buyQuant"]) * Decimal(sec["buyPrice"]).quantize(Decimal('0.01'),
                                                                                         rounding=ROUND_DOWN)
                profit = (current_value - buy_value).quantize(Decimal('0.01'), rounding=ROUND_DOWN)

                # Append results
                results[sec["buyID"]] = {
                    "currentValue": float(current_value),
                    "profit": float(profit),
                }

            return results

        except Exception as e:
            self.logger.error(f"An error occurred: {e}")
            return []

    def insert_security_transaction(self, transaction_data: dict):
        """
        Inserts a new record into the SecurityTransactions table.
        Args:
            transaction_data (dict): Dictionary containing transaction details:
                {
                    "date": str (format: '%Y-%m-%d'),
                    "quant": int,
                    "price": Decimal or float,
                    "transactionType": str ("buy" or "sell"),
                    "userID": str,
                    "securityType": str
                }
        Returns:
            SecurityTransactions: The inserted SecurityTransactions object.
        """
        try:
            # Validate input data
            required_keys = {"date", "quant", "price", "transactionType", "userID", "securityType"}
            if not required_keys.issubset(transaction_data):
                raise ValueError(f"Missing required fields: {required_keys - transaction_data.keys()}")

            # Parse and create SecurityTransactions instance
            new_transaction = SecurityTransactions(
                date=transaction_data["date"],
                quant=transaction_data["quant"],
                price=Decimal(transaction_data["price"]),
                transactionType=transaction_data["transactionType"],
                userID=transaction_data["userID"],
                securityType=transaction_data["securityType"]
            )

            # Add and commit the new transaction
            self.db.session.add(new_transaction)\

            if transaction_data['securityType'] != MSNENUM.Stocks.value:
                # Only commit when outside stocks transaction loop
                self.db.session.commit()

            return new_transaction
        except Exception as e:
            self.db.session.rollback()  # Rollback in case of error
            self.logger.error(f"Error inserting security transaction: {e}")
            return None

    def fetchTransactionsForUserAndService(self, security_type: str, user_id: str, ):
        try:
            transactions = (
                self.db.session.query(SecurityTransactions)
                .filter(
                    SecurityTransactions.userID == user_id,
                    SecurityTransactions.securityType == security_type
                )
                .all()
            )
            transactionDict = []
            for transaction in transactions:
                transactionDict.append( {
                    'id': transaction.transactionId,
                    'date': transaction.date,
                    'price': transaction.price,
                    'quant': transaction.quant,
                    'transactionType': transaction.transactionType,
                })
            return transactionDict
        except Exception as ex:
            # Handle/log exception appropriately
            self.logger.error(f"Error fetching transactions for user {user_id} and security type {security_type}: {ex}")
            return []

    def delete_purchased_securities_by_user(self, user_id: str):
        """
        Deletes records from the PurchasedSecurities table for a given userID.

        Args:
            user_id (str): The ID of the user whose records are to be deleted.

        Returns:
            dict: Status message indicating success or failure.
        """
        try:
            # Fetch records for the given userID
            records_to_delete = self.db.session.query(PurchasedSecurities).filter(PurchasedSecurities.userID == user_id).all()

            if not records_to_delete:
                return {"message": f"No records found for userID: {user_id}"}

            # Delete the records
            for record in records_to_delete:
                self.db.session.delete(record)

            # Commit the transaction
            self.db.session.commit()
            return {"message": f"Successfully deleted all records for userID: {user_id}"}

        except SQLAlchemyError as e:
            # Rollback in case of an error
            self.db.session.rollback()
            return {"error": f"Failed to delete records for userID: {user_id}. Error: {str(e)}"}