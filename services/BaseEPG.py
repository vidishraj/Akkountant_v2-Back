"""class that will act as the base class for all pf, ppf and gold based operations"""
import os
from logging import Logger as LG

from flask import g
from flask_sqlalchemy import session
from flask_sqlalchemy.session import Session
from sqlalchemy import and_, create_engine
from sqlalchemy.exc import SQLAlchemyError
from sqlalchemy.orm import scoped_session, sessionmaker

from enums.EPGEnum import EPGEnum
from models import DepositSecurities
from services.JsonDownloadService import JSONDownloadService
from utils.DateTimeUtil import DateTimeUtil
from utils.DotDict import DotDict
from utils.GenericUtils import GenericUtil
from utils.logger import Logger


class Base_EPG:
    baseAPIURL: str
    db: session.Session
    logger: LG
    genericUtil: GenericUtil
    baseDirectory: str = f'{os.getcwd()}/services/'
    JsonDownloadService: JSONDownloadService

    @property
    def db(self):
        """Retrieve the database session from the Flask global `g`.""""""Retrieve the database session from the Flask global `g`."""
        if g.get('db') is None:
            DATABASE_URL = os.getenv('DATABASE_URL')

            engine = create_engine(DATABASE_URL)
            db_session = scoped_session(sessionmaker(autocommit=False,
                                                     autoflush=False,
                                                     bind=engine))
            return DotDict({'session': db_session})
        return g.db

    def __init__(self):
        """
        Each will be fetched from a base API url
        """
        if not hasattr(self, 'initialized'):  # Prevent multiple initializations
            self.genericUtil = GenericUtil()
            self.dateTimeUtil = DateTimeUtil()
            self.JsonDownloadService = JSONDownloadService(save_directory=f"{os.getcwd()}/services/assets/")
            self.logger = Logger(__name__).get_logger()

    def validate_security_type(self, security_type: str):
        """
        Validate the security type against the defined EPGEnum.

        :param security_type: The security type to validate.
        :return: The corresponding EPGEnum value if valid.
        :raises ValueError: If the security type is invalid.
        """
        try:
            return EPGEnum(security_type)
        except ValueError:
            self.logger.error(f"Invalid security type '{security_type}'.")
            raise ValueError(f"Invalid security type '{security_type}'. Must be one of {[e.value for e in EPGEnum]}.")

    def insertDepositFinal(self, deposit):
        """
        Insert a new deposit into the database. Ensure no duplicate deposits exist for the same date
        for 'PF' service type.

        :param deposit: Instance of DepositSecurities containing the deposit details.
        :return: None
        """
        try:
            # Validate the security type
            security_type = self.validate_security_type(deposit.securityType)

            # Check for PF service type
            if security_type == EPGEnum.PF:
                # Query to check for duplicate deposits on the same date
                existing_deposit = self.db.session.query(DepositSecurities).filter_by(
                    date=deposit.date,
                    userID=deposit.userID,
                    securityType=EPGEnum.PF.value
                ).first()

                if existing_deposit:
                    self.logger.warning(
                        f"Duplicate PF deposit found for date {deposit.date} and userID {deposit.userID}. Skipping "
                        f"insertion.")
                    return {'error': "Duplicate PF data"}

            # Insert the deposit record
            self.db.session.add(deposit)
            self.db.session.commit()
            self.logger.info(f"Deposit inserted successfully for user {deposit.userID} on {deposit.date}.")
            return {'message': 'success'}
        except Exception as e:
            # Rollback in case of any exception
            self.logger.error(f"Error while inserting deposit: {e}")
            return {'error': 'Error occurred'}

    def deleteDeposit(self, buyID: str):
        """
        Delete a deposit record from the database based on the buyID.

        :param buyID: The unique ID of the deposit to delete.
        :return: None
        :raises ValueError: If no deposit with the given buyID is found.
        """
        try:
            # Query the database for the deposit with the given buyID
            deposit = self.db.query(DepositSecurities).filter_by(buyID=buyID).first()

            # Check if the deposit exists
            if not deposit:
                self.logger.error(f"No deposit found with buyID '{buyID}'.")
                raise ValueError(f"No deposit found with buyID '{buyID}'.")

            # Delete the deposit
            self.db.delete(deposit)
            self.db.commit()
            self.logger.info(f"Deposit with buyID '{buyID}' deleted successfully.")

        except Exception as e:
            # Rollback in case of any exception
            self.db.rollback()
            self.logger.error(f"Error while deleting deposit with buyID '{buyID}': {e}")
            raise

    def get_securities(self, user_id: str, security_type: str):
        """
        Retrieves securities from the DepositSecurities table based on deposit date, userID, and security type.

        Args:
            user_id (str): The ID of the user.
            security_type (str): The type of security.

        Returns:
            List[DepositSecurities]: List of DepositSecurities objects matching the criteria.
        """
        try:
            securities = (
                self.db.session.query(DepositSecurities)
                .filter(
                    and_(
                        DepositSecurities.userID == user_id,
                        DepositSecurities.securityType == security_type
                    )
                )
                .order_by(DepositSecurities.date.asc())
                .all()
            )
            return securities
        except Exception as e:
            self.logger.error(f"Error while fetching securities: {e}")
            return []

    def delete_deposit_securities_by_user(self, user_id: str):
        """
        Deletes records from the DepositSecurities table for a given userID.

        Args:user_id (str): The ID of the user whose records are to be deleted.

        Returns:
            dict: Status message indicating success or failure.
        """
        try:
            # Fetch records for the given userID
            records_to_delete = self.db.session.query(DepositSecurities).filter(DepositSecurities.userID == user_id).all()

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
