from sqlalchemy.exc import IntegrityError

from enums.PatternEnum import PatternEnum
from enums.TransactionTypeEnum import TransactionTypeEnum
from models import User, UserToken, Transactions, TransactionForReview
from utils.DateTimeUtil import DateTimeUtil
from services.Base_Service import BaseService
from utils.logger import Logger


class TransactionService(BaseService):

    def __init__(self):
        super().__init__()
        self.logger = Logger(__name__).get_logger()

    def readTransactionFromMail(self, dateTo, dateFrom, userID):
        if dateTo is None or dateFrom is None:
            # If we are not reading for a specific range, read for current month
            dateFrom, dateTo = DateTimeUtil.currentMonthDatesForEmail()

        # Fetch the banks the user has opted for
        user = self.db.session.query(User).filter_by(userID=userID).first()
        optedBanks = user.optedBanks.split(',')

        # Fetch the gmail token of the user
        userToken = self.db.session.query(UserToken).filter_by(user_id=userID).first()
        token = {
            'token': userToken.access_token,
            'refresh_token': userToken.refresh_token,
            'client_id': userToken.client_id,
            'client_secret': userToken.client_secret,
        }
        integrityErrors = 0
        totalMails = 0
        for bank in optedBanks:
            patternString = getattr(PatternEnum, bank)
            # Fetch the emails in the date range
            mails = self.gmailService.findEmailInIntervalForPattern(userID, token, patternString.value,
                                                                    dateFrom, dateTo)
            # Process the items to get them all in the required format
            cleanedMails, conflicts = self.genericUtil.extractDetailsFromEmail(mails, bank)
            totalMails += len(cleanedMails)
            # Insert the processed transactions in the database
            for mail in cleanedMails:
                transaction = Transactions(
                    referenceID=mail[0],
                    date=mail[1],
                    details=mail[2],
                    amount=mail[3],
                    tag="",
                    fileID=None,
                    bank=bank,
                    source=TransactionTypeEnum.Email.value,
                    user=user.userID
                )
                try:
                    self.db.session.add(transaction)
                    self.db.session.commit()
                except IntegrityError as e:
                    self.logger.warning(f"Duplicate entry error occurred: {e.__cause__}")
                    self.db.session.rollback()
                    integrityErrors += 1

            for conflict in conflicts:
                conflict = TransactionForReview(
                    user=user.userID,
                    conflict=conflict
                )
                self.db.session.add(conflict)
            try:
                self.db.session.commit()
            except IntegrityError as e:
                self.logger.warning(f"Duplicate entry error occurred: {e.__cause__}")
                self.db.session.rollback()
                integrityErrors += 1
        self.logger.info(f"Total integrity errors: {integrityErrors}. Total mails: {totalMails}")
        return
