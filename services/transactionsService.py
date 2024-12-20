import os

from flask_sqlalchemy.session import Session
from sqlalchemy.exc import IntegrityError, SQLAlchemyError
from sqlalchemy import func, case

from enums.BanksEnum import BankEnums
from enums.ServiceTypeEnum import ServiceTypeEnum
from enums.StatementPatternEnum import StatementPatternEnum
from enums.PatternEnum import PatternEnum
from enums.TransactionTypeEnum import TransactionTypeEnum
from models import User, UserToken, Transactions, TransactionForReview, StatementPasswords, FileDetails
from services.parsers.HDFC_Credit import HDFCMilleniaParse
from services.parsers.HDFC_Debit import HDFCDebitParser
from services.parsers.ICICI_Amazon_Credit import ICICICreditCardStatementParser
from services.parsers.YES_Credit import YESBankCreditParser
from services.parsers.YES_Debit import YESBankDebitParser
from services.Base_Service import BaseService
from utils.logger import Logger


class TransactionService(BaseService):

    def __init__(self):
        super().__init__()
        self.logger = Logger(__name__).get_logger()

    def fetchTransactions(self, page: int, filters: dict, page_size: int = 100):
        query = self.db.session.query(Transactions)

        # Apply filters if they are provided
        if filters:
            if date_range := filters.get('dateRange'):
                date_from = date_range.get('dateFrom')
                date_to = date_range.get('dateTo')
                if date_from and date_to:
                    query = query.filter(Transactions.date.between(date_from, date_to))
            if details := filters.get('details'):
                query = query.filter(Transactions.details.ilike(f"%{details}%"))
            if tag := filters.get('tags'):
                query = query.filter(Transactions.tag.ilike(f"%{tag}%"))
            if bank := filters.get('bank'):
                query = query.filter(Transactions.bank == bank)
            if source := filters.get('source'):
                query = query.filter(Transactions.source == source)

        # Get the total count before pagination by creating a new count query
        total_count_query = self.db.session.query(func.count(Transactions.referenceID))

        # Reapply the same filters for the count query
        if filters:
            if date_range := filters.get('dateRange'):
                date_from = date_range.get('dateFrom')
                date_to = date_range.get('dateTo')
                if date_from and date_to:
                    total_count_query = total_count_query.filter(Transactions.date.between(date_from, date_to))
            if details := filters.get('details'):
                total_count_query = total_count_query.filter(Transactions.details.ilike(f"%{details}%"))
            if tag := filters.get('tags'):
                total_count_query = total_count_query.filter(Transactions.tag.ilike(f"%{tag}%"))
            if bank := filters.get('bank'):
                total_count_query = total_count_query.filter(Transactions.bank == bank)
            if source := filters.get('source'):
                total_count_query = total_count_query.filter(Transactions.source == source)

        total_count = total_count_query.scalar()

        # Calculate sums for debit and credit amounts
        sum_query = self.db.session.query(
            func.sum(case((Transactions.amount < 0, Transactions.amount), else_=0)).label("credit_sum"),
            func.sum(case((Transactions.amount > 0, Transactions.amount), else_=0)).label("debit_sum"),
        )

        # Reapply the same filters for the sum query
        if filters:
            if date_range := filters.get('dateRange'):
                date_from = date_range.get('dateFrom')
                date_to = date_range.get('dateTo')
                if date_from and date_to:
                    sum_query = sum_query.filter(Transactions.date.between(date_from, date_to))
            if details := filters.get('details'):
                sum_query = sum_query.filter(Transactions.details.ilike(f"%{details}%"))
            if tag := filters.get('tags'):
                sum_query = sum_query.filter(Transactions.tag.ilike(f"%{tag}%"))
            if bank := filters.get('bank'):
                sum_query = sum_query.filter(Transactions.bank == bank)
            if source := filters.get('source'):
                sum_query = sum_query.filter(Transactions.source == source)

        credit_sum, debit_sum = sum_query.first() or (0, 0)

        # Apply sorting if `sorted` is provided
        if sorted := filters.get('sorted'):
            column, order = sorted.get('column'), sorted.get('order', 'asc')
            if column and hasattr(Transactions, column):
                column_attr = getattr(Transactions, column)
                if order == 'desc':
                    query = query.order_by(column_attr.desc())
                else:
                    query = query.order_by(column_attr.asc())

        # Apply pagination to the main query
        paginated_query = query.offset((page - 1) * page_size).limit(filters.get('limit', page_size))

        # Execute and return the results
        paginated_results = paginated_query.all()

        return {
            "count": total_count,
            "results": paginated_results,
            "credit_sum": credit_sum,
            "debit_sum": debit_sum,
            "page": page
        }

    def fetchBanksOptedByUser(self, userID):
        return self.db.session.query(User).filter_by(userID=userID).first().optedBanks.split(',')

    def fetchTransactionDates(self, date_from: str, date_to: str):
        """
        Service to fetch transaction and statement dates within a given date range.
        """
        transaction_query = (
            self.db.session.query(Transactions.date)
            .filter(Transactions.date.between(date_from, date_to))
            .distinct()
        )

        statement_query = (
            self.db.session.query(FileDetails.uploadDate)
            .filter(FileDetails.uploadDate.between(date_from, date_to))
            .distinct()
        )

        transaction_dates = [t[0].strftime("%Y-%m-%d") for t in transaction_query]
        statement_dates = [s[0].strftime("%Y-%m-%d") for s in statement_query]

        return {
            "transaction_dates": transaction_dates,
            "statement_dates": statement_dates,
        }

    def readTransactionFromMail(self, dateTo, dateFrom, userID):
        if dateTo is None or dateFrom is None:
            # If we are not reading for a specific range, read for current month
            dateFrom, dateTo = self.dateTimeUtil.currentMonthDatesForEmail()

        # Fetch the banks the user has opted for
        optedBanks = self.fetchBanksOptedByUser(userID)

        # Fetch the gmail token of the user
        token = self.fetchGmailTokenForUser(userID)
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
            self.insertTransactions(cleanedMails, bank, userID, conflicts, TransactionTypeEnum.Email.value)
        self.logger.info(f"Finished reading mail. Inserted {totalMails} transactions")
        return totalMails, len(conflicts)

    def insertTransactions(self, transactions, bank, userId, conflicts, source, fileId=None):
        integrityErrors = 0
        for transaction in transactions:
            date = self.dateTimeUtil.convert_to_sql_datetime(transaction['date'], bank)
            transaction = Transactions(
                referenceID=transaction['reference'],
                date=date,
                details=transaction['description'],
                amount=transaction['amount'],
                tag="",
                fileID=fileId,
                bank=bank,
                source=source,
                user=userId
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
                user=userId,
                conflict=conflict
            )
            self.db.session.add(conflict)
        try:
            self.db.session.commit()
        except IntegrityError as e:
            self.logger.warning(f"Duplicate entry error occurred while committing: {e.__cause__}")
            self.db.session.rollback()
        return integrityErrors

    @staticmethod
    def getParserInstanceByBank(bank):
        bank = getattr(StatementPatternEnum, bank)

        # Define a mapping of StatementPatternEnum values to parser classes
        parser_mapping = {
            StatementPatternEnum.YES_BANK_DEBIT: YESBankDebitParser,
            StatementPatternEnum.YES_BANK_ACE: YESBankCreditParser,
            StatementPatternEnum.ICICI_AMAZON_PAY: ICICICreditCardStatementParser,
            StatementPatternEnum.HDFC_DEBIT: HDFCDebitParser,
            StatementPatternEnum.Millenia_Credit: HDFCMilleniaParse
        }

        # Get the appropriate parser class from the mapping
        parser_class = parser_mapping.get(bank)

        return parser_class()

    def fetchGmailTokenForUser(self, userID):
        userToken = self.db.session.query(UserToken).filter_by(user_id=userID) \
            .filter_by(service_type=ServiceTypeEnum.Gmail.value).first()
        return {
            'token': userToken.access_token,
            'refresh_token': userToken.refresh_token,
            'client_id': userToken.client_id,
            'client_secret': userToken.client_secret,
        }

    def fetchDriveTokenForUser(self, userID):
        userToken = self.db.session.query(UserToken).filter_by(user_id=userID). \
            filter_by(service_type=ServiceTypeEnum.Gdrive.value).first()
        return {
            'token': userToken.access_token,
            'refresh_token': userToken.refresh_token,
            'client_id': userToken.client_id,
            'client_secret': userToken.client_secret,
        }

    def readStatementsFromMail(self, dateTo, dateFrom, userID, bank):
        """
        We download all the files first. Then individually process and upload them. Current approach is to upload the
        file first. If there is a failure during processing and insertion, then we delete the file from googleDrive.
            /:param dateTo: Date Range Info
            :param dateTo:
            :param dateFrom: Date Range Info
            :param userID: UserID firebase
            :param bank: bank
            :return:
        """
        if dateTo is None or dateFrom is None:
            # If we are not reading for a specific range, read for current month
            dateFrom, dateTo = self.dateTimeUtil.currentMonthDatesForEmail()
        if bank is None:
            # Fetch the banks the user has opted for
            optedBanks = self.fetchBanksOptedByUser(userID)
        else:
            optedBanks = bank.split(',')
        # Fetch the gmail token of the user
        gmailToken = self.fetchGmailTokenForUser(userID)
        # Fetch the drive token of the user
        driveToken = self.fetchDriveTokenForUser(userID)
        totalTransactions = 0
        totalIntegrityErrors = 0
        for bank in optedBanks:
            # Fetch the password for the bank
            self.logger.info(f"Processing bank {bank}")
            password = self.db.session.query(StatementPasswords).filter_by(user=userID).filter_by(bank=bank).first()
            # Download files to temp. A list of file paths will be available.
            filepaths = self.gmailService.downloadFilesInRange(userID, gmailToken, password, bank, dateTo, dateFrom)

            # Get relevant parser
            parserInstance = self.getParserInstanceByBank(bank)
            for path in filepaths:
                self.logger.info(f"Processing file {path}")
                # Parse the statement
                parserInstance.setPath(os.getcwd() + '/tmp/' + path)
                parserInstance.setPassword(password.password_hash)
                transactions = parserInstance.parseFile()
                totalTransactions += len(transactions)
                self.logger.info("Finished reading transactions")
                if len(transactions) > 0:
                    # Get fileName
                    month = self.dateTimeUtil.getMonthYearRange(transactions[0]['date'], transactions[-1]['date'], bank)
                    fileName = f"{bank}_{month}.pdf"
                    # Upload to Drive
                    fileId = self.driveService.uploadFileToDrive(
                        fileName, f"Akkountant/{bank}/", userID, driveToken, os.getcwd() + '/tmp/' + path)
                    self.insertFileDetails(fileId, fileName, len(transactions), bank, userID, path)
                    # Insert transactions
                    try:
                        integrityErrors = self.insertTransactions(transactions, bank, userID, [],
                                                                  TransactionTypeEnum.Statement.value,
                                                                  fileId)
                        totalIntegrityErrors += integrityErrors
                        if integrityErrors == len(transactions):
                            # No transaction were inserted, delete the file
                            self.deleteFileDetails(fileId)
                            self.driveService.deleteFile(fileId, userID, driveToken)
                        elif integrityErrors > 0:
                            self.updateStatementCount(fileId, len(transactions) - integrityErrors)
                    except Exception as ex:
                        self.logger.error(f"Error occurred while inserting transaction. Possibly EOF {ex}")
                        # Delete file from drive if uploaded
                        if fileId is not None:
                            self.driveService.deleteFile(fileId, userID, driveToken)
                            self.deleteFileDetails(fileId)

        # Delete the file from temp
        self.genericUtil.emptyTemp()

        self.logger.info(f"Finished reading mail. Inserted {totalTransactions} transactions")
        return totalTransactions, totalIntegrityErrors

    def insertFileDetails(self, fileId, fileName, statementCount,
                          bank, user, path):
        fileDetails = FileDetails(
            fileID=fileId,
            uploadDate=self.dateTimeUtil.getCurrentDatetimeSqlFormat(),
            fileName=fileName,
            fileSize=self.genericUtil.getFileSize(path),
            statementCount=statementCount,
            bank=bank,
            user=user
        )
        try:
            self.db.session.add(fileDetails)
            self.db.session.commit()
        except IntegrityError as e:
            self.logger.warning(f"Duplicate file details entry error occurred: {e.__cause__}")
            self.db.session.rollback()

    def deleteFileDetails(self, fileId):
        # Find the row by ID and delete it
        row = self.db.session.query(FileDetails).filter_by(fileID=fileId).first()
        if row:
            self.db.session.delete(row)

    def updateStatementCount(self, fileId, newStatementCount):
        # Find the row by ID to update
        row = self.db.session.query(FileDetails).filter_by(fileID=fileId).first()
        if row:
            setattr(row, 'statementCount', newStatementCount)
            self.db.session.commit()

    def fetchFileDetails(self, page: int, filters: dict, page_size: int = 100):
        query = self.db.session.query(FileDetails)

        # Apply filters if they are provided
        if filters:
            if upload_date_range := filters.get('dateRange'):
                date_from = upload_date_range.get('dateFrom')
                date_to = upload_date_range.get('dateTo')
                if date_from and date_to:
                    query = query.filter(FileDetails.uploadDate.between(date_from, date_to))
            if file_name := filters.get('fileName'):
                query = query.filter(FileDetails.fileName.ilike(f"%{file_name}%"))
            if bank := filters.get('bank'):
                query = query.filter(FileDetails.bank == bank)

        # Get the total count before pagination
        total_count = query.count()

        # Apply sorting if `sorted` is provided
        if sorted := filters.get('sorted'):
            column, order = sorted.get('column'), sorted.get('order', 'asc')
            if column and hasattr(FileDetails, column):
                column_attr = getattr(FileDetails, column)
                if order == 'desc':
                    query = query.order_by(column_attr.desc())
                else:
                    query = query.order_by(column_attr.asc())

        # Apply pagination to the main query
        paginated_query = query.offset((page - 1) * page_size).limit(filters.get('limit', page_size))

        # Execute and return the results
        paginated_results = paginated_query.all()

        return {
            "count": total_count,
            "results": paginated_results,
            "page": page
        }

    def updateTransaction(self, reference_id: int, updates: dict):
        # Fetch the transaction by referenceID
        transaction = self.db.session.query(Transactions).filter_by(referenceID=reference_id).first()

        # If transaction is not found, return an error message
        if not transaction:
            return {"error": f"Transaction with referenceID {reference_id} not found"}

        # Update only the allowed fields if they are present in the updates dictionary
        if 'details' in updates:
            transaction.details = updates['details']
        if 'tag' in updates:
            transaction.tag = updates['tag']
        if 'amount' in updates:
            transaction.amount = updates['amount']

        # Commit the changes
        self.db.session.commit()
        return {"message": "Transaction updated successfully"}

    def addUser(self, user_data: dict):
        # Create a new User instance
        new_user = User(
            userID=user_data['userID'],
            email=user_data.get('email'),
            optedBanks=user_data.get('optedBanks')  # Can be None if not provided
        )

        # Add and commit the new user to the session
        self.db.session.add(new_user)
        self.db.session.commit()
        return {"message": "User added successfully"}

    def updateOptedBanks(self, user_id: str, opted_banks: str):
        # Fetch the user by userID
        user = self.db.session.query(User).filter_by(userID=user_id).first()

        # If user doesn't exist, return an error message
        if not user:
            return {"error": f"User with userID {user_id} not found"}

        # Update optedBanks, overwriting if it already has a value
        user.optedBanks = opted_banks
        self.db.session.commit()
        return {"message": "optedBanks updated successfully"}

    def addUpdateUserToken(self, token_data: dict):
        # Check if the token already exists for the user and service type
        user_token = self.db.session.query(UserToken).filter_by(
            user_id=token_data['user_id'],
            service_type=token_data['service_type']
        ).first()

        if user_token:
            # Update existing token
            user_token.access_token = token_data['access_token']
            user_token.refresh_token = token_data['refresh_token']
            user_token.client_id = token_data['client_id']
            user_token.client_secret = token_data['client_secret']
            user_token.expiry = token_data['expiry']
            message = "User token updated successfully."
        else:
            # Add new token
            user_token = UserToken(
                user_id=token_data['user_id'],
                access_token=token_data['access_token'],
                refresh_token=token_data['refresh_token'],
                client_id=token_data['client_id'],
                client_secret=token_data['client_secret'],
                expiry=token_data['expiry'],
                service_type=token_data['service_type']
            )
            self.db.session.add(user_token)
            message = "User token added successfully."

        # Commit changes to the database
        self.db.session.commit()
        return {"message": message}

    def deleteFile(self, user_id: str, fileId: str):
        session = self.db.session
        try:
            with session.begin():  # Start an outer transaction
                self.logger.info("Fetched drive token")
                driveToken = self.fetchDriveTokenForUser(user_id)
                self.deleteFileDetails(fileId)
                self.logger.info("Deleted file details")
                self.deleteTransactionsFromAFile(fileId)
                self.logger.info("Deleted transaction related to file")
                self.driveService.deleteFile(fileId, user_id, driveToken)
                self.logger.info("Deleted file on Google Drive")
        except SQLAlchemyError as e:
            session.rollback()  # Roll back the entire transaction if any error occurs
            self.logger.error(f"Error deleting file details. Error: {e}")
            raise Exception(e.__str__())  # Reraise the exception after logging
        except Exception as e:
            session.rollback()  # Roll back the entire transaction if any error occurs
            self.logger.error(f"Error deleting file. Error: {e}")
            raise Exception(e.__str__())  # Reraise the exception after logging
        return {"message": "File deleted successfully"}

    def deleteTransactionsFromAFile(self, fileID):
        result = self.db.session.query(Transactions).filter_by(fileID=fileID).delete(synchronize_session='fetch')
        return result

    def renameFile(self, user_id: str, fileId: str, newName: str):
        driveToken = self.fetchDriveTokenForUser(user_id)
        self.driveService.renameFile(fileId, newName, user_id, driveToken)
        fileDetails = self.db.session.query(FileDetails).filter_by(fileID=fileId).first()
        fileDetails.fileName = newName
        # If transaction is not found, return an error message
        if not fileDetails:
            return {"error": f"File info with fileID {fileId} not found"}
        self.db.session.commit()
        return {"message": "File renamed successfully"}

    def downloadFile(self, user_id: str, fileId: str):
        driveToken = self.fetchDriveTokenForUser(user_id)
        return self.driveService.downloadFile(fileId, user_id, driveToken)

    def checkGoogleStatus(self, user_id: str, serviceType: ServiceTypeEnum):
        if serviceType == ServiceTypeEnum.Gdrive:
            scopes = self.driveService.googleService.getDriveScope()
            try:
                token = self.fetchDriveTokenForUser(user_id)
                serviceCheck = self.driveService.checkStatus(token)
                if not serviceCheck:
                    return self.driveService.googleService.start_fresh_auth_flow(scopes)
                return {"Message": "Successful"}
            except:
                return self.driveService.googleService.start_fresh_auth_flow(scopes)
        elif serviceType == ServiceTypeEnum.Gmail:
            scopes = self.gmailService.googleService.getGmailScope()
            try:
                token = self.fetchGmailTokenForUser(user_id)
                serviceCheck = self.gmailService.checkStatus(token)
                if not serviceCheck:
                    return self.gmailService.googleService.start_fresh_auth_flow(scopes)
                return {"Message": "Successful"}
            except:
                return self.gmailService.googleService.start_fresh_auth_flow(scopes)
        return {"Message": "Weird Failure"}

    def setOptedBanks(self, user_id: str, banks:dict):
        """
           Updates the optedBanks for a user in the database.

           Args:
               user_id (str): The ID of the user.
               banks (list[str]): The list of bank names to set.

           Returns:
               User: The updated user object.
           """
        # Validate banks
        passwords = list(banks.values())
        banks = list(banks.keys())

        session: Session = self.db.session  # Replace with your DB session management
        user = None
        try:
            # Fetch user from DB
            with session.begin():
                user = session.query(User).filter(User.userID == user_id).first()
                if not user:
                    return None
                # Update optedBanks
                user.optedBanks = ','.join(banks)
                for index, password in passwords:
                    statement_password = StatementPasswords(
                        bank=banks[index],
                        password_hash=password,
                        user=user
                    )
                    # Add and commit the record
                    session.add(statement_password)
            return user
        except Exception as e:
            session.rollback()
            raise e
        finally:
            session.close()
