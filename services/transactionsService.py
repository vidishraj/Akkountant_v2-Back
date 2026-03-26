import os
import shutil

from flask_sqlalchemy.session import Session
from sqlalchemy.exc import IntegrityError, SQLAlchemyError
from sqlalchemy import func, case

from enums.ServiceTypeEnum import ServiceTypeEnum
from enums.TransactionTypeEnum import TransactionTypeEnum
from models import User, UserToken, Transactions, TransactionForReview, StatementPasswords, FileDetails
from models.transactions import ProcessingMethod
from services.Base_Service import BaseService
from utils.logger import Logger


class TransactionService(BaseService):
    _instance = None

    def __new__(cls):
        if cls._instance is None:
            cls._instance = super(TransactionService, cls).__new__(cls)
            cls.logger = Logger(__name__).get_logger()
        return cls._instance

    def __init__(self):
        super().__init__()

    def fetchTransactions(self, page: int, filters: dict, user_id: str = None, page_size: int = 100):
        query = self.db.session.query(Transactions)

        # Filter by user
        if user_id:
            query = query.filter(Transactions.user == user_id)

        # Determine if we should apply the default Claude filter
        apply_claude_default = True
        if filters and 'processed_via' in filters:
            processed_via = filters.get('processed_via')
            if processed_via == 'ALL':
                apply_claude_default = False
            elif processed_via == 'CLAUDE_CODE':
                query = query.filter(Transactions.processed_via == ProcessingMethod.CLAUDE_CODE)
                apply_claude_default = False
            elif processed_via == 'PATTERN_MATCH':
                query = query.filter(Transactions.processed_via == ProcessingMethod.PATTERN_MATCH)
                apply_claude_default = False
        
        # Apply default Claude filter if no explicit processing method filter
        if apply_claude_default:
            query = query.filter(Transactions.processed_via == ProcessingMethod.CLAUDE_CODE)

        # Apply other filters if they are provided
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

        # Apply user filter to count query
        if user_id:
            total_count_query = total_count_query.filter(Transactions.user == user_id)

        # Apply the same processing method filter to count query
        if apply_claude_default:
            total_count_query = total_count_query.filter(Transactions.processed_via == ProcessingMethod.CLAUDE_CODE)
        elif filters and 'processed_via' in filters:
            processed_via = filters.get('processed_via')
            if processed_via == 'CLAUDE_CODE':
                total_count_query = total_count_query.filter(Transactions.processed_via == ProcessingMethod.CLAUDE_CODE)
            elif processed_via == 'PATTERN_MATCH':
                total_count_query = total_count_query.filter(Transactions.processed_via == ProcessingMethod.PATTERN_MATCH)

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

        # Apply user filter to sum query
        if user_id:
            sum_query = sum_query.filter(Transactions.user == user_id)

        # Apply the same processing method filter to sum query
        if apply_claude_default:
            sum_query = sum_query.filter(Transactions.processed_via == ProcessingMethod.CLAUDE_CODE)
        elif filters and 'processed_via' in filters:
            processed_via = filters.get('processed_via')
            if processed_via == 'CLAUDE_CODE':
                sum_query = sum_query.filter(Transactions.processed_via == ProcessingMethod.CLAUDE_CODE)
            elif processed_via == 'PATTERN_MATCH':
                sum_query = sum_query.filter(Transactions.processed_via == ProcessingMethod.PATTERN_MATCH)

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
        from enums.BanksEnum import BankEnums
        return [bank.value for bank in BankEnums]

    def fetchTransactionDates(self, date_from: str, date_to: str, user_id: str = None):
        """
        Service to fetch transaction and statement dates within a given date range,
        plus statement period coverage for the calendar.
        """
        from models.statementPeriods import StatementPeriod

        transaction_query = (
            self.db.session.query(func.date(Transactions.date).label('txn_date'))
            .filter(Transactions.date.between(date_from, date_to))
        )
        if user_id:
            transaction_query = transaction_query.filter(Transactions.user == user_id)
        transaction_query = transaction_query.distinct()

        statement_query = (
            self.db.session.query(FileDetails.uploadDate)
            .filter(FileDetails.uploadDate.between(date_from, date_to))
            .filter(FileDetails.deleted == False)
        )
        if user_id:
            statement_query = statement_query.filter(FileDetails.user == user_id)
        statement_query = statement_query.distinct()

        # Statement periods that overlap with the requested date range
        period_query = self.db.session.query(StatementPeriod).filter(
            StatementPeriod.period_start <= date_to,
            StatementPeriod.period_end >= date_from,
        )
        if user_id:
            period_query = period_query.filter(StatementPeriod.user == user_id)
        period_query = period_query.all()

        transaction_dates = [t[0].strftime("%Y-%m-%d") if hasattr(t[0], 'strftime') else str(t[0]) for t in transaction_query]
        statement_dates = [s[0].strftime("%Y-%m-%d") for s in statement_query]
        covered_periods = [{
            "bank": p.bank,
            "period_start": p.period_start.isoformat(),
            "period_end": p.period_end.isoformat(),
        } for p in period_query]

        return {
            "transaction_dates": transaction_dates,
            "statement_dates": statement_dates,
            "covered_periods": covered_periods,
        }



    def insertTransactions(self, transactions, bank, userId, conflicts, source, fileId=None, source_emails=None):
        """OPTIMIZED: Batch insert transactions for better performance"""
        integrityErrors = 0
        
        if not transactions and not conflicts:
            return 0
            
        try:
            # OPTIMIZATION: Batch prepare all transaction objects
            transaction_objects = []
            
            for i, transaction in enumerate(transactions):
                date = self.dateTimeUtil.convert_to_sql_datetime(transaction['date'], bank)
                
                # Determine processing method
                processing_method = ProcessingMethod.CLAUDE_CODE if transaction.get('processed_via') == 'CLAUDE_CODE' else ProcessingMethod.PATTERN_MATCH
                
                # For email-based transactions, get the Gmail message ID
                gmail_message_id = None
                if source_emails and i < len(source_emails) and source == TransactionTypeEnum.Email.value:
                    gmail_message_id = source_emails[i].get('message_id')
                
                transaction_obj = Transactions(
                    referenceID=transaction['reference'],
                    date=date,
                    details=transaction['description'],
                    amount=transaction['amount'],
                    tag="",
                    fileID=fileId,
                    bank=bank,
                    source=source,
                    user=userId,
                    processed_via=processing_method,
                    gmail_message_id=gmail_message_id
                )
                transaction_objects.append(transaction_obj)
            
            # OPTIMIZATION: Batch insert all transactions in a single transaction
            if transaction_objects:
                try:
                    if isinstance(self.db, dict):
                        with self.db.session() as session:
                            session.add_all(transaction_objects)
                            session.commit()
                    else:
                        self.db.session.add_all(transaction_objects)
                        self.db.session.commit()
                    self.logger.info(f"Batch inserted {len(transaction_objects)} transactions")
                except IntegrityError as e:
                    self.logger.warning(f"Batch insert failed, falling back to individual inserts: {e}")
                    self.db.session.rollback()
                    # Fallback to individual inserts to handle duplicates
                    integrityErrors = self._insert_transactions_individually(transaction_objects)

            # OPTIMIZATION: Batch insert conflicts with duplicate checking
            if conflicts:
                # Check for existing conflicts to avoid duplicates
                existing_conflicts = set()
                if isinstance(self.db, dict):
                    with self.db.session() as session:
                        existing = session.query(TransactionForReview.conflict).filter(
                            TransactionForReview.user == userId
                        ).all()
                        existing_conflicts = {row.conflict for row in existing}
                else:
                    existing = self.db.session.query(TransactionForReview.conflict).filter(
                        TransactionForReview.user == userId
                    ).all()
                    existing_conflicts = {row.conflict for row in existing}
                
                # Filter out duplicate conflicts
                new_conflicts = [conflict for conflict in conflicts if conflict not in existing_conflicts]
                
                if new_conflicts:
                    conflict_objects = [
                        TransactionForReview(user=userId, conflict=conflict) 
                        for conflict in new_conflicts
                    ]
                    try:
                        if isinstance(self.db, dict):
                            with self.db.session() as session:
                                session.add_all(conflict_objects)
                                session.commit()
                        else:
                            self.db.session.add_all(conflict_objects)
                            self.db.session.commit()
                        self.logger.info(f"Batch inserted {len(conflict_objects)} new conflicts (skipped {len(conflicts) - len(new_conflicts)} duplicates)")
                    except IntegrityError as e:
                        self.logger.warning(f"Batch conflict insert failed: {e}")
                        self.db.session.rollback()
                else:
                    self.logger.info(f"Skipped {len(conflicts)} duplicate conflicts")
                    
        except Exception as e:
            self.logger.error(f"Error in batch insert: {str(e)}")
            self.db.session.rollback()
            
        return integrityErrors

    def _insert_transactions_individually(self, transaction_objects):
        """Fallback method for individual transaction inserts when batch fails"""
        integrityErrors = 0
        
        for transaction_obj in transaction_objects:
            try:
                if isinstance(self.db, dict):
                    with self.db.session() as session:
                        session.add(transaction_obj)
                        session.commit()
                else:
                    self.db.session.add(transaction_obj)
                    self.db.session.commit()
            except IntegrityError as e:
                error_msg = str(e)
                if 'gmail_message_id' in error_msg and transaction_obj.gmail_message_id:
                    self.logger.debug(f"Skipping duplicate email transaction (Gmail ID: {transaction_obj.gmail_message_id})")
                else:
                    self.logger.debug(f"Skipping duplicate transaction: {transaction_obj.referenceID}")
                self.db.session.rollback()
                integrityErrors += 1
                
        return integrityErrors

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

    def insertFileDetails(self, fileId, fileName, statementCount,
                          bank, user, path, gmail_message_id=None):
        fileDetails = FileDetails(
            fileID=fileId,
            uploadDate=self.dateTimeUtil.getCurrentDatetimeSqlFormat(),
            fileName=fileName,
            fileSize=self.genericUtil.getFileSize(path),
            statementCount=statementCount,
            bank=bank,
            user=user,
            gmail_message_id=gmail_message_id
        )
        try:
            if isinstance(self.db, dict):
                with self.db.session() as session:
                    session.add(fileDetails)
                    session.commit()
            else:
                self.db.session.add(fileDetails)
                self.db.session.commit()
        except IntegrityError as e:
            self.logger.warning(f"Duplicate file details entry error occurred: {e.__cause__}")
            self.db.session.rollback()

    def deleteFileDetails(self, fileId, user_id=None):
        # Find the row by ID and delete it
        query = self.db.session.query(FileDetails).filter_by(fileID=fileId)
        if user_id:
            query = query.filter(FileDetails.user == user_id)
        row = query.first()
        if row:
            if isinstance(self.db, dict):
                with self.db.session() as session:
                    session.delete(row)
                    session.commit()
            else:
                self.db.session.delete(row)
                self.db.session.commit()

    def updateStatementCount(self, fileId, newStatementCount):
        # Find the row by ID to update
        row = self.db.session.query(FileDetails).filter_by(fileID=fileId).first()
        if row:
            setattr(row, 'statementCount', newStatementCount)
            if isinstance(self.db, dict):
                with self.db.session() as session:
                    session.commit()
            else:
                self.db.session.commit()

    def fetchFileDetails(self, page: int, filters: dict, user_id: str = None, page_size: int = 100):
        query = self.db.session.query(FileDetails).filter(FileDetails.deleted == False)

        # Filter by user
        if user_id:
            query = query.filter(FileDetails.user == user_id)

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

    def updateTransaction(self, reference_id: int, updates: dict, user_id: str = None):
        # Fetch the transaction by referenceID
        query = self.db.session.query(Transactions).filter_by(referenceID=reference_id)
        if user_id:
            query = query.filter(Transactions.user == user_id)
        transaction = query.first()

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
                self.deleteFileDetails(fileId, user_id=user_id)
                self.logger.info("Deleted file details")
                self.deleteTransactionsFromAFile(fileId, user_id=user_id)
                self.logger.info("Deleted transaction related to file")
                self.driveService.deleteFile(fileId, user_id, driveToken)
                self.logger.info("Deleted file on Google Drive")
        except SQLAlchemyError as e:
            session.rollback()
            self.logger.error(f"Error deleting file details. Error: {e}")
            raise
        except Exception as e:
            session.rollback()
            self.logger.error(f"Error deleting file. Error: {e}")
            raise
        return {"message": "File deleted successfully"}

    def deleteTransactionsFromAFile(self, fileID, user_id=None):
        query = self.db.session.query(Transactions).filter_by(fileID=fileID)
        if user_id:
            query = query.filter(Transactions.user == user_id)
        result = query.delete(synchronize_session='fetch')
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
            except Exception:
                return self.driveService.googleService.start_fresh_auth_flow(scopes)
        elif serviceType == ServiceTypeEnum.Gmail:
            scopes = self.gmailService.googleService.getGmailScope()
            try:
                token = self.fetchGmailTokenForUser(user_id)
                serviceCheck = self.gmailService.checkStatus(token)
                if not serviceCheck:
                    return self.gmailService.googleService.start_fresh_auth_flow(scopes)
                return {"Message": "Successful"}
            except Exception:
                return self.gmailService.googleService.start_fresh_auth_flow(scopes)
        return {"Message": "Weird Failure"}

    def setOptedBanks(self, user_id: str, banks: dict):
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
                for index, password in enumerate(passwords):
                    statementPassword = self.db.session.query(StatementPasswords).filter_by(user=user_id) \
                        .filter_by(bank=banks[index]).first()
                    if statementPassword is None:
                        statement_password = StatementPasswords(
                            bank=banks[index],
                            password_hash=password,
                            user=user_id
                        )
                        # Add and commit the record
                        session.add(statement_password)
                    else:
                        statementPassword.password_hash = password
            return user
        except Exception as e:
            session.rollback()
            raise e
        finally:
            session.close()

    def getProcessingStats(self, userId):
        try:
            total_count = self.db.session.query(func.count(Transactions.referenceID)).filter_by(user=userId).scalar()
            
            pattern_count = self.db.session.query(func.count(Transactions.referenceID)).filter(
                Transactions.user == userId,
                Transactions.processed_via == ProcessingMethod.PATTERN_MATCH
            ).scalar()
            
            claude_count = self.db.session.query(func.count(Transactions.referenceID)).filter(
                Transactions.user == userId,
                Transactions.processed_via == ProcessingMethod.CLAUDE_CODE
            ).scalar()

            return {
                "total_transactions": total_count or 0,
                "pattern_match_count": pattern_count or 0,
                "claude_code_count": claude_count or 0,
                "claude_success_rate": round((claude_count or 0) / max(total_count or 1, 1) * 100, 2)
            }
        except Exception as e:
            self.logger.error(f"Error fetching processing stats: {str(e)}")
            return {"error": str(e)}

    # NEW: Local Statement Management Methods
    def fetchLocalStatements(self, userId, bank=None):
        """Fetch list of locally stored Claude-analyzed statements"""
        try:
            statements_dir = os.path.join(os.getcwd(), "claude_statements", userId)
            
            if not os.path.exists(statements_dir):
                return {"statements": [], "total_count": 0}
            
            statements = []
            banks_to_check = [bank] if bank else os.listdir(statements_dir)
            
            for bank_name in banks_to_check:
                bank_dir = os.path.join(statements_dir, bank_name)
                if not os.path.isdir(bank_dir):
                    continue
                    
                for filename in os.listdir(bank_dir):
                    if filename.endswith('.pdf'):
                        file_path = os.path.join(bank_dir, filename)
                        file_stat = os.stat(file_path)
                        
                        statements.append({
                            "filename": filename,
                            "bank": bank_name,
                            "file_size": file_stat.st_size,
                            "created_date": file_stat.st_ctime,
                            "modified_date": file_stat.st_mtime,
                            "file_path": file_path,
                            "storage_type": "local_claude"
                        })
            
            # Sort by modified date (newest first)
            statements.sort(key=lambda x: x['modified_date'], reverse=True)
            
            return {
                "statements": statements,
                "total_count": len(statements)
            }
            
        except Exception as e:
            self.logger.error(f"Error fetching local statements: {str(e)}")
            return {"error": str(e)}

    def downloadLocalStatement(self, userId, bank, filename):
        """Download a locally stored statement"""
        try:
            # Sanitize to prevent path traversal
            safe_bank = os.path.basename(bank)
            safe_filename = os.path.basename(filename)
            base_dir = os.path.join(os.getcwd(), "claude_statements", userId)
            file_path = os.path.join(base_dir, safe_bank, safe_filename)

            # Verify the resolved path is within the expected directory
            if not os.path.realpath(file_path).startswith(os.path.realpath(base_dir)):
                return {"error": "Invalid file path"}

            if not os.path.exists(file_path):
                return {"error": "Statement not found"}
            
            # Return file path for download (Flask will handle the actual file serving)
            return {"file_path": file_path, "filename": filename}
            
        except Exception as e:
            self.logger.error(f"Error downloading local statement: {str(e)}")
            return {"error": str(e)}

    def deleteLocalStatement(self, userId, bank, filename):
        """Delete a locally stored statement"""
        try:
            # Sanitize to prevent path traversal
            safe_bank = os.path.basename(bank)
            safe_filename = os.path.basename(filename)
            base_dir = os.path.join(os.getcwd(), "claude_statements", userId)
            file_path = os.path.join(base_dir, safe_bank, safe_filename)

            # Verify the resolved path is within the expected directory
            if not os.path.realpath(file_path).startswith(os.path.realpath(base_dir)):
                return {"error": "Invalid file path"}

            if not os.path.exists(file_path):
                return {"error": "Statement not found"}

            os.remove(file_path)
            self.logger.info(f"Deleted local statement: {file_path}")
            
            # Check if bank directory is empty and remove if so
            bank_dir = os.path.dirname(file_path)
            if not os.listdir(bank_dir):
                os.rmdir(bank_dir)
                self.logger.info(f"Removed empty bank directory: {bank_dir}")
            
            return {"message": "Statement deleted successfully"}
            
        except Exception as e:
            self.logger.error(f"Error deleting local statement: {str(e)}")
            return {"error": str(e)}

    def fetchProcessedStatements(self, userId, page=1, limit=100, filters=None):
        """Fetch processed email records from processedEmails table for the statements view."""
        from models.processedEmails import ProcessedEmails
        from sqlalchemy import desc, asc

        filters = filters or {}
        query = self.db.session.query(ProcessedEmails).filter_by(user_id=userId)

        # Default to bank_statement category — only show statement files
        # unless a specific category filter is provided
        bank_filter = filters.get("bank")
        if bank_filter:
            query = query.filter(ProcessedEmails.category == bank_filter)
        else:
            query = query.filter(ProcessedEmails.category == 'bank_statement')

        # Filter by fileName (search in subject)
        file_name_filter = filters.get("fileName")
        if file_name_filter:
            query = query.filter(ProcessedEmails.subject.ilike(f"%{file_name_filter}%"))

        # Date range filter
        date_range = filters.get("dateRange")
        if date_range:
            if date_range.get("dateFrom"):
                query = query.filter(ProcessedEmails.email_date >= date_range["dateFrom"])
            if date_range.get("dateTo"):
                query = query.filter(ProcessedEmails.email_date <= date_range["dateTo"])

        # Sort
        sorted_config = filters.get("sorted", {})
        sort_col_name = sorted_config.get("column", "processed_at")
        sort_dir = sorted_config.get("order", "desc")

        col_map = {
            "uploadDate": ProcessedEmails.processed_at,
            "processed_at": ProcessedEmails.processed_at,
            "fileName": ProcessedEmails.subject,
            "bank": ProcessedEmails.category,
            "email_date": ProcessedEmails.email_date,
        }
        sort_col = col_map.get(sort_col_name, ProcessedEmails.processed_at)
        order_fn = desc if sort_dir == "desc" else asc
        query = query.order_by(order_fn(sort_col))

        total = query.count()
        offset = (page - 1) * limit
        rows = query.offset(offset).limit(limit).all()

        # Batch-load statement periods for all gmail_ids in this page
        from models.statementPeriods import StatementPeriod
        gmail_ids = [row.gmail_id for row in rows if row.gmail_id]
        period_map = {}
        if gmail_ids:
            periods = self.db.session.query(StatementPeriod).filter(
                StatementPeriod.gmail_message_id.in_(gmail_ids),
                StatementPeriod.user == userId,
            ).all()
            for p in periods:
                period_map[p.gmail_message_id] = p

        results = []
        for row in rows:
            has_pdf = False
            if row.pdf_filename:
                full_path = os.path.join(os.getcwd(), "claude_statements", userId, row.pdf_filename)
                has_pdf = os.path.exists(full_path)

            # Extract bank from extraction_summary if available
            summary = row.extraction_summary or {}
            bank = summary.get("bank", row.category or "Unknown")

            # Build a human-readable summary string
            summary_text = ""
            items = row.items_extracted or 0
            if items > 0:
                summary_text = f"{items} item(s) extracted"
                if summary.get("inserted"):
                    summary_text = f"{summary['inserted']} inserted"
                if summary.get("duplicates"):
                    summary_text += f", {summary['duplicates']} duplicates"
            elif row.status == "skipped":
                summary_text = row.error_message or "Skipped"
            elif row.status == "failed":
                summary_text = row.error_message or "Processing failed"
            else:
                summary_text = "No items extracted"

            # Look up statement period coverage
            period_record = period_map.get(row.gmail_id)

            results.append({
                "fileID": f"pe_{row.gmail_id}",
                "fileName": row.subject or row.pdf_filename or row.gmail_id,
                "bank": bank,
                "uploadDate": row.processed_at.isoformat() if row.processed_at else "",
                "email_date": row.email_date.isoformat() if row.email_date else "",
                "statementCount": items,
                "category": row.category,
                "status": row.status,
                "items_extracted": items,
                "extraction_summary": summary,
                "summary_text": summary_text,
                "sender": row.sender,
                "subject": row.subject,
                "has_pdf": has_pdf,
                "user": userId,
                "period_start": period_record.period_start.isoformat() if period_record and period_record.period_start else None,
                "period_end": period_record.period_end.isoformat() if period_record and period_record.period_end else None,
            })

        return {
            "total_count": total,
            "page": page,
            "page_size": limit,
            "results": results,
        }

    def getProcessedEmailByGmailId(self, userId, gmail_id):
        """Look up a processedEmails row by gmail_id and user_id."""
        from models.processedEmails import ProcessedEmails
        try:
            return self.db.session.query(ProcessedEmails).filter_by(
                gmail_id=gmail_id, user_id=userId
            ).first()
        except Exception as e:
            self.logger.error(f"Error fetching processed email {gmail_id}: {str(e)}")
            return None

    def delete_email_transactions_for_period(self, user_id, bank, period_start, period_end):
        """Delete email-sourced transactions for a bank+period. Returns count deleted."""
        try:
            from sqlalchemy import func as sa_func
            deleted = self.db.session.query(Transactions).filter(
                Transactions.user == user_id,
                Transactions.bank == bank,
                sa_func.lower(Transactions.source) == 'email',
                Transactions.date.between(period_start, period_end),
            ).delete(synchronize_session='fetch')
            self.db.session.commit()
            self.logger.info(
                f"Deleted {deleted} email transactions for {bank} "
                f"({period_start} to {period_end})"
            )
            return deleted
        except Exception as e:
            self.db.session.rollback()
            self.logger.error(f"Error deleting email transactions: {str(e)}")
            return 0

    def fetchAllStatements(self, userId, include_legacy=True, include_local=True):
        """Fetch both legacy (Google Drive) and local statements"""
        try:
            result = {
                "legacy_statements": [],
                "local_statements": [],
                "total_count": 0
            }
            
            # Fetch legacy Google Drive statements
            if include_legacy:
                legacy_files = self.fetchFileDetails(page=1, filters={}, user_id=userId)
                result["legacy_statements"] = [
                    {**{key: value for key, value in fd.__dict__.items() if key != '_sa_instance_state'}, 
                     "storage_type": "google_drive"}
                    for fd in legacy_files["results"]
                ]
            
            # Fetch local Claude statements  
            if include_local:
                local_statements = self.fetchLocalStatements(userId)
                result["local_statements"] = local_statements.get("statements", [])
            
            result["total_count"] = len(result["legacy_statements"]) + len(result["local_statements"])
            
            return result
            
        except Exception as e:
            self.logger.error(f"Error fetching all statements: {str(e)}")
            return {"error": str(e)}
