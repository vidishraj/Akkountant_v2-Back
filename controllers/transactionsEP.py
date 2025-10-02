from enums.BanksEnum import BankEnums
from enums.ServiceTypeEnum import ServiceTypeEnum
from services.transactionsService import TransactionService
from utils.logger import Logger
from flask import request, jsonify

from flask import g


class TransactionController:
    TransactionService: TransactionService

    def __init__(self, transactionService):
        self.TransactionService = transactionService
        self.logger = Logger(__name__).get_logger()

    @Logger.standardLogger
    def fetchTransactions(self):
        data = request.get_json(force=True)
        page = data.get("Page", 1)
        filters = data.get("Filter", None)
        self.logger.info(f"Fetch {page} with filter {filters}")
        transactions = self.TransactionService.fetchTransactions(page=page, filters=filters)
        # Format the transactions for JSON response
        results = []
        for t in transactions["results"]:
            transaction_dict = {}
            for key, value in t.__dict__.items():
                if key != '_sa_instance_state':
                    # Convert enum objects to their string values for JSON serialization
                    if hasattr(value, 'value'):  # Check if it's an enum
                        transaction_dict[key] = value.value
                    else:
                        transaction_dict[key] = value
            results.append(transaction_dict)
        response = {
            "total_count": transactions["count"],
            "page": page,
            "credit_sum": transactions["credit_sum"],
            "debit_sum": transactions["debit_sum"],
            "page_size": len(results),
            "results": results,
        }

        # Return the response
        return jsonify(response), 200

    def fetchOptedBanks(self):
        """
                Endpoint to fetch transaction and statement dates for the calendar view.
                """

        userId = g.get('firebase_id')
        # Ensure both `userID` and `optedBanks` are provided
        if not userId:
            return jsonify({"error": "userID are required"}), 400

        # Call the update_opted_banks service function
        result = self.TransactionService.fetchBanksOptedByUser(userId)
        if "error" in result:
            return jsonify(result), 404  # Return 404 if user not found
        return jsonify(result), 200  # Return 200 OK on success

    @Logger.standardLogger
    def fetchCalendarTransactions(self):
        """
        Endpoint to fetch transaction and statement dates for the calendar view.
        """
        data = request.get_json(force=True)
        month_start = data.get("monthStart")  # Expected in "yyyy-mm-dd" format
        month_end = data.get("monthEnd")  # Expected in "yyyy-mm-dd" format

        if not month_start or not month_end:
            return jsonify({"error": "Invalid or missing date range"}), 400

        self.logger.info(f"Fetching transactions for range: {month_start} - {month_end}")

        # Fetch transactions and statements from the service
        transactions = self.TransactionService.fetchTransactionDates(
            date_from=month_start, date_to=month_end
        )

        # Format the response
        response = {
            "transaction_dates": transactions.get("transaction_dates", []),
            "statement_dates": transactions.get("statement_dates", []),
        }

        return jsonify(response), 200

    @Logger.standardLogger
    def triggerEmailCheck(self):
        userId = request.headers.get("X-Firebase-ID")
        dateTo = request.args.get('dateTo')
        dateFrom = request.args.get('dateFrom')
        algorithm = request.args.get('algorithm', 'claude')  # Default to claude, can be 'regex' or 'claude'
        self.logger.info(f"Reading email for user {userId} using {algorithm} algorithm")
        successCount, errorCount = \
            self.TransactionService.readTransactionFromMail(dateTo=dateTo, dateFrom=dateFrom, userID=userId, algorithm=algorithm)
        return jsonify({"Message": {
            "read": successCount,
            "conflicts": errorCount
        }}), 200

    @Logger.standardLogger
    def triggerStatementCheck(self):
        userId = request.headers.get("X-Firebase-ID")
        dateTo = request.args.get('dateTo')
        dateFrom = request.args.get('dateFrom')
        bank = request.args.get('bank')
        algorithm = request.args.get('algorithm', 'claude')  # Default to claude, can be 'regex' or 'claude'
        self.logger.info(f"Reading statements for user {userId} using {algorithm} algorithm")
        successCount, errorCount = \
            self.TransactionService.readStatementsFromMail(dateTo=dateTo, dateFrom=dateFrom, userID=userId, bank=bank, algorithm=algorithm)
        return jsonify({"Message": {
            "read": successCount,
            "conflicts": errorCount
        }}), 200

    @Logger.standardLogger
    def updateTransaction(self):
        data = request.get_json()
        reference_id = data.get("referenceID")
        updates = data.get("updates", {})
        # Validate that referenceID is provided
        if not reference_id:
            return jsonify({"error": "referenceID is required"}), 400
        # Call the update service function
        result = self.TransactionService.updateTransaction(reference_id, updates)
        if "error" in result:
            return jsonify(result), 404  # Return 404 if transaction not found
        return jsonify(result), 200  # Return 200 if update is successful

    @Logger.standardLogger
    def addUser(self):
        data = request.get_json()

        # Ensure `userID` is provided in the request data
        if 'userID' not in data:
            return jsonify({"error": "userID is required"}), 400

        # Call the add_user service function
        result = self.TransactionService.addUser(data)
        return jsonify(result), 201  # Return 201 Created on success

    @Logger.standardLogger
    def updateOptedBanks(self):
        data = request.get_json()
        user_id = data.get("userID")
        opted_banks = data.get("optedBanks")

        # Ensure both `userID` and `optedBanks` are provided
        if not user_id or opted_banks is None:
            return jsonify({"error": "userID and optedBanks are required"}), 400

        # Call the update_opted_banks service function
        result = self.TransactionService.updateOptedBanks(user_id, opted_banks)
        if "error" in result:
            return jsonify(result), 404  # Return 404 if user not found
        return jsonify(result), 200  # Return 200 OK on success

    @Logger.standardLogger
    def addUpdateUserToken(self):
        data = request.get_json()
        userId = g.get('firebase_id')
        # Ensure all required fields are present
        required_fields = ['access_token', 'refresh_token', 'client_id', 'client_secret', 'expiry',
                           'service_type']
        missing_fields = [field for field in required_fields if field not in data]
        if missing_fields or not userId:
            return jsonify({"error": f"Missing required fields: {', '.join(missing_fields)}"}), 400
        # Call the service function to add or update the token
        data['user_id'] = userId
        result = self.TransactionService.addUpdateUserToken(data)
        return jsonify(result), 200

    @Logger.standardLogger
    def deleteFile(self):
        """Enhanced to handle both legacy Google Drive and local statements"""
        userId = g.get('firebase_id')
        fileId = request.args.get('fileId')
        
        # Ensure all required fields are present
        if userId is None or fileId is None:
            return jsonify({"error": "Missing required fields"}), 400
            
        # Check if this is a local statement (fileId format: local_BANK_filename)
        if fileId.startswith('local_'):
            try:
                # Parse local file ID: local_BANK_filename.pdf
                parts = fileId.split('_', 2)  # Split into max 3 parts
                if len(parts) >= 3:
                    bank = parts[1]
                    filename = parts[2]
                    result = self.TransactionService.deleteLocalStatement(userId, bank, filename)
                    if "error" in result:
                        return jsonify(result), 404
                    return jsonify(result), 200
                else:
                    return jsonify({"error": "Invalid local file ID format"}), 400
            except Exception as e:
                return jsonify({"error": f"Failed to delete local statement: {str(e)}"}), 500
        else:
            # Legacy Google Drive deletion
            result = self.TransactionService.deleteFile(userId, fileId)
            return jsonify(result), 200

    @Logger.standardLogger
    def renameFile(self):
        """Enhanced to handle both legacy Google Drive and local statements"""
        data = request.get_json()

        # Ensure all required fields are present
        required_fields = ['user_id', 'fileId', 'newName']
        missing_fields = [field for field in required_fields if field not in data]
        if missing_fields:
            return jsonify({"error": f"Missing required fields: {', '.join(missing_fields)}"}), 400
            
        fileId = data['fileId']
        userId = data['user_id']
        newName = data['newName']
        
        # Check if this is a local statement (fileId format: local_BANK_filename)
        if fileId.startswith('local_'):
            # Note: Local statements cannot be renamed as they are stored based on email identifiers
            # This could be implemented if needed by moving files, but for now return not supported
            return jsonify({"error": "Renaming local Claude statements is not currently supported"}), 400
        else:
            # Legacy Google Drive rename
            result = self.TransactionService.renameFile(userId, fileId, newName)
            return jsonify(result), 200

    @Logger.standardLogger
    def downloadFile(self):
        """Enhanced to handle both legacy Google Drive and local statements"""
        userId = g.get('firebase_id')
        fileId = request.args.get('fileId')
        
        # Ensure all required fields are present
        if userId is None or fileId is None:
            return jsonify({"error": "Missing required fields"}), 400
            
        # Check if this is a local statement (fileId format: local_BANK_filename)
        if fileId.startswith('local_'):
            try:
                # Parse local file ID: local_BANK_filename.pdf
                parts = fileId.split('_', 2)  # Split into max 3 parts
                if len(parts) >= 3:
                    bank = parts[1]
                    filename = parts[2]
                    return self.downloadLocalStatement_internal(userId, bank, filename)
                else:
                    return jsonify({"error": "Invalid local file ID format"}), 400
            except Exception as e:
                return jsonify({"error": f"Failed to download local statement: {str(e)}"}), 500
        else:
            # Legacy Google Drive download
            result = self.TransactionService.downloadFile(userId, fileId)
            return result

    def downloadLocalStatement_internal(self, userId, bank, filename):
        """Internal method for downloading local statements"""
        from flask import send_file
        result = self.TransactionService.downloadLocalStatement(userId, bank, filename)
        if "error" in result:
            return jsonify(result), 404
            
        try:
            return send_file(result["file_path"], as_attachment=True, download_name=result["filename"])
        except Exception as e:
            return jsonify({"error": f"File download failed: {str(e)}"}), 500

    @Logger.standardLogger
    def fetchFileDetails(self):
        """Redirects to enhanced version that handles both legacy and local statements"""
        return self.fetchFileDetailsEnhanced()

    @Logger.standardLogger
    def checkGoogleApiStatus(self):
        userId = g.get('firebase_id')
        service = request.args.get('serviceType')
        self.logger.info(f"userID: {userId}")
        # Call the service function to add or update the token
        result = self.TransactionService.checkGoogleStatus(userId, ServiceTypeEnum[service.capitalize()])
        return jsonify(result), 200

    @Logger.standardLogger
    def setOptedBanks(self):
        try:
            data = request.json
            userId = request.headers.get("X-Firebase-ID")
            banks = data.get('banks')
            validBanks = list(banks.keys())
            # Validate input
            if not userId or not banks:
                return jsonify({"error": "userID and banks are required"}), 400

            if not all(bank in BankEnums.__members__ for bank in validBanks):
                return jsonify({"error": "Invalid bank(s) provided"}), 400

            # Call the service method
            updated_user = self.TransactionService.setOptedBanks(userId, banks)
            if updated_user is None:
                return jsonify({"error": "User not found"}), 404

            return jsonify({"message": "Opted banks updated successfully"}), 200
        except Exception as e:
            return jsonify({"error": str(e)}), 500

    @Logger.standardLogger
    def getProcessingStats(self):
        userId = g.get('firebase_id')
        stats = self.TransactionService.getProcessingStats(userId)
        return jsonify(stats), 200


    # ENHANCED: Legacy endpoints with storage type awareness
    @Logger.standardLogger
    def fetchFileDetailsEnhanced(self):
        """Enhanced file details - by default returns local statements only"""
        data = request.get_json(force=True)
        page = data.get("Page", 1)
        filters = data.get("Filter", {})
        userId = g.get('firebase_id')
        
        # Check if legacy statements should be included (opt-in)
        include_legacy = filters.get("include_legacy", False) if filters else False
        
        self.logger.info(f"Fetch Enhanced FileDetails Page {page} with filter {filters}, include_legacy={include_legacy}")
        
        # Initialize results
        legacy_results = []
        local_results = []
        
        # Get legacy file details only if requested
        if include_legacy:
            # Add user filter if not present
            if userId:
                filters["user"] = userId
            legacy_files = self.TransactionService.fetchFileDetails(page=page, filters=filters)
            
            # Format legacy file details
            legacy_results = [
                {**{key: value for key, value in fd.__dict__.items() if key != '_sa_instance_state'},
                 "storage_type": "google_drive"}
                for fd in legacy_files["results"]
            ]
        
        # Get local statements (default behavior)
        local_statements = self.TransactionService.fetchLocalStatements(userId)
        
        # Format local statements to match legacy structure
        for stmt in local_statements.get("statements", []):
            local_results.append({
                "fileID": f"local_{stmt['bank']}_{stmt['filename']}",
                "fileName": stmt["filename"],
                "bank": stmt["bank"],
                "fileSize": stmt["file_size"],
                "uploadDate": stmt["created_date"],
                "statementCount": "N/A",  # Local statements don't track this
                "storage_type": "local_claude",
                "user": userId
            })
        
        # By default, only return local statements
        results = local_results
        if include_legacy:
            results = legacy_results + local_results
        
        response = {
            "total_count": len(results),
            "legacy_count": len(legacy_results),
            "local_count": len(local_results),
            "page": page,
            "page_size": len(results),
            "results": results,
        }
        return jsonify(response), 200
