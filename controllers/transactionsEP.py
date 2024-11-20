from enums.ServiceTypeEnum import ServiceTypeEnum
from services.transactionsService import TransactionService
from utils.logger import Logger
from flask import request, jsonify


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
        results = [
            {key: value for key, value in t.__dict__.items() if key != '_sa_instance_state'}
            for t in transactions["results"]
        ]
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

    @Logger.standardLogger
    def triggerEmailCheck(self):
        userId = request.headers.get("FIREBASEID")
        dateTo = request.args.get('dateTo')
        dateFrom = request.args.get('dateFrom')
        self.logger.info(f"Reading email for user {userId}")
        self.TransactionService.readTransactionFromMail(dateTo=dateTo, dateFrom=dateFrom, userID=userId)
        return jsonify({"Message": "Success"}), 200

    @Logger.standardLogger
    def triggerStatementCheck(self):
        userId = request.headers.get("FIREBASEID")
        dateTo = request.args.get('dateTo')
        dateFrom = request.args.get('dateFrom')
        bank = request.args.get('bank')
        self.logger.info(f"Reading statements for user {userId}")
        self.TransactionService.readStatementsFromMail(dateTo=dateTo, dateFrom=dateFrom, userID=userId, bank=bank)
        return jsonify({"Message": "Success"}), 200

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

        # Ensure all required fields are present
        required_fields = ['user_id', 'access_token', 'refresh_token', 'client_id', 'client_secret', 'expiry',
                           'service_type']
        missing_fields = [field for field in required_fields if field not in data]
        if missing_fields:
            return jsonify({"error": f"Missing required fields: {', '.join(missing_fields)}"}), 400
        # Call the service function to add or update the token
        result = self.TransactionService.addUpdateUserToken(data)
        return jsonify(result), 200

    @Logger.standardLogger
    def deleteFile(self):
        data = request.get_json()

        # Ensure all required fields are present
        required_fields = ['user_id', 'fileId']
        missing_fields = [field for field in required_fields if field not in data]
        if missing_fields:
            return jsonify({"error": f"Missing required fields: {', '.join(missing_fields)}"}), 400
        # Call the service function to add or update the token
        result = self.TransactionService.deleteFile(data['userId'], data['fileId'])
        return jsonify(result), 200

    @Logger.standardLogger
    def renameFile(self):
        data = request.get_json()

        # Ensure all required fields are present
        required_fields = ['user_id', 'fileId', 'newName']
        missing_fields = [field for field in required_fields if field not in data]
        if missing_fields:
            return jsonify({"error": f"Missing required fields: {', '.join(missing_fields)}"}), 400
        # Call the service function to add or update the token
        result = self.TransactionService.renameFile(data['userId'], data['fileId'], data['newName'])
        return jsonify(result), 200

    @Logger.standardLogger
    def downloadFile(self):
        data = request.get_json()

        # Ensure all required fields are present
        required_fields = ['userId', 'fileId']
        missing_fields = [field for field in required_fields if field not in data]
        if missing_fields:
            return jsonify({"error": f"Missing required fields: {', '.join(missing_fields)}"}), 400
        # Call the service function to add or update the token
        result = self.TransactionService.downloadFile(data['userId'], data['fileId'])
        return jsonify(result), 200

    @Logger.standardLogger
    def getFileDetails(self):
        data = request.get_json()

        # Ensure all required fields are present
        required_fields = ['userId']
        missing_fields = [field for field in required_fields if field not in data]
        if missing_fields:
            return jsonify({"error": f"Missing required fields: {', '.join(missing_fields)}"}), 400
        # Call the service function to add or update the token
        result = self.TransactionService.getFileDetails(data['userId'])
        return jsonify(result), 200

    @Logger.standardLogger
    def checkGoogleApiStatus(self):
        userId = request.args.get('userId')
        service = request.args.get('serviceType')
        if userId is None:
            return jsonify({"error": f"Missing required fields: {', '.join('user_id')}"}), 400
        # Call the service function to add or update the token
        result = self.TransactionService.checkGoogleStatus(userId, ServiceTypeEnum[service])
        return jsonify(result), 200
