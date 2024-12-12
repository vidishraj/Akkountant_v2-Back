import os
from datetime import datetime

from werkzeug.utils import secure_filename

from enums.EPGEnum import EPGEnum
from enums.MsnEnum import MSNENUM
from services.InvestmentService import InvestmentService
from utils.logger import Logger
from flask import request, jsonify, g


class InvestmentController:
    InvestmentService: InvestmentService

    def __init__(self, investmentService):
        self.InvestmentService = investmentService
        self.logger = Logger(__name__).get_logger()

    @staticmethod
    def getUserIdServiceType(service_type_param):
        user_id = g.get('firebase_id')
        if not user_id:
            return jsonify({"error": "User ID not found"}), 400
        # Validate and get the serviceType from query parameter
        if not service_type_param or (service_type_param not in MSNENUM.__members__ and
                                      service_type_param not in EPGEnum.__members__):
            return jsonify({"error": "Invalid or missing serviceType parameter"}), 400
        try:
            service_type = MSNENUM[service_type_param]
        except:
            service_type = EPGEnum[service_type_param]
        return user_id, service_type

    @Logger.standardLogger
    def fetchSecurityList(self):
        # Get user ID from context
        service_type_param = request.args.get('serviceType')
        params = self.getUserIdServiceType(service_type_param)
        if not isinstance(params, tuple):
            # Error during validation
            return params
        user_id, service_type = params
        self.logger.info(f"userID: {user_id}, investmentType: {service_type.value}")
        return self.InvestmentService.fetchAllSecurities(service_type)

    @Logger.standardLogger
    def fetchSecurityRate(self):
        # Get user ID from context
        service_type_param = request.args.get('serviceType')
        schemeCode = request.args.get('schemeCode')
        params = self.getUserIdServiceType(service_type_param)
        if not schemeCode:
            return jsonify({"error": "Scheme code missing"}), 400
        if not isinstance(params, tuple):
            # Error has happened during validation
            return params
        user_id, service_type = params
        self.logger.info(f"userID: {user_id}, investmentType: {service_type.value}, scheme: {schemeCode}")
        return self.InvestmentService.fetchSecuritySchemeRate(service_type.value, schemeCode)

    @Logger.standardLogger
    def process_file_upload(self):
        # Get user ID from context
        service_type_param = request.args.get('serviceType')
        params = self.getUserIdServiceType(service_type_param)
        if not isinstance(params, tuple):
            # Error has happened during validation
            return params
        user_id, service_type = params
        self.logger.info(f"userID: {user_id}, investmentType: {service_type.value}")

        # Check if a file is in the request
        if 'file' not in request.files:
            return jsonify({"error": "No file provided"}), 400

        file = request.files['file']
        if file.filename == '':
            return jsonify({"error": "No file selected"}), 400

        # Save the file to the /tmp directory
        file_path = None
        try:
            tmp_folder = os.path.join(os.getcwd(), '/tmp')
            os.makedirs(tmp_folder, exist_ok=True)
            file_path = os.path.join(tmp_folder, secure_filename(file.filename))
            file.save(file_path)

            self.logger.info(f"File saved to: {file_path}")

            insertedDetails = self.InvestmentService.processFiles(service_type, file_path, user_id)
        finally:
            if file_path is not None:
                self.logger.info("Temp file deleted successfully.")
                os.remove(file_path)
        return jsonify({"Details": insertedDetails}), 200

    @Logger.standardLogger
    def fetchSummary(self):
        # Get user ID from context
        service_type_param = request.args.get('serviceType')
        params = self.getUserIdServiceType(service_type_param)
        if not isinstance(params, tuple):
            # Error has happened during validation
            return params
        user_id, service_type = params
        self.logger.info(f"userID: {user_id}, investmentType: {service_type.value}")
        return self.InvestmentService.fetchSummary(service_type.value, user_id)

    @Logger.standardLogger
    def fetchSecurityTransactions(self):
        # Get user ID from context
        service_type_param = request.args.get('serviceType')
        params = self.getUserIdServiceType(service_type_param)
        if not isinstance(params, tuple):
            # Error has happened during validation
            return params
        user_id, service_type = params
        self.logger.info(f"userID: {user_id}, investmentType: {service_type.value}")
        return jsonify(self.InvestmentService.fetchSecurityTransactions(service_type.value, user_id)), 200

    @Logger.standardLogger
    def fetchUserSecurities(self):
        # Get user ID from context
        service_type_param = request.args.get('serviceType')
        params = self.getUserIdServiceType(service_type_param)
        if not isinstance(params, tuple):
            # Error has happened during validation
            return params
        user_id, service_type = params
        self.logger.info(f"userID: {user_id}, investmentType: {service_type.value}")
        return self.InvestmentService.fetchUserSecurities(service_type.value, user_id)

    @Logger.standardLogger
    def fetchHistory(self):
        # Get user ID from context
        service_type_param = request.args.get('serviceType')
        params = self.getUserIdServiceType(service_type_param)
        if not isinstance(params, tuple):
            # Error has happened during validation
            return params
        user_id, service_type = params
        self.logger.info(f"userID: {user_id}, investmentType: {service_type.value}")
        return self.InvestmentService.fetchHistory(service_type, user_id)

    @Logger.standardLogger
    def fetchActiveSecurities(self):
        # Get user ID from context
        service_type_param = request.args.get('serviceType')
        params = self.getUserIdServiceType(service_type_param)
        if not isinstance(params, tuple):
            # Error has happened during validation
            return params
        user_id, service_type = params
        self.logger.info(f"userID: {user_id}, investmentType: {service_type.value}")
        return self.InvestmentService.fetchActiveSecurities(service_type, user_id)

    @Logger.standardLogger
    def insertSecurityTransaction(self):
        # Get user ID from context
        service_type_param = request.args.get('serviceType')
        params = self.getUserIdServiceType(service_type_param)

        if not isinstance(params, tuple):
            # Error has happened during validation
            return params
        user_id, service_type = params
        # Validate the request JSON body
        self.logger.info(f"userID: {user_id}, investmentType: {service_type.value}")
        data, error = self.validate_security_transaction(service_type)
        if error:
            return error  # Return the validation error response
        return self.InvestmentService.insertSecurityPurchase(service_type, user_id, data)

    @Logger.standardLogger
    def fetchCompleteDataForEPG(self):
        # Get user ID from context
        service_type_param = request.args.get('serviceType')
        params = self.getUserIdServiceType(service_type_param)

        if not isinstance(params, tuple):
            # Error has happened during validation
            return params
        user_id, service_type = params
        # Validate the request JSON body
        self.logger.info(f"userID: {user_id}, investmentType: {service_type.value}")
        return self.InvestmentService.fetchCompleteDataForEPG(service_type, user_id)

    @staticmethod
    def validate_security_transaction(service_type):
        """
        Validates the JSON body for the insertSecurityTransaction request.

        Args:
            service_type (str): The service type, e.g., 'Mutual_Funds'.

        Returns:
            tuple: A tuple with (validated_data, error_message).
                   If valid, error_message will be None, else it will contain the error details.
        """
        try:
            # Parse JSON body
            data = request.get_json()

            # Check if the JSON body is provided
            if not data:
                return None, (jsonify({"error": "Request body must be JSON"}), 400)

            # Required keys
            required_keys = ["date", "description", "amount"]

            # Ensure all required fields are present
            missing_keys = [key for key in required_keys if key not in data]
            if missing_keys:
                return None, (jsonify({"error": f"Missing keys: {', '.join(missing_keys)}"}), 400)

            # Validate 'date' field
            try:
                datetime.strptime(data["date"], "%d-%m-%Y")
            except ValueError:
                return None, (jsonify({"error": "Invalid date format. Use dd/mm/YYYY"}), 400)

            # Validate 'deposit' and 'amount' fields
            if not isinstance(data["description"], str) or not data["description"]:
                return None, (jsonify({"error": "Invalid 'description'. It must be a non-empty string"}), 400)
            if not isinstance(data["amount"], (int, float)) or data["amount"] <= 0:
                return None, (jsonify({"error": "Invalid 'amount'. It must be a positive number"}), 400)

            # Additional check for 'Mutual_Funds' service type
            if service_type == "Mutual_Funds" and "buyID" not in data:
                return None, (jsonify({"error": "'buyID' is required for Mutual_Funds service type"}), 400)

            # All validations passed
            return data, None

        except Exception as e:
            return None, jsonify({"error": f"Unexpected error: {str(e)}"}), 500

    @Logger.standardLogger
    def fetchRateForEPG(self):
        # Get user ID from context
        service_type_param = request.args.get('serviceType')
        params = self.getUserIdServiceType(service_type_param)

        if not isinstance(params, tuple):
            # Error has happened during validation
            return params
        user_id, service_type = params
        # Validate the request JSON body
        self.logger.info(f"userID: {user_id}, investmentType: {service_type.value}")
        return self.InvestmentService.fetchRateForEPG(service_type)

    @Logger.standardLogger
    def deleteAllInvestments(self):
        # Get user ID from context
        service_type_param = request.args.get('serviceType')
        params = self.getUserIdServiceType(service_type_param)
        if not isinstance(params, tuple):
            # Error has happened during validation
            return params
        user_id, service_type = params
        # Validate the request JSON body
        self.logger.info(f"userID: {user_id}, investmentType: {service_type.value}")
        return self.InvestmentService.deleteAll(service_type, user_id)

    @Logger.standardLogger
    def deleteSingleRecord(self):
        # Get user ID from context
        service_type_param = request.args.get('serviceType')
        params = self.getUserIdServiceType(service_type_param)

        if not isinstance(params, tuple):
            # Error has happened during validation
            return params
        user_id, service_type = params
        buyId = request.args.get('buyId')
        if buyId is None:
            return jsonify({"Error":"BuyID is missing"}), 406
        # Validate the request JSON body
        self.logger.info(f"userID: {user_id}, investmentType: {service_type.value}")
        return self.InvestmentService.deleteSingleRecord(service_type, buyId)
