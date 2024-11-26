import os

from werkzeug.utils import secure_filename

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
        if not service_type_param or service_type_param not in MSNENUM.__members__:
            return jsonify({"error": "Invalid or missing serviceType parameter"}), 400

        service_type = MSNENUM[service_type_param]
        return user_id, service_type

    @Logger.standardLogger
    def fetchSecurityList(self):
        # Get user ID from context
        service_type_param = request.args.get('serviceType')
        params = self.getUserIdServiceType(service_type_param)
        if not isinstance(params, tuple):
            # Error has happened during validation
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
        return self.InvestmentService.fetchSecuritySchemeRate(service_type, schemeCode)

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
        tmp_folder = os.path.join(os.getcwd(), '/tmp')
        os.makedirs(tmp_folder, exist_ok=True)
        file_path = os.path.join(tmp_folder, secure_filename(file.filename))
        file.save(file_path)

        self.logger.info(f"File saved to: {file_path}")

        self.InvestmentService.processFilesForMSN(service_type, file_path, user_id)

        return jsonify({"message": "File processed successfully"}), 200
