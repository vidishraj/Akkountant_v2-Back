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
        except KeyError:
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
            tmp_folder = os.path.join(os.getcwd(), 'tmp')
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
        return self.InvestmentService.fetchActiveSecurities(service_type, user_id)

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
            required_keysMF = ["date", "amount", "quantity", "schemeCode"]
            # Ensure all required fields are present
            if service_type != MSNENUM.Mutual_Funds:
                missing_keys = [key for key in required_keys if key not in data]
            else:
                missing_keys = [key for key in required_keysMF if key not in data]

            if missing_keys:
                return None, (jsonify({"error": f"Missing keys: {', '.join(missing_keys)}"}), 400)

            # Validate 'date' field
            try:
                if not data["date"]:
                    return None, (jsonify({"error": "Date cannot be empty"}), 400)
                datetime.strptime(data["date"], "%d-%m-%Y")
            except (ValueError, TypeError):
                return None, (jsonify({"error": "Invalid date format. Use dd-mm-YYYY"}), 400)

            # Validate 'deposit' and 'amount' fields
            if service_type != MSNENUM.Mutual_Funds:
                if not isinstance(data["description"], str) or not data["description"]:
                    return None, (jsonify({"error": "Invalid 'description'. It must be a non-empty string"}), 400)
                if not isinstance(data["amount"], (int, float)) or data["amount"] <= 0:
                    return None, (jsonify({"error": "Invalid 'amount'. It must be a positive number"}), 400)

            # All validations passed
            return data, None

        except Exception as e:
            return None, (jsonify({"error": f"Unexpected error: {str(e)}"}), 500)

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
    def fetchRealizedPnL(self):
        """Fetch realized profit/loss and historical closed trades"""
        service_type_param = request.args.get('serviceType')
        params = self.getUserIdServiceType(service_type_param)
        if not isinstance(params, tuple):
            return params
        user_id, service_type = params
        self.logger.info(f"userID: {user_id}, investmentType: {service_type.value}")
        return jsonify(self.InvestmentService.fetchRealizedPnL(service_type.value, user_id)), 200

    @Logger.standardLogger
    def fetchFOSummary(self):
        """Fetch F&O P&L summary"""
        user_id = g.get('firebase_id')
        if not user_id:
            return jsonify({"error": "User ID not found"}), 400
        return jsonify(self.InvestmentService.fetchFOSummary(user_id)), 200

    @Logger.standardLogger
    def fetchFOTrades(self):
        """Fetch all individual F&O trades"""
        user_id = g.get('firebase_id')
        if not user_id:
            return jsonify({"error": "User ID not found"}), 400
        return jsonify(self.InvestmentService.fetchFOTrades(user_id)), 200

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
            return jsonify({"Error": "BuyID is missing"}), 406
        # Validate the request JSON body
        self.logger.info(f"userID: {user_id}, investmentType: {service_type.value}")
        return self.InvestmentService.deleteSingleRecord(service_type, buyId)

    @Logger.standardLogger
    def getJobsTable(self):
        # Get page and filter/sort parameters
        page = request.args.get('page')
        if page is None:
            return jsonify({"Error": "Page is missing"}), 406
        
        # Extract filter parameters
        filters = {
            'title': request.args.get('title'),
            'status': request.args.get('status'),
            'priority': request.args.get('priority'),
            'user_id': request.args.get('user_id'),
            'min_failures': request.args.get('min_failures'),
            'max_failures': request.args.get('max_failures')
        }
        
        # Extract sort parameters
        sort_by = request.args.get('sort_by', 'due_date')  # Default sort by due_date
        sort_order = request.args.get('sort_order', 'desc')  # Default descending
        
        return jsonify(self.InvestmentService.getJobsTable(page, filters, sort_by, sort_order)), 200

    @Logger.standardLogger
    def setJobs(self):
        # Get user ID from context
        jobId = request.args.get('jobId')
        userId = request.headers.get("X-Firebase-ID")
        if jobId is None:
            return jsonify({"Error": "JobID is missing"}), 406
        return self.InvestmentService.setJobsTable(jobId, userId)

    @Logger.standardLogger
    def fetchTimeStamps(self):
        return jsonify(self.InvestmentService.getFileTimeStamps()), 200

    @Logger.standardLogger
    def fetchInvestmentEmails(self):
        user_id = g.get('firebase_id')
        if not user_id:
            return jsonify({"error": "User ID not found"}), 400
        category = request.args.get('category')
        service_type = request.args.get('serviceType')
        page = int(request.args.get('page', 1))
        page_size = int(request.args.get('pageSize', 50))
        categories_param = request.args.get('categories')
        categories = categories_param.split(',') if categories_param else None
        return jsonify(self.InvestmentService.fetchInvestmentEmails(user_id, category, page, page_size, service_type, categories)), 200

    @Logger.standardLogger
    def fetchInvestmentSnapshots(self):
        """Fetch historical investment snapshots for growth charts."""
        user_id = g.get('firebase_id')
        if not user_id:
            return jsonify({"error": "User ID not found"}), 400
        date_from = request.args.get('dateFrom')
        date_to = request.args.get('dateTo')
        investment_type = request.args.get('investmentType')
        result = self.InvestmentService.getInvestmentSnapshots(
            user_id, date_from, date_to, investment_type
        )
        return jsonify({"snapshots": result}), 200

    @Logger.standardLogger
    def fetchEmailBody(self):
        user_id = g.get('firebase_id')
        if not user_id:
            return jsonify({"error": "User ID not found"}), 400
        gmail_id = request.args.get('gmailId')
        if not gmail_id:
            return jsonify({"error": "gmailId parameter is required"}), 400
        try:
            result = self.InvestmentService.fetchEmailBody(user_id, gmail_id)
            if isinstance(result, tuple):
                return jsonify(result[0]), result[1]
            return jsonify(result), 200
        except Exception as e:
            self.logger.error(f"Error fetching email body: {str(e)}")
            return jsonify({"error": "Failed to fetch email body"}), 500

    @Logger.standardLogger
    def fetchKiteHoldings(self):
        """Fetch holdings from Kite Connect API"""
        user_id = g.get('firebase_id')
        if not user_id:
            return jsonify({"error": "User ID not found"}), 400
        
        try:
            holdings = self.InvestmentService.fetchKiteHoldings(user_id)
            return jsonify({"holdings": holdings}), 200
        except Exception as e:
            self.logger.error(f"Error fetching Kite holdings: {str(e)}")
            return jsonify({"error": "Failed to fetch holdings"}), 500

    @Logger.standardLogger
    def fetchKitePositions(self):
        """Fetch positions from Kite Connect API"""
        user_id = g.get('firebase_id')
        if not user_id:
            return jsonify({"error": "User ID not found"}), 400
        
        try:
            positions = self.InvestmentService.fetchKitePositions(user_id)
            return jsonify({"positions": positions}), 200
        except Exception as e:
            self.logger.error(f"Error fetching Kite positions: {str(e)}")
            return jsonify({"error": "Failed to fetch positions"}), 500

    @Logger.standardLogger
    def syncKiteHoldings(self):
        """Sync Kite holdings to local database"""
        user_id = g.get('firebase_id')
        if not user_id:
            return jsonify({"error": "User ID not found"}), 400
        
        try:
            result = self.InvestmentService.syncKiteHoldings(user_id)
            return jsonify({
                "message": "Holdings synced successfully", 
                "result": result
            }), 200
        except Exception as e:
            self.logger.error(f"Error syncing Kite holdings: {str(e)}")
            return jsonify({"error": "Failed to sync holdings"}), 500

    @Logger.standardLogger
    def getKiteLoginUrl(self):
        """Get Kite Connect login URL for frontend"""
        try:
            login_url = self.InvestmentService.getKiteLoginUrl()
            return jsonify({"login_url": login_url}), 200
        except Exception as e:
            self.logger.error(f"Error getting Kite login URL: {str(e)}")
            return jsonify({"error": "Failed to get login URL"}), 500

    @Logger.standardLogger
    def generateKiteSession(self):
        """Generate Kite session using request token from frontend"""
        user_id = g.get('firebase_id')
        if not user_id:
            return jsonify({"error": "User ID not found"}), 400

        data = request.get_json()
        if not data or 'request_token' not in data:
            return jsonify({"error": "Request token is required"}), 400

        request_token = data['request_token']
        
        try:
            session_data = self.InvestmentService.generateKiteSession(user_id, request_token)
            return jsonify({
                "message": "Session generated successfully",
                "session_data": session_data
            }), 200
        except Exception as e:
            self.logger.error(f"Error generating Kite session: {str(e)}")
            return jsonify({"error": "Failed to generate session"}), 500
