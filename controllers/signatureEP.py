from flask import request, jsonify
from services.signatureService import SignatureService
from utils.logger import Logger


class SignatureController:
    def __init__(self, signature_service):
        self.signature_service = signature_service
        self.logger = Logger(__name__).get_logger()

    @Logger.standardLogger
    def upload_signature(self):
        try:
            # Handle multipart form data
            if 'signature' not in request.files:
                return jsonify({"error": "No signature file provided"}), 400

            file = request.files['signature']
            name = request.form.get('name', 'Uploaded Signature')
            is_default = request.form.get('is_default', 'false').lower() == 'true'

            signature = self.signature_service.upload_signature_file(file, name, is_default)
            
            # Don't return signature_data in response for security
            result = {key: value for key, value in signature.__dict__.items() 
                     if key not in ['_sa_instance_state', 'signature_data']}
            
            return jsonify({"signature": result}), 201

        except ValueError as e:
            return jsonify({"error": str(e)}), 400
        except Exception as e:
            self.logger.error(f"Error in upload_signature: {str(e)}")
            return jsonify({"error": "Internal server error"}), 500

    @Logger.standardLogger
    def create_signature(self):
        try:
            data = request.get_json(force=True)
            
            required_fields = ['name', 'signature_data']
            if not all(field in data for field in required_fields):
                return jsonify({"error": "Name and signature_data are required"}), 400

            signature = self.signature_service.create_signature(
                data['signature_data'], 
                data['name'], 
                data.get('is_default', False)
            )
            
            # Don't return signature_data in response for security
            result = {key: value for key, value in signature.__dict__.items() 
                     if key not in ['_sa_instance_state', 'signature_data']}
            
            return jsonify({"signature": result}), 201

        except ValueError as e:
            return jsonify({"error": str(e)}), 400
        except Exception as e:
            self.logger.error(f"Error in create_signature: {str(e)}")
            return jsonify({"error": "Internal server error"}), 500

    @Logger.standardLogger
    def get_signatures(self):
        try:
            signatures = self.signature_service.get_signatures()
            
            # Include signature_data in response for frontend display
            results = [
                {key: value for key, value in signature.__dict__.items() 
                 if key not in ['_sa_instance_state']}
                for signature in signatures
            ]
            
            return jsonify(results), 200

        except Exception as e:
            self.logger.error(f"Error in get_signatures: {str(e)}")
            return jsonify({"error": "Internal server error"}), 500

    @Logger.standardLogger
    def get_signature_data(self, signatureId):
        try:
            if not signatureId:
                return jsonify({"error": "Signature ID is required"}), 400

            signature = self.signature_service.get_signature_by_id(signatureId)
            
            # Return full signature data including the base64 data
            result = {key: value for key, value in signature.__dict__.items() 
                     if key != '_sa_instance_state'}
            
            return jsonify({"signature": result}), 200

        except ValueError as e:
            return jsonify({"error": str(e)}), 404
        except Exception as e:
            self.logger.error(f"Error in get_signature_data: {str(e)}")
            return jsonify({"error": "Internal server error"}), 500

    @Logger.standardLogger
    def delete_signature(self, signatureId):
        try:
            if not signatureId:
                return jsonify({"error": "Signature ID is required"}), 400

            self.signature_service.delete_signature(signatureId)
            
            return jsonify({"message": "Signature deleted successfully"}), 200

        except ValueError as e:
            return jsonify({"error": str(e)}), 404
        except Exception as e:
            self.logger.error(f"Error in delete_signature: {str(e)}")
            return jsonify({"error": "Internal server error"}), 500