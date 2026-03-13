from flask import request, jsonify
from services.signatureService import SignatureService
from utils.logger import Logger


class SignatureController:
    def __init__(self, signature_service):
        self.signature_service = signature_service
        self.logger = Logger(__name__).get_logger()

    @staticmethod
    def _format_signature(signature, include_data=False):
        """Format a Signature ORM object to a JSON-serializable dict."""
        result = {
            "id": signature.id,
            "name": signature.name,
            "signature_type": signature.signature_type,
            "is_default": signature.is_default,
            "user_id": signature.user_id,
            "created_at": signature.created_at.strftime('%Y-%m-%dT%H:%M:%S.%fZ') if signature.created_at else "",
            "updated_at": signature.updated_at.strftime('%Y-%m-%dT%H:%M:%S.%fZ') if signature.updated_at else "",
        }
        if include_data:
            result["signature_data"] = signature.signature_data
        return result

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
            result = self._format_signature(signature, include_data=False)

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
            result = self._format_signature(signature, include_data=False)

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
            results = [self._format_signature(sig, include_data=True) for sig in signatures]

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
            result = self._format_signature(signature, include_data=True)

            return jsonify({"signature": result}), 200

        except ValueError as e:
            return jsonify({"error": str(e)}), 404
        except Exception as e:
            self.logger.error(f"Error in get_signature_data: {str(e)}")
            return jsonify({"error": "Internal server error"}), 500

    @Logger.standardLogger
    def set_default_signature(self, signatureId):
        try:
            if not signatureId:
                return jsonify({"error": "Signature ID is required"}), 400

            signature = self.signature_service.set_default_signature(signatureId)
            result = self._format_signature(signature, include_data=False)

            return jsonify({"signature": result}), 200

        except ValueError as e:
            return jsonify({"error": str(e)}), 404
        except Exception as e:
            self.logger.error(f"Error in set_default_signature: {str(e)}")
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