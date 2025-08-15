from flask import request, jsonify, g
from services.customFieldService import CustomFieldService
from utils.logger import Logger


class CustomFieldController:
    def __init__(self, custom_field_service: CustomFieldService):
        self.custom_field_service = custom_field_service
        self.logger = Logger(__name__).get_logger()

    def get_custom_fields(self, invoiceId):
        try:
            result = self.custom_field_service.get_custom_fields(invoiceId)
            return jsonify({"customFields": result}), 200

        except ValueError as e:
            self.logger.error(f"Validation error fetching custom fields: {str(e)}")
            return jsonify({"error": str(e)}), 400
        except Exception as e:
            self.logger.error(f"Error fetching custom fields: {str(e)}")
            return jsonify({"error": "Internal server error"}), 500

    def add_custom_field(self, invoiceId):
        try:
            field_data = request.get_json()
            if not field_data:
                return jsonify({"error": "Custom field data is required"}), 400

            result = self.custom_field_service.add_custom_field(invoiceId, field_data)
            return jsonify({"customField": result}), 201

        except ValueError as e:
            self.logger.error(f"Validation error adding custom field: {str(e)}")
            return jsonify({"error": str(e)}), 400
        except Exception as e:
            self.logger.error(f"Error adding custom field: {str(e)}")
            return jsonify({"error": "Internal server error"}), 500

    def update_custom_field(self, invoiceId, fieldId):
        try:
            field_data = request.get_json()
            if not field_data:
                return jsonify({"error": "Custom field data is required"}), 400

            result = self.custom_field_service.update_custom_field(invoiceId, fieldId, field_data)
            return jsonify({"customField": result}), 200

        except ValueError as e:
            self.logger.error(f"Validation error updating custom field: {str(e)}")
            return jsonify({"error": str(e)}), 400
        except Exception as e:
            self.logger.error(f"Error updating custom field: {str(e)}")
            return jsonify({"error": "Internal server error"}), 500

    def delete_custom_field(self, invoiceId, fieldId):
        try:
            result = self.custom_field_service.delete_custom_field(invoiceId, fieldId)
            return jsonify({"success": result}), 200

        except ValueError as e:
            self.logger.error(f"Validation error deleting custom field: {str(e)}")
            return jsonify({"error": str(e)}), 400
        except Exception as e:
            self.logger.error(f"Error deleting custom field: {str(e)}")
            return jsonify({"error": "Internal server error"}), 500
