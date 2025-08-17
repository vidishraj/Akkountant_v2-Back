from flask import request, jsonify
from services.templateService import TemplateService
from utils.logger import Logger


class TemplateController:
    def __init__(self, template_service):
        self.template_service = template_service
        self.logger = Logger(__name__).get_logger()

    @Logger.standardLogger
    def create_template(self):
        try:
            data = request.get_json(force=True)
            
            required_fields = ['name', 'templateData']
            if not all(field in data for field in required_fields):
                return jsonify({"error": "Name and templateData are required"}), 400

            template = self.template_service.create_template(data)
            
            return jsonify({"template": template}), 201  # Already formatted in service

        except ValueError as e:
            return jsonify({"error": str(e)}), 400
        except Exception as e:
            self.logger.error(f"Error in create_template: {str(e)}")
            return jsonify({"error": "Internal server error"}), 500

    @Logger.standardLogger
    def get_templates(self):
        try:
            templates = self.template_service.get_templates()
            
            return jsonify({"templates": templates}), 200  # Already formatted in service

        except Exception as e:
            self.logger.error(f"Error in get_templates: {str(e)}")
            return jsonify({"error": "Internal server error"}), 500

    @Logger.standardLogger
    def update_template(self, templateId):
        try:
            if not templateId:
                return jsonify({"error": "Template ID is required"}), 400

            data = request.get_json(force=True)
            template = self.template_service.update_template(templateId, data)
            
            return jsonify({"template": template}), 200  # Already formatted in service

        except ValueError as e:
            return jsonify({"error": str(e)}), 404
        except Exception as e:
            self.logger.error(f"Error in update_template: {str(e)}")
            return jsonify({"error": "Internal server error"}), 500

    @Logger.standardLogger
    def delete_template(self, templateId):
        try:
            if not templateId:
                return jsonify({"error": "Template ID is required"}), 400

            self.template_service.delete_template(templateId)
            return jsonify({"message": "Template deleted successfully"}), 200

        except ValueError as e:
            return jsonify({"error": str(e)}), 404
        except Exception as e:
            self.logger.error(f"Error in delete_template: {str(e)}")
            return jsonify({"error": "Internal server error"}), 500

    @Logger.standardLogger
    def update_customer_template(self, customerId):
        try:
            if not customerId:
                return jsonify({"error": "Customer ID is required"}), 400

            data = request.get_json(force=True)
            
            # Handle different request formats
            template_data = None
            if 'template' in data:
                # New format: {"template": {...}}
                template_data = data['template']
            elif 'templateData' in data:
                # Legacy format: {"templateData": {...}}
                template_data = data['templateData']
            elif 'template_data' in data:
                # Snake case format: {"template_data": {...}}
                template_data = data['template_data']
            else:
                return jsonify({"error": "template data is required"}), 400

            # Prepare the data for the service
            service_data = {
                'templateData': template_data,
                'customerId': customerId,
                'name': data.get('name')  # Pass name if provided
            }

            template = self.template_service.update_customer_default_template(customerId, service_data)
            
            return jsonify({"template": template}), 200  # Already formatted in service

        except ValueError as e:
            return jsonify({"error": str(e)}), 404
        except Exception as e:
            self.logger.error(f"Error in update_customer_template: {str(e)}")
            return jsonify({"error": "Internal server error"}), 500