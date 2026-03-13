from flask import request, jsonify
from services.customerService import CustomerService
from utils.logger import Logger


class CustomerController:
    def __init__(self, customer_service):
        self.customer_service = customer_service
        self.logger = Logger(__name__).get_logger()

    @Logger.standardLogger
    def create_customer(self):
        try:
            data = request.get_json(force=True)
            
            if not data.get('name'):
                return jsonify({"error": "Name is required"}), 400

            customer = self.customer_service.create_customer(data)
            
            return jsonify({"customer": customer}), 201  # Already formatted in service

        except ValueError as e:
            return jsonify({"error": str(e)}), 400
        except Exception as e:
            self.logger.error(f"Error in create_customer: {str(e)}")
            return jsonify({"error": "Internal server error"}), 500

    @Logger.standardLogger
    def delete_customer(self, customerId):
        try:
            if not customerId:
                return jsonify({"error": "Customer ID is required"}), 400

            self.customer_service.delete_customer(customerId)
            return jsonify({"message": "Customer deleted successfully"}), 200

        except ValueError as e:
            return jsonify({"error": str(e)}), 404
        except Exception as e:
            self.logger.error(f"Error in delete_customer: {str(e)}")
            return jsonify({"error": "Internal server error"}), 500

    @Logger.standardLogger
    def get_customers(self):
        try:
            page = int(request.args.get("page", 1))
            limit = int(request.args.get("limit", 20))

            if page < 1 or limit < 1 or limit > 100:
                return jsonify({"error": "Invalid pagination parameters"}), 400

            result = self.customer_service.get_customers(page, limit)

            response = {
                "customers": result["customers"],  # Already formatted in service
                "total_count": result["total_count"],
                "page": result["page"],
                "page_size": result["page_size"]
            }

            return jsonify(response), 200

        except Exception as e:
            self.logger.error(f"Error in get_customers: {str(e)}")
            return jsonify({"error": "Internal server error"}), 500

    @Logger.standardLogger
    def get_customer(self, customerId):
        try:
            if not customerId:
                return jsonify({"error": "Customer ID is required"}), 400

            customer = self.customer_service.get_customer_by_id(customerId)
            return jsonify({"customer": customer}), 200

        except ValueError as e:
            return jsonify({"error": str(e)}), 404
        except Exception as e:
            self.logger.error(f"Error in get_customer: {str(e)}")
            return jsonify({"error": "Internal server error"}), 500

    @Logger.standardLogger
    def update_customer(self, customerId):
        try:
            if not customerId:
                return jsonify({"error": "Customer ID is required"}), 400

            data = request.get_json(force=True)
            customer = self.customer_service.update_customer(customerId, data)
            
            return jsonify({"customer": customer}), 200  # Already formatted in service

        except ValueError as e:
            return jsonify({"error": str(e)}), 404
        except Exception as e:
            self.logger.error(f"Error in update_customer: {str(e)}")
            return jsonify({"error": "Internal server error"}), 500