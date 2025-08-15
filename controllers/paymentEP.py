from flask import request, jsonify, g
from services.paymentService import PaymentService
from utils.logger import Logger


class PaymentController:
    def __init__(self, payment_service: PaymentService):
        self.payment_service = payment_service
        self.logger = Logger(__name__).get_logger()

    def add_payment(self, invoiceId):
        try:
            payment_data = request.get_json()
            if not payment_data:
                return jsonify({"error": "Payment data is required"}), 400

            result = self.payment_service.add_payment(invoiceId, payment_data)
            return jsonify({"payment": result}), 201

        except ValueError as e:
            self.logger.error(f"Validation error adding payment: {str(e)}")
            return jsonify({"error": str(e)}), 400
        except Exception as e:
            self.logger.error(f"Error adding payment: {str(e)}")
            return jsonify({"error": "Internal server error"}), 500

    def update_payment(self, invoiceId, paymentId):
        try:
            payment_data = request.get_json()
            if not payment_data:
                return jsonify({"error": "Payment data is required"}), 400

            result = self.payment_service.update_payment(invoiceId, paymentId, payment_data)
            return jsonify({"payment": result}), 200

        except ValueError as e:
            self.logger.error(f"Validation error updating payment: {str(e)}")
            return jsonify({"error": str(e)}), 400
        except Exception as e:
            self.logger.error(f"Error updating payment: {str(e)}")
            return jsonify({"error": "Internal server error"}), 500

    def delete_payment(self, invoiceId, paymentId):
        try:
            result = self.payment_service.delete_payment(invoiceId, paymentId)
            return jsonify({"success": result}), 200

        except ValueError as e:
            self.logger.error(f"Validation error deleting payment: {str(e)}")
            return jsonify({"error": str(e)}), 400
        except Exception as e:
            self.logger.error(f"Error deleting payment: {str(e)}")
            return jsonify({"error": "Internal server error"}), 500

    def replace_payment(self, invoiceId):
        try:
            payment_data = request.get_json()
            if not payment_data:
                return jsonify({"error": "Payment data is required"}), 400

            result = self.payment_service.replace_payment(invoiceId, payment_data)
            return jsonify({"payment": result}), 200

        except ValueError as e:
            self.logger.error(f"Validation error replacing payment: {str(e)}")
            return jsonify({"error": str(e)}), 400
        except Exception as e:
            self.logger.error(f"Error replacing payment: {str(e)}")
            return jsonify({"error": "Internal server error"}), 500
