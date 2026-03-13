from flask import request, jsonify, g
from services.customerEmailService import CustomerEmailService
from utils.logger import Logger


class CustomerEmailController:
    def __init__(self, customer_email_service):
        self.customer_email_service = customer_email_service
        self.logger = Logger(__name__).get_logger()

    @Logger.standardLogger
    def get_customer_emails(self, customerId):
        try:
            if not customerId:
                return jsonify({"error": "Customer ID is required"}), 400

            emails = self.customer_email_service.get_emails_for_customer(customerId)
            return jsonify({"emails": emails}), 200

        except ValueError as e:
            return jsonify({"error": str(e)}), 404
        except Exception as e:
            self.logger.error(f"Error in get_customer_emails: {str(e)}")
            return jsonify({"error": "Internal server error"}), 500

    @Logger.standardLogger
    def link_email(self, customerId):
        try:
            if not customerId:
                return jsonify({"error": "Customer ID is required"}), 400

            data = request.get_json(force=True)
            email_id = data.get('email_id')
            if not email_id:
                return jsonify({"error": "email_id is required"}), 400

            link = self.customer_email_service.link_email(customerId, email_id)
            return jsonify({"link": link}), 201

        except ValueError as e:
            return jsonify({"error": str(e)}), 400
        except Exception as e:
            self.logger.error(f"Error in link_email: {str(e)}")
            return jsonify({"error": "Internal server error"}), 500

    @Logger.standardLogger
    def unlink_email(self, customerId, emailId):
        try:
            if not customerId or not emailId:
                return jsonify({"error": "Customer ID and Email ID are required"}), 400

            self.customer_email_service.unlink_email(customerId, int(emailId))
            return jsonify({"message": "Email unlinked successfully"}), 200

        except ValueError as e:
            return jsonify({"error": str(e)}), 404
        except Exception as e:
            self.logger.error(f"Error in unlink_email: {str(e)}")
            return jsonify({"error": "Internal server error"}), 500

    @Logger.standardLogger
    def search_emails(self):
        try:
            query = request.args.get('q', '')
            if not query or len(query) < 2:
                return jsonify({"error": "Search query must be at least 2 characters"}), 400

            emails = self.customer_email_service.search_emails(query)
            return jsonify({"emails": emails}), 200

        except Exception as e:
            self.logger.error(f"Error in search_emails: {str(e)}")
            return jsonify({"error": "Internal server error"}), 500

    @Logger.standardLogger
    def batch_relink(self):
        """Re-scan all processed emails and auto-link to all customers."""
        try:
            user_id = g.get('firebase_id')
            if not user_id:
                return jsonify({"error": "User ID not found"}), 400

            from models.freelance_management import Customer
            from services.Base_Service import BaseService
            db = BaseService().db

            customers = db.session.query(Customer).filter(
                Customer.user_id == user_id,
                Customer.email.isnot(None),
            ).all()

            total_linked = 0
            for customer in customers:
                count = self.customer_email_service.batch_auto_link_for_customer(customer.id, user_id)
                total_linked += count

            return jsonify({"message": f"Linked {total_linked} emails across {len(customers)} customers", "linked": total_linked}), 200

        except Exception as e:
            self.logger.error(f"Error in batch_relink: {str(e)}")
            return jsonify({"error": "Internal server error"}), 500
