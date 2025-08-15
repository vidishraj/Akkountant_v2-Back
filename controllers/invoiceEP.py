from flask import request, jsonify
from services.invoiceService import InvoiceService
from utils.logger import Logger


class InvoiceController:
    def __init__(self, invoice_service):
        self.invoice_service = invoice_service
        self.logger = Logger(__name__).get_logger()

    @Logger.standardLogger
    def create_invoice(self):
        try:
            data = request.get_json(force=True)
            
            # Handle camelCase field names and make from.email optional
            required_fields = ['invoiceNumber', 'projectName', 'issueDate', 'dueDate']
            if not all(field in data for field in required_fields):
                return jsonify({"error": "Missing required fields: invoiceNumber, projectName, issueDate, dueDate"}), 400
            
            # Validate from and to objects exist
            if 'from' not in data or 'to' not in data:
                return jsonify({"error": "from and to objects are required"}), 400
            
            # Validate required fields in from object (email is optional)
            from_required = ['name', 'address']
            if not all(field in data['from'] for field in from_required):
                return jsonify({"error": "from.name and from.address are required"}), 400
            
            # Validate required fields in to object
            to_required = ['name', 'address']
            if not all(field in data['to'] for field in to_required):
                return jsonify({"error": "to.name and to.address are required"}), 400

            invoice = self.invoice_service.create_invoice(data)
            
            return jsonify({"invoice": invoice}), 201  # Already formatted in service

        except ValueError as e:
            return jsonify({"error": str(e)}), 400
        except Exception as e:
            self.logger.error(f"Error in create_invoice: {str(e)}")
            return jsonify({"error": "Internal server error"}), 500

    @Logger.standardLogger
    def get_invoices(self):
        try:
            page = int(request.args.get("page", 1))
            limit = int(request.args.get("limit", 20))

            if page < 1 or limit < 1 or limit > 100:
                return jsonify({"error": "Invalid pagination parameters"}), 400

            result = self.invoice_service.get_invoices(page, limit)

            response = {
                "invoices": result["invoices"],  # Already formatted in service
                "total_count": result["total_count"],
                "page": result["page"],
                "page_size": result["page_size"]
            }

            return jsonify(response), 200

        except Exception as e:
            self.logger.error(f"Error in get_invoices: {str(e)}")
            return jsonify({"error": "Internal server error"}), 500

    @Logger.standardLogger
    def get_invoice_by_id(self, invoice_number):
        try:
            if not invoice_number:
                return jsonify({"error": "Invoice number is required"}), 400

            invoice = self.invoice_service.get_invoice_by_number(invoice_number)
            
            return jsonify({"invoice": invoice}), 200  # Already formatted in service

        except ValueError as e:
            return jsonify({"error": str(e)}), 404
        except Exception as e:
            self.logger.error(f"Error in get_invoice_by_id: {str(e)}")
            return jsonify({"error": "Internal server error"}), 500

    @Logger.standardLogger
    def update_invoice(self, invoiceId):
        try:
            if not invoiceId:
                return jsonify({"error": "Invoice number is required"}), 400

            data = request.get_json(force=True)
            invoice = self.invoice_service.update_invoice(invoiceId, data)
            
            return jsonify({"invoice": invoice}), 200  # Already formatted in service

        except ValueError as e:
            return jsonify({"error": str(e)}), 404
        except Exception as e:
            self.logger.error(f"Error in update_invoice: {str(e)}")
            return jsonify({"error": "Internal server error"}), 500

    @Logger.standardLogger
    def delete_invoice(self, invoiceId):
        try:
            if not invoiceId:
                return jsonify({"error": "Invoice number is required"}), 400

            self.invoice_service.delete_invoice(invoiceId)
            return jsonify({"message": "Invoice deleted successfully"}), 200

        except ValueError as e:
            return jsonify({"error": str(e)}), 404
        except Exception as e:
            self.logger.error(f"Error in delete_invoice: {str(e)}")
            return jsonify({"error": "Internal server error"}), 500