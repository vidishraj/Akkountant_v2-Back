from flask import request, jsonify, make_response
from services.pdfService import PDFService
from services.invoiceService import InvoiceService
from utils.logger import Logger


class PDFController:
    def __init__(self, pdf_service, invoice_service):
        self.pdf_service = pdf_service
        self.invoice_service = invoice_service
        self.logger = Logger(__name__).get_logger()

    @Logger.standardLogger
    def generate_invoice_pdf(self):
        try:
            data = request.get_json(force=True)
            
            if not data:
                return jsonify({"error": "Invoice data is required"}), 400

            pdf_data = self.pdf_service.generate_invoice_pdf(data)
            
            response = make_response(pdf_data)
            response.headers['Content-Type'] = 'application/pdf'
            response.headers['Content-Disposition'] = 'attachment; filename=invoice.pdf'
            
            return response

        except Exception as e:
            self.logger.error(f"Error in generate_invoice_pdf: {str(e)}")
            return jsonify({"error": "Internal server error"}), 500

    @Logger.standardLogger
    def sign_invoice_pdf(self, invoiceId):
        try:
            if not invoiceId:
                return jsonify({"error": "Invoice number is required"}), 400

            data = request.get_json(force=True)
            
            if not data or 'signature_data' not in data:
                return jsonify({"error": "Signature data is required"}), 400

            signed_pdf = self.pdf_service.sign_invoice_pdf(invoiceId, data)
            
            response = make_response(signed_pdf)
            response.headers['Content-Type'] = 'application/pdf'
            response.headers['Content-Disposition'] = f'attachment; filename=signed_invoice_{invoiceId}.pdf'
            
            return response

        except ValueError as e:
            return jsonify({"error": str(e)}), 404
        except Exception as e:
            self.logger.error(f"Error in sign_invoice_pdf: {str(e)}")
            return jsonify({"error": "Internal server error"}), 500