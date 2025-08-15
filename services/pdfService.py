from flask import g
from models.freelance_management import Invoice, InvoiceSignature, Signature
from services.Base_Service import BaseService
from utils.logger import Logger
from utils.pdfGenerator import PDFGenerator


class PDFService(BaseService):
    _instance = None

    def __new__(cls):
        if cls._instance is None:
            cls._instance = super(PDFService, cls).__new__(cls)
            cls.logger = Logger(__name__).get_logger()
            cls.pdf_generator = PDFGenerator()
        return cls._instance

    def __init__(self):
        super().__init__()

    def generate_invoice_pdf(self, invoice_data):
        try:
            pdf_data = self.pdf_generator.generate_invoice_pdf(invoice_data)
            self.logger.info("Invoice PDF generated successfully")
            return pdf_data

        except Exception as e:
            self.logger.error(f"Error generating invoice PDF: {str(e)}")
            raise

    def sign_invoice_pdf(self, invoice_number, signature_data):
        try:
            user_id = g.get('firebase_id')
            if not user_id:
                raise ValueError("User ID is required")

            # Get the invoice
            invoice = self.db.session.query(Invoice).filter_by(
                invoice_number=invoice_number, 
                user_id=user_id
            ).first()

            if not invoice:
                raise ValueError("Invoice not found")

            # Generate the original PDF first
            invoice_dict = {key: value for key, value in invoice.__dict__.items() 
                           if key != '_sa_instance_state'}
            
            # Include items
            invoice_dict['items'] = [
                {key: value for key, value in item.__dict__.items() if key != '_sa_instance_state'}
                for item in invoice.items
            ]

            pdf_data = self.pdf_generator.generate_invoice_pdf(invoice_dict)

            # Add signature to PDF
            position = {
                'x': signature_data.get('position_x', 100),
                'y': signature_data.get('position_y', 100),
                'width': signature_data.get('width', 100),
                'height': signature_data.get('height', 50)
            }

            signed_pdf = self.pdf_generator.add_signature_to_pdf(
                pdf_data, 
                signature_data.get('signature_data'), 
                position
            )

            # Update invoice as signed
            invoice.is_signed = True
            self.db.session.commit()

            # Save signature record if signature_id provided
            if 'signature_id' in signature_data:
                invoice_signature = InvoiceSignature(
                    invoice_id=invoice.id,
                    signature_id=signature_data['signature_id'],
                    signature_position_x=position['x'],
                    signature_position_y=position['y'],
                    signature_width=position['width'],
                    signature_height=position['height']
                )
                self.db.session.add(invoice_signature)
                self.db.session.commit()

            self.logger.info(f"Invoice PDF signed successfully: {invoice_number}")
            return signed_pdf

        except Exception as e:
            self.db.session.rollback()
            self.logger.error(f"Error signing invoice PDF: {str(e)}")
            raise