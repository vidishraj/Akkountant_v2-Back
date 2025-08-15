from flask import g
from sqlalchemy.exc import IntegrityError, SQLAlchemyError
from models.freelance_management import Invoice, InvoicePayment
from services.Base_Service import BaseService
from utils.logger import Logger
from datetime import datetime


class PaymentService(BaseService):
    _instance = None

    def __new__(cls):
        if cls._instance is None:
            cls._instance = super(PaymentService, cls).__new__(cls)
            cls.logger = Logger(__name__).get_logger()
        return cls._instance

    def __init__(self):
        super().__init__()

    def add_payment(self, invoice_id, payment_data):
        try:
            user_id = g.get('firebase_id')
            if not user_id:
                raise ValueError("User ID is required")

            # Verify invoice belongs to user
            invoice = self.db.session.query(Invoice).filter_by(
                id=invoice_id, 
                user_id=user_id
            ).first()

            if not invoice:
                raise ValueError("Invoice not found")

            # Check if payment already exists for this invoice
            existing_payment = self.db.session.query(InvoicePayment).filter_by(
                invoice_id=invoice_id
            ).first()
            
            if existing_payment:
                raise ValueError("Payment already exists for this invoice. Only one payment per invoice is allowed.")

            payment = InvoicePayment(
                invoice_id=invoice_id,
                payment_method=payment_data.get('paymentMethod'),
                amount_received=float(payment_data['amountReceived']),
                payment_date=datetime.strptime(payment_data['paymentDate'], '%Y-%m-%d').date() if payment_data.get('paymentDate') else None,
                breakdown=payment_data.get('breakdown'),
                notes=payment_data.get('notes')
            )

            self.db.session.add(payment)
            self.db.session.commit()
            
            self.logger.info(f"Payment added successfully: {payment.id}")
            return self._format_payment(payment)

        except Exception as e:
            self.db.session.rollback()
            self.logger.error(f"Error adding payment: {str(e)}")
            raise

    def replace_payment(self, invoice_id, payment_data):
        """Replace existing payment with new payment data"""
        try:
            user_id = g.get('firebase_id')
            if not user_id:
                raise ValueError("User ID is required")

            # Verify invoice belongs to user
            invoice = self.db.session.query(Invoice).filter_by(
                id=invoice_id, 
                user_id=user_id
            ).first()

            if not invoice:
                raise ValueError("Invoice not found")

            # Delete existing payment if it exists
            existing_payment = self.db.session.query(InvoicePayment).filter_by(
                invoice_id=invoice_id
            ).first()
            
            if existing_payment:
                self.db.session.delete(existing_payment)

            # Create new payment
            payment = InvoicePayment(
                invoice_id=invoice_id,
                payment_method=payment_data.get('paymentMethod'),
                amount_received=float(payment_data['amountReceived']),
                payment_date=datetime.strptime(payment_data['paymentDate'], '%Y-%m-%d').date() if payment_data.get('paymentDate') else None,
                breakdown=payment_data.get('breakdown'),
                notes=payment_data.get('notes')
            )

            self.db.session.add(payment)
            self.db.session.commit()
            
            self.logger.info(f"Payment replaced successfully: {payment.id}")
            return self._format_payment(payment)

        except Exception as e:
            self.db.session.rollback()
            self.logger.error(f"Error replacing payment: {str(e)}")
            raise

    def update_payment(self, invoice_id, payment_id, payment_data):
        try:
            user_id = g.get('firebase_id')
            
            # Verify invoice belongs to user
            invoice = self.db.session.query(Invoice).filter_by(
                id=invoice_id, 
                user_id=user_id
            ).first()

            if not invoice:
                raise ValueError("Invoice not found")

            payment = self.db.session.query(InvoicePayment).filter_by(
                id=payment_id,
                invoice_id=invoice_id
            ).first()

            if not payment:
                raise ValueError("Payment not found")

            # Update fields
            if 'paymentMethod' in payment_data:
                payment.payment_method = payment_data['paymentMethod']
            if 'amountReceived' in payment_data:
                payment.amount_received = float(payment_data['amountReceived'])
            if 'paymentDate' in payment_data:
                payment.payment_date = datetime.strptime(payment_data['paymentDate'], '%Y-%m-%d').date() if payment_data['paymentDate'] else None
            if 'breakdown' in payment_data:
                payment.breakdown = payment_data['breakdown']
            if 'notes' in payment_data:
                payment.notes = payment_data['notes']

            self.db.session.commit()
            
            self.logger.info(f"Payment updated successfully: {payment_id}")
            return self._format_payment(payment)

        except Exception as e:
            self.db.session.rollback()
            self.logger.error(f"Error updating payment: {str(e)}")
            raise

    def delete_payment(self, invoice_id, payment_id):
        try:
            user_id = g.get('firebase_id')
            
            # Verify invoice belongs to user
            invoice = self.db.session.query(Invoice).filter_by(
                id=invoice_id, 
                user_id=user_id
            ).first()

            if not invoice:
                raise ValueError("Invoice not found")

            payment = self.db.session.query(InvoicePayment).filter_by(
                id=payment_id,
                invoice_id=invoice_id
            ).first()

            if not payment:
                raise ValueError("Payment not found")

            self.db.session.delete(payment)
            self.db.session.commit()
            
            self.logger.info(f"Payment deleted successfully: {payment_id}")
            return True

        except Exception as e:
            self.db.session.rollback()
            self.logger.error(f"Error deleting payment: {str(e)}")
            raise

    def _format_payment(self, payment):
        """Format payment data for API response"""
        return {
            "id": payment.id,
            "paymentMethod": payment.payment_method,
            "amountReceived": float(payment.amount_received),
            "paymentDate": payment.payment_date.strftime('%Y-%m-%d') if payment.payment_date else None,
            "breakdown": payment.breakdown,
            "notes": payment.notes,
            "createdAt": payment.created_at.strftime('%Y-%m-%dT%H:%M:%S.%fZ') if payment.created_at else "",
            "updatedAt": payment.updated_at.strftime('%Y-%m-%dT%H:%M:%S.%fZ') if payment.updated_at else ""
        }
