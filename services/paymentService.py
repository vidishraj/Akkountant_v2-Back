from flask import g
from sqlalchemy.exc import IntegrityError, SQLAlchemyError
from models.freelance_management import Invoice, InvoicePayment
from services.Base_Service import BaseService
from services.money_utils import money, q2
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

            # ak-lvu A.1 + A.5: delegate to invoiceService._replace_payment so
            # the FX-idempotency + full metadata + fail-closed conversion path
            # runs on this write too — never write a payment without inr_amount /
            # fx_rate / fx_rate_source populated.
            from services.invoiceService import InvoiceService
            invoice_service = InvoiceService()
            payment = invoice_service._replace_payment(invoice_id, payment_data)

            # ak-lvu A.4: status recompute after mutation.
            invoice_service._recompute_invoice_status(invoice)

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

            # ak-lvu A.1 + A.4: delegate to invoiceService for FX idempotency
            # + status recompute. Same reasoning as add_payment.
            from services.invoiceService import InvoiceService
            invoice_service = InvoiceService()
            payment = invoice_service._replace_payment(invoice_id, payment_data)
            invoice_service._recompute_invoice_status(invoice)
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

            # ak-lvu A.1 + A.5: if amountReceived / originalAmount is in
            # the payload, delegate to invoiceService._replace_payment for
            # the full FX-metadata recompute (this handles the "amount
            # changed" case, which needs a fresh FX conversion + audit
            # trail). For metadata-only changes (payment_method / notes),
            # patch in place.
            amount_changed = (
                'amountReceived' in payment_data or 'originalAmount' in payment_data
            )
            if amount_changed:
                # Ensure the payload has an id so _replace_payment can
                # match the idempotency path if the amount is unchanged.
                if 'id' not in payment_data:
                    payment_data = {**payment_data, 'id': payment.id}
                from services.invoiceService import InvoiceService
                invoice_service = InvoiceService()
                payment = invoice_service._replace_payment(invoice_id, payment_data)
                invoice_service._recompute_invoice_status(invoice)
            else:
                # Metadata-only patch — no FX conversion needed.
                if 'paymentMethod' in payment_data:
                    payment.payment_method = payment_data['paymentMethod']
                if 'paymentDate' in payment_data:
                    payment.payment_date = (
                        datetime.strptime(payment_data['paymentDate'], '%Y-%m-%d').date()
                        if payment_data['paymentDate'] else None
                    )
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
            self.db.session.flush()

            # ak-lvu A.4: payment delete triggers status recompute — an
            # invoice that WAS paid becomes sent/overdue when the only
            # payment is removed.
            from services.invoiceService import InvoiceService
            invoice_service = InvoiceService()
            invoice_service._recompute_invoice_status(invoice)

            self.db.session.commit()

            self.logger.info(f"Payment deleted successfully: {payment_id}")
            return True

        except Exception as e:
            self.db.session.rollback()
            self.logger.error(f"Error deleting payment: {str(e)}")
            raise

    def _format_payment(self, payment):
        """Format payment data for API response.

        ak-lvu A.1: includes the 6 FX audit fields (original_amount,
        original_currency, inr_amount, fx_rate, fx_rate_source,
        converted_at) — read tolerantly via the invoiceService helper
        so pre-migration rows serialize gracefully as None.
        """
        # Tolerantly read FX metadata (matches invoiceService.
        # _safe_read_payment_fx shape).
        try:
            original_amount = float(payment.original_amount) if payment.original_amount is not None else None
            original_currency = payment.original_currency
            inr_amount = float(payment.inr_amount) if payment.inr_amount is not None else None
            fx_rate = float(payment.fx_rate) if payment.fx_rate is not None else None
            fx_rate_source = payment.fx_rate_source
            converted_at = payment.converted_at.strftime('%Y-%m-%dT%H:%M:%S.%fZ') if payment.converted_at else None
        except AttributeError:
            # Very-pre-migration ORM shape didn't have the columns at all.
            original_amount = original_currency = inr_amount = None
            fx_rate = fx_rate_source = converted_at = None

        return {
            "id": payment.id,
            "paymentMethod": payment.payment_method,
            # Legacy alias for pre-ak-awp FE builds:
            "amountReceived": inr_amount if inr_amount is not None else (
                float(payment.amount_received) if payment.amount_received is not None else 0.0
            ),
            # New authoritative fields (ak-lvu wire contract):
            "originalAmount": original_amount,
            "originalCurrency": original_currency,
            "inrAmount": inr_amount,
            "fxRate": fx_rate,
            "fxRateSource": fx_rate_source,
            "convertedAt": converted_at,
            "paymentDate": payment.payment_date.strftime('%Y-%m-%d') if payment.payment_date else None,
            "breakdown": payment.breakdown,
            "notes": payment.notes,
            "createdAt": payment.created_at.strftime('%Y-%m-%dT%H:%M:%S.%fZ') if payment.created_at else "",
            "updatedAt": payment.updated_at.strftime('%Y-%m-%dT%H:%M:%S.%fZ') if payment.updated_at else ""
        }
