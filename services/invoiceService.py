from flask import g
from sqlalchemy.exc import IntegrityError, SQLAlchemyError
from sqlalchemy import func, desc, asc, or_
from sqlalchemy.orm import joinedload
from models.freelance_management import Invoice, InvoiceItem, Customer, InvoicePayment, CurrencyEnum, InvoiceCustomField
from services.Base_Service import BaseService
from services.currencyService import CurrencyService
from utils.logger import Logger
from datetime import datetime


class InvoiceService(BaseService):
    _instance = None

    def __new__(cls):
        if cls._instance is None:
            cls._instance = super(InvoiceService, cls).__new__(cls)
            cls.logger = Logger(__name__).get_logger()
        return cls._instance

    def __init__(self):
        super().__init__()
        self.currency_service = CurrencyService()

    def create_invoice(self, invoice_data):
        try:
            user_id = g.get('firebase_id')
            if not user_id:
                raise ValueError("User ID is required")

            # Handle camelCase format
            from_data = invoice_data.get('from', {})
            to_data = invoice_data.get('to', {})
            tax_data = invoice_data.get('tax', {})
            
            invoice = Invoice(
                user_id=user_id,
                customer_id=invoice_data.get('customerId'),
                invoice_number=invoice_data['invoiceNumber'],
                project_name=invoice_data['projectName'],
                issue_date=datetime.strptime(invoice_data['issueDate'], '%Y-%m-%d').date(),
                due_date=datetime.strptime(invoice_data['dueDate'], '%Y-%m-%d').date(),
                from_name=from_data.get('name', ''),
                from_email=from_data.get('email', ''),
                # v3 hotfix (hq-wisp-wf8kb): address optional — agent may
                # omit if unknown rather than silent-refuse the whole call.
                # Overseer confirmed blank from_address is acceptable; user
                # can fill in via UI post-create.
                from_address=from_data.get('address', ''),
                from_phone=from_data.get('phone'),
                to_name=to_data.get('name', ''),
                to_email=to_data.get('email', ''),
                to_address=to_data.get('address', ''),
                to_company=to_data.get('company'),
                subtotal=float(invoice_data.get('subtotal', 0)),
                tax_rate=float(tax_data.get('rate', 0)),
                tax_amount=float(tax_data.get('amount', 0)),
                total=float(invoice_data.get('total', 0)),
                notes=invoice_data.get('notes'),
                terms=invoice_data.get('terms'),
                status=invoice_data.get('status', 'draft'),
                currency=CurrencyEnum(invoice_data.get('currency', 'USD'))
            )

            self.db.session.add(invoice)
            self.db.session.flush()

            if 'items' in invoice_data:
                for idx, item_data in enumerate(invoice_data['items']):
                    item = InvoiceItem(
                        invoice_id=invoice.id,
                        description=item_data.get('description', ''),
                        quantity=float(item_data.get('quantity', 1)),
                        rate=float(item_data.get('rate', 0)),
                        amount=float(item_data.get('amount', 0)),
                        item_order=idx + 1  # Use index for order
                    )
                    self.db.session.add(item)

            # Handle payment if provided
            if 'payment' in invoice_data and invoice_data['payment']:
                self._replace_payment(invoice.id, invoice_data['payment'])

            # Handle custom fields if provided
            if 'customFields' in invoice_data and invoice_data['customFields']:
                self._create_custom_fields(invoice.id, invoice_data['customFields'])

            self.db.session.commit()
            self.logger.info(f"Invoice created successfully: {invoice.id}")
            
            # Return formatted invoice
            return self._format_invoice(invoice)

        except Exception as e:
            self.db.session.rollback()
            self.logger.error(f"Error creating invoice: {str(e)}")
            raise

    def _mark_overdue_invoices(self, user_id):
        """Auto-detect and mark sent invoices past their due date as overdue."""
        try:
            overdue = self.db.session.query(Invoice).filter(
                Invoice.user_id == user_id,
                Invoice.status == 'sent',
                Invoice.due_date < datetime.now().date()
            ).all()
            if overdue:
                for inv in overdue:
                    inv.status = 'overdue'
                self.db.session.commit()
                self.logger.info(f"Marked {len(overdue)} invoice(s) as overdue for user {user_id}")
        except Exception as e:
            self.db.session.rollback()
            self.logger.error(f"Error marking overdue invoices: {str(e)}")

    def get_invoices(self, page=1, limit=20, status=None, sort_by="created_at", sort_order="desc", search=None, customer_id=None):
        try:
            user_id = g.get('firebase_id')
            if not user_id:
                raise ValueError("User ID is required")

            # Auto-detect overdue invoices before querying
            self._mark_overdue_invoices(user_id)

            offset = (page - 1) * limit

            query = self.db.session.query(Invoice).options(
                joinedload(Invoice.payments),
                joinedload(Invoice.custom_fields)
            ).filter_by(user_id=user_id)
            
            # Apply customer filter
            if customer_id:
                query = query.filter(Invoice.customer_id == customer_id)

            # Apply status filter
            if status:
                query = query.filter(Invoice.status == status)
            
            # Apply search filter (searches across multiple fields)
            if search:
                search_term = f"%{search}%"
                query = query.filter(
                    or_(
                        Invoice.invoice_number.ilike(search_term),
                        Invoice.project_name.ilike(search_term),
                        Invoice.to_name.ilike(search_term),
                        Invoice.to_company.ilike(search_term),
                        Invoice.notes.ilike(search_term)
                    )
                )
            
            # Apply sorting
            sort_column = getattr(Invoice, sort_by, Invoice.created_at)
            if sort_order == "asc":
                query = query.order_by(asc(sort_column))
            else:
                query = query.order_by(desc(sort_column))
            
            total_count = query.count()
            invoices = query.offset(offset).limit(limit).all()

            # Format invoices to match InvoiceData interface
            formatted_invoices = [self._format_invoice(invoice) for invoice in invoices]

            return {
                "invoices": formatted_invoices,
                "total_count": total_count,
                "page": page,
                "page_size": len(formatted_invoices)
            }

        except Exception as e:
            self.logger.error(f"Error fetching invoices: {str(e)}")
            raise

    def get_invoice_by_number(self, invoice_number):
        try:
            user_id = g.get('firebase_id')
            invoice = self.db.session.query(Invoice).options(
                joinedload(Invoice.payments),
                joinedload(Invoice.custom_fields)
            ).filter_by(
                invoice_number=invoice_number, 
                user_id=user_id
            ).first()

            if not invoice:
                raise ValueError("Invoice not found")

            return self._format_invoice(invoice)

        except Exception as e:
            self.logger.error(f"Error fetching invoice: {str(e)}")
            raise

    def update_invoice(self, invoice_number, invoice_data):
        try:
            user_id = g.get('firebase_id')
            invoice = self.db.session.query(Invoice).filter_by(
                invoice_number=invoice_number, 
                user_id=user_id
            ).first()

            if not invoice:
                raise ValueError("Invoice not found")

            # Handle camelCase format for updates
            from_data = invoice_data.get('from', {})
            to_data = invoice_data.get('to', {})
            tax_data = invoice_data.get('tax', {})
            
            # Update basic fields
            if 'invoiceNumber' in invoice_data:
                invoice.invoice_number = invoice_data['invoiceNumber']
            if 'projectName' in invoice_data:
                invoice.project_name = invoice_data['projectName']
            if 'customerId' in invoice_data:
                invoice.customer_id = invoice_data['customerId']
            if 'notes' in invoice_data:
                invoice.notes = invoice_data['notes']
            if 'terms' in invoice_data:
                invoice.terms = invoice_data['terms']
            if 'status' in invoice_data:
                invoice.status = invoice_data['status']

            # Update from fields
            if from_data:
                if 'name' in from_data:
                    invoice.from_name = from_data['name']
                if 'email' in from_data:
                    invoice.from_email = from_data['email']
                if 'address' in from_data:
                    invoice.from_address = from_data['address']
                if 'phone' in from_data:
                    invoice.from_phone = from_data['phone']

            # Update to fields
            if to_data:
                if 'name' in to_data:
                    invoice.to_name = to_data['name']
                if 'email' in to_data:
                    invoice.to_email = to_data['email']
                if 'address' in to_data:
                    invoice.to_address = to_data['address']
                if 'company' in to_data:
                    invoice.to_company = to_data['company']

            # Handle date fields
            if 'issueDate' in invoice_data:
                invoice.issue_date = datetime.strptime(invoice_data['issueDate'], '%Y-%m-%d').date()
            if 'dueDate' in invoice_data:
                invoice.due_date = datetime.strptime(invoice_data['dueDate'], '%Y-%m-%d').date()

            # Handle numeric fields
            if 'subtotal' in invoice_data:
                invoice.subtotal = float(invoice_data['subtotal'])
            if 'total' in invoice_data:
                invoice.total = float(invoice_data['total'])
            
            # Handle currency
            if 'currency' in invoice_data:
                invoice.currency = CurrencyEnum(invoice_data['currency'])
            
            # Handle tax fields
            if tax_data:
                if 'rate' in tax_data:
                    invoice.tax_rate = float(tax_data['rate'])
                if 'amount' in tax_data:
                    invoice.tax_amount = float(tax_data['amount'])

            # Update items if provided
            if 'items' in invoice_data:
                # Delete existing items
                self.db.session.query(InvoiceItem).filter_by(invoice_id=invoice.id).delete()
                
                # Add new items
                for idx, item_data in enumerate(invoice_data['items']):
                    item = InvoiceItem(
                        invoice_id=invoice.id,
                        description=item_data.get('description', ''),
                        quantity=float(item_data.get('quantity', 1)),
                        rate=float(item_data.get('rate', 0)),
                        amount=float(item_data.get('amount', 0)),
                        item_order=idx + 1  # Use index for order
                    )
                    self.db.session.add(item)

            # Handle payment if provided
            if 'payment' in invoice_data and invoice_data['payment']:
                self._replace_payment(invoice.id, invoice_data['payment'])

            # Handle custom fields if provided
            if 'customFields' in invoice_data:
                # Delete existing custom fields
                self.db.session.query(InvoiceCustomField).filter_by(invoice_id=invoice.id).delete()
                
                # Add new custom fields
                if invoice_data['customFields']:
                    self._create_custom_fields(invoice.id, invoice_data['customFields'])

            self.db.session.commit()
            self.logger.info(f"Invoice updated successfully: {invoice_number}")
            return self._format_invoice(invoice)

        except Exception as e:
            self.db.session.rollback()
            self.logger.error(f"Error updating invoice: {str(e)}")
            raise

    def delete_invoice(self, invoice_number):
        try:
            user_id = g.get('firebase_id')
            invoice = self.db.session.query(Invoice).filter_by(
                invoice_number=invoice_number, 
                user_id=user_id
            ).first()

            if not invoice:
                raise ValueError("Invoice not found")

            self.db.session.delete(invoice)
            self.db.session.commit()
            
            self.logger.info(f"Invoice deleted successfully: {invoice_number}")
            return True

        except Exception as e:
            self.db.session.rollback()
            self.logger.error(f"Error deleting invoice: {str(e)}")
            raise

    def _format_invoice(self, invoice):
        """Format invoice to match InvoiceData interface"""
        # Format items
        formatted_items = []
        for item in invoice.items:
            formatted_items.append({
                "description": item.description,
                "quantity": float(item.quantity),
                "rate": float(item.rate),
                "amount": float(item.amount)
            })

        # Format tax information
        tax = None
        if invoice.tax_rate and invoice.tax_rate > 0:
            tax = {
                "rate": float(invoice.tax_rate),
                "amount": float(invoice.tax_amount or 0)
            }

        # Format payments (only one payment allowed per invoice)
        payment = None
        if invoice.payments:
            # Get the single payment (only one should exist)
            single_payment = invoice.payments[0]
            payment = {
                "paymentMethod": single_payment.payment_method,
                "amountReceived": float(single_payment.amount_received),
                "breakdown": single_payment.breakdown or {},
                "paymentDate": single_payment.payment_date.strftime('%Y-%m-%d') if single_payment.payment_date else None,
                "notes": single_payment.notes
            }

        # Format custom fields
        custom_fields = []
        if invoice.custom_fields:
            for field in sorted(invoice.custom_fields, key=lambda x: x.sort_order):
                custom_fields.append({
                    "key": field.field_key,
                    "value": field.field_value,
                    "hidden": field.is_hidden
                })

        # Format the invoice
        formatted_invoice = {
            "invoiceNumber": invoice.invoice_number,
            "projectName": invoice.project_name,
            "issueDate": invoice.issue_date.strftime('%Y-%m-%d') if invoice.issue_date else "",
            "dueDate": invoice.due_date.strftime('%Y-%m-%d') if invoice.due_date else "",
            "customerId": invoice.customer_id,
            "from": {
                "name": invoice.from_name,
                "email": invoice.from_email,
                "address": invoice.from_address,
                "phone": invoice.from_phone
            },
            "to": {
                "name": invoice.to_name,
                "email": invoice.to_email,
                "address": invoice.to_address,
                "company": invoice.to_company
            },
            "customFields": custom_fields,
            "items": formatted_items,
            "subtotal": float(invoice.subtotal),
            "tax": tax,
            "total": float(invoice.total),
            "currency": invoice.currency.value if hasattr(invoice.currency, 'value') else str(invoice.currency),
            "payment": payment,
            "notes": invoice.notes,
            "terms": invoice.terms,
            "status": invoice.status.value if hasattr(invoice.status, 'value') else str(invoice.status),
            "createdAt": invoice.created_at.strftime('%Y-%m-%dT%H:%M:%S.%fZ') if invoice.created_at else None,
            "updatedAt": invoice.updated_at.strftime('%Y-%m-%dT%H:%M:%S.%fZ') if invoice.updated_at else None
        }

        return formatted_invoice

    def _replace_payment(self, invoice_id, payment_data):
        """Helper method to replace existing payment or create new one for an invoice"""
        # Delete existing payment if it exists
        existing_payment = self.db.session.query(InvoicePayment).filter_by(
            invoice_id=invoice_id
        ).first()
        
        if existing_payment:
            self.db.session.delete(existing_payment)
        
        # Get invoice to check currency
        invoice = self.db.session.query(Invoice).filter_by(id=invoice_id).first()
        if not invoice:
            raise ValueError("Invoice not found")
        
        # Convert payment amount to INR if needed
        payment_amount = float(payment_data['amountReceived'])
        converted_amount = self._convert_to_inr(payment_amount, invoice.currency)
        
        # Create new payment
        payment = InvoicePayment(
            invoice_id=invoice_id,
            payment_method=payment_data['paymentMethod'],
            amount_received=converted_amount,
            payment_date=datetime.strptime(payment_data['paymentDate'], '%Y-%m-%d').date() if payment_data.get('paymentDate') else None,
            notes=payment_data.get('notes'),
            breakdown=payment_data.get('breakdown', {})
        )
        self.db.session.add(payment)
        
        # Update invoice status to paid if payment covers the full amount
        # Compare in original currency for status determination
        if invoice and payment_amount >= float(invoice.total):
            invoice.status = 'paid'

    def _convert_to_inr(self, amount, currency):
        """Convert amount to INR using live exchange rates"""
        currency_str = currency.value if hasattr(currency, 'value') else str(currency)
        
        if currency_str == 'INR':
            return float(amount)
        
        try:
            converted_amount = self.currency_service.convert_to_inr(amount, currency_str)
            self.logger.info(f"Converted {amount} {currency_str} to {converted_amount} INR using live rates")
            return converted_amount
        except Exception as e:
            self.logger.error(f"Error converting currency using live rates: {str(e)}")
            # This should not happen as currency service has its own fallbacks
            raise

    def _create_custom_fields(self, invoice_id, custom_fields_data):
        """Helper method to create custom fields for an invoice"""
        if not custom_fields_data:
            return
            
        # Validate maximum 8 custom fields
        if len(custom_fields_data) > 8:
            raise ValueError("Maximum 8 custom fields allowed per invoice")
        
        # Track used keys to prevent duplicates
        used_keys = set()
        
        for idx, field_data in enumerate(custom_fields_data):
            # Validate required fields
            if not field_data.get('key'):
                raise ValueError("Custom field key is required")
            
            # Process key and hidden status
            field_key = field_data['key'].strip()
            is_hidden = False
            
            # Handle asterisk prefix for hidden fields
            if field_key.startswith('*'):
                is_hidden = True
                field_key = field_key[1:].strip()
            
            # Validate key length
            if len(field_key) > 50:
                raise ValueError("Custom field key must be 50 characters or less")
            
            # Check for duplicate keys
            if field_key.lower() in used_keys:
                raise ValueError(f"Duplicate custom field key: {field_key}")
            used_keys.add(field_key.lower())
            
            # Validate value length
            field_value = field_data.get('value', '').strip()
            if len(field_value) > 200:
                raise ValueError("Custom field value must be 200 characters or less")
            
            # Create custom field
            custom_field = InvoiceCustomField(
                invoice_id=invoice_id,
                field_key=field_key,
                field_value=field_value,
                is_hidden=is_hidden or field_data.get('hidden', False),
                sort_order=idx
            )
            self.db.session.add(custom_field)