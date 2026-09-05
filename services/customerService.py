from flask import g
from sqlalchemy.exc import IntegrityError, SQLAlchemyError
from sqlalchemy import func
from models.freelance_management import Customer, Invoice, InvoiceStatusEnum, CurrencyEnum
from services.Base_Service import BaseService
from services.earningsService import get_earnings_in_inr
from utils.logger import Logger


class CustomerService(BaseService):
    _instance = None

    def __new__(cls):
        if cls._instance is None:
            cls._instance = super(CustomerService, cls).__new__(cls)
            cls.logger = Logger(__name__).get_logger()
        return cls._instance

    def __init__(self):
        super().__init__()

    def create_customer(self, customer_data):
        try:
            user_id = g.get('firebase_id')
            if not user_id:
                raise ValueError("User ID is required")

            if not customer_data.get('name'):
                raise ValueError("Name is required")

            customer = Customer(
                user_id=user_id,
                name=customer_data['name'],
                email=customer_data.get('email'),
                company=customer_data.get('company'),
                address=customer_data.get('address'),
                phone=customer_data.get('phone')
            )

            self.db.session.add(customer)
            self.db.session.commit()

            # Auto-link existing emails matching customer's email
            if customer.email:
                from services.customerEmailService import CustomerEmailService
                ce_service = CustomerEmailService()
                ce_service.batch_auto_link_for_customer(customer.id, user_id)

            # Return formatted customer
            formatted_customer = {
                "id": customer.id,
                "name": customer.name,
                "email": customer.email,
                "company": customer.company,
                "address": customer.address,  # Required field, won't be None
                "phone": customer.phone,
                "defaultTemplate": None,  # New customer won't have default template yet
                "totalEarnings": 0.0,
                "projectCount": 0,
                "lastInvoiceDate": None,
                "createdAt": customer.created_at.strftime('%Y-%m-%dT%H:%M:%S.%fZ') if customer.created_at else "",
                "updatedAt": customer.updated_at.strftime('%Y-%m-%dT%H:%M:%S.%fZ') if customer.updated_at else ""
            }
            
            self.logger.info(f"Customer created successfully: {customer.id}")
            return formatted_customer

        except IntegrityError as e:
            self.db.session.rollback()
            self.logger.error(f"Database integrity error creating customer: {str(e)}")
            raise ValueError("Customer with this email may already exist")
        except Exception as e:
            self.db.session.rollback()
            self.logger.error(f"Error creating customer: {str(e)}")
            raise

    def delete_customer(self, customer_id):
        try:
            user_id = g.get('firebase_id')
            customer = self.db.session.query(Customer).filter_by(
                id=customer_id, 
                user_id=user_id
            ).first()

            if not customer:
                raise ValueError("Customer not found")

            self.db.session.delete(customer)
            self.db.session.commit()
            
            self.logger.info(f"Customer deleted successfully: {customer_id}")
            return True

        except Exception as e:
            self.db.session.rollback()
            self.logger.error(f"Error deleting customer: {str(e)}")
            raise

    def get_customers(self, page=1, page_size=20):
        try:
            user_id = g.get('firebase_id')
            if not user_id:
                raise ValueError("User ID is required")

            offset = (page - 1) * page_size
            
            # Use eager loading to fetch templates along with customers
            from models.freelance_management import InvoiceTemplate
            from sqlalchemy.orm import joinedload
            
            query = self.db.session.query(Customer).options(
                joinedload(Customer.invoice_templates)
            ).filter_by(user_id=user_id)
            
            total_count = query.count()
            customers = query.offset(offset).limit(page_size).all()

            # Format customers to match Customer interface
            formatted_customers = []
            for customer in customers:
                # Get default template for customer
                default_template = None
                if customer.invoice_templates:
                    for template in customer.invoice_templates:
                        if template.is_customer_default:
                            default_template = template.template_data
                            break

                # Calculate customer analytics from invoices
                analytics = self._calculate_customer_analytics(customer.id, user_id)

                formatted_customer = {
                    "id": customer.id,
                    "name": customer.name,
                    "email": customer.email,
                    "company": customer.company,
                    "address": customer.address,
                    "phone": customer.phone,
                    "defaultTemplate": default_template,
                    "totalEarnings": analytics['total_earnings'],
                    "projectCount": analytics['project_count'],
                    "lastInvoiceDate": analytics['last_invoice_date'],
                    "createdAt": customer.created_at.strftime('%Y-%m-%dT%H:%M:%S.%fZ') if customer.created_at else "",
                    "updatedAt": customer.updated_at.strftime('%Y-%m-%dT%H:%M:%S.%fZ') if customer.updated_at else ""
                }
                formatted_customers.append(formatted_customer)

            return {
                "customers": formatted_customers,
                "total_count": total_count,
                "page": page,
                "page_size": len(formatted_customers)
            }

        except Exception as e:
            self.logger.error(f"Error fetching customers: {str(e)}")
            raise

    def get_customer_by_id(self, customer_id):
        try:
            user_id = g.get('firebase_id')
            from sqlalchemy.orm import joinedload
            
            customer = self.db.session.query(Customer).options(
                joinedload(Customer.invoice_templates)
            ).filter_by(
                id=customer_id, 
                user_id=user_id
            ).first()

            if not customer:
                raise ValueError("Customer not found")

            # Format customer to match Customer interface
            default_template = None
            if customer.invoice_templates:
                for template in customer.invoice_templates:
                    if template.is_customer_default:
                        default_template = template.template_data
                        break

            # Calculate customer analytics from invoices
            analytics = self._calculate_customer_analytics(customer.id, user_id)

            formatted_customer = {
                "id": customer.id,
                "name": customer.name,
                "email": customer.email,
                "company": customer.company,
                "address": customer.address,
                "phone": customer.phone,
                "defaultTemplate": default_template,
                "totalEarnings": analytics['total_earnings'],
                "projectCount": analytics['project_count'],
                "lastInvoiceDate": analytics['last_invoice_date'],
                "createdAt": customer.created_at.strftime('%Y-%m-%dT%H:%M:%S.%fZ') if customer.created_at else "",
                "updatedAt": customer.updated_at.strftime('%Y-%m-%dT%H:%M:%S.%fZ') if customer.updated_at else ""
            }

            return formatted_customer

        except Exception as e:
            self.logger.error(f"Error fetching customer: {str(e)}")
            raise

    def update_customer(self, customer_id, customer_data):
        try:
            user_id = g.get('firebase_id')
            from sqlalchemy.orm import joinedload
            
            customer = self.db.session.query(Customer).options(
                joinedload(Customer.invoice_templates)
            ).filter_by(
                id=customer_id, 
                user_id=user_id
            ).first()

            if not customer:
                raise ValueError("Customer not found")

            # Update fields
            if 'name' in customer_data:
                customer.name = customer_data['name']
            if 'email' in customer_data:
                customer.email = customer_data['email']
            if 'company' in customer_data:
                customer.company = customer_data['company']
            if 'address' in customer_data:
                customer.address = customer_data['address']
            if 'phone' in customer_data:
                customer.phone = customer_data['phone']

            self.db.session.commit()

            # Auto-link existing emails if email was updated
            if 'email' in customer_data and customer.email:
                from services.customerEmailService import CustomerEmailService
                ce_service = CustomerEmailService()
                ce_service.batch_auto_link_for_customer(customer.id, user_id)

            # Return formatted customer
            default_template = None
            if customer.invoice_templates:
                for template in customer.invoice_templates:
                    if template.is_customer_default:
                        default_template = template.template_data
                        break

            # Calculate customer analytics from invoices
            analytics = self._calculate_customer_analytics(customer.id, user_id)

            formatted_customer = {
                "id": customer.id,
                "name": customer.name,
                "email": customer.email,
                "company": customer.company,
                "address": customer.address,
                "phone": customer.phone,
                "defaultTemplate": default_template,
                "totalEarnings": analytics['total_earnings'],
                "projectCount": analytics['project_count'],
                "lastInvoiceDate": analytics['last_invoice_date'],
                "createdAt": customer.created_at.strftime('%Y-%m-%dT%H:%M:%S.%fZ') if customer.created_at else "",
                "updatedAt": customer.updated_at.strftime('%Y-%m-%dT%H:%M:%S.%fZ') if customer.updated_at else ""
            }

            self.logger.info(f"Customer updated successfully: {customer_id}")
            return formatted_customer

        except Exception as e:
            self.db.session.rollback()
            self.logger.error(f"Error updating customer: {str(e)}")
            raise

    def _calculate_customer_analytics(self, customer_id, user_id):
        """Calculate customer analytics from invoices.

        ak-lvu A.3: uses the canonical `get_earnings_in_inr` — same
        formula as dashboardService, no more per-currency branch that
        was double-counting INR invoices via invoice.total while foreign
        invoices used payment.amount_received.
        """
        try:
            # Canonical earnings (paid + partially_paid, in INR).
            total_earnings = get_earnings_in_inr(
                self.db.session, user_id, customer_id=customer_id,
            )

            # Paid-invoice count + last-invoice-date derived from a
            # single query (no relationship-eager-load needed for these
            # counts).
            invoices = self.db.session.query(Invoice).filter_by(
                customer_id=customer_id, user_id=user_id,
            ).all()
            paid_invoice_count = sum(
                1 for inv in invoices
                if inv.status in (
                    InvoiceStatusEnum.paid, InvoiceStatusEnum.partially_paid,
                )
            )
            last_invoice_date = None
            for inv in invoices:
                if inv.issue_date and (
                    last_invoice_date is None or inv.issue_date > last_invoice_date
                ):
                    last_invoice_date = inv.issue_date

            return {
                'total_earnings': float(total_earnings),
                'project_count': paid_invoice_count,
                'last_invoice_date': last_invoice_date.strftime('%Y-%m-%d') if last_invoice_date else None
            }

        except Exception as e:
            self.logger.error(f"Error calculating customer analytics: {str(e)}")
            return {
                'total_earnings': 0.0,
                'project_count': 0,
                'last_invoice_date': None
            }