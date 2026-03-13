from flask import g
from sqlalchemy.exc import IntegrityError
from sqlalchemy import or_
from models.customer_emails import CustomerEmail
from models.processedEmails import ProcessedEmails
from models.freelance_management import Customer
from services.Base_Service import BaseService
from utils.logger import Logger


class CustomerEmailService(BaseService):
    _instance = None

    def __new__(cls):
        if cls._instance is None:
            cls._instance = super(CustomerEmailService, cls).__new__(cls)
            cls.logger = Logger(__name__).get_logger()
        return cls._instance

    def __init__(self):
        super().__init__()

    def get_emails_for_customer(self, customer_id):
        try:
            user_id = g.get('firebase_id')
            if not user_id:
                raise ValueError("User ID is required")

            # Verify customer belongs to user
            customer = self.db.session.query(Customer).filter_by(
                id=customer_id, user_id=user_id
            ).first()
            if not customer:
                raise ValueError("Customer not found")

            # Get linked emails with join
            links = self.db.session.query(CustomerEmail).filter_by(
                customer_id=customer_id
            ).all()

            results = []
            for link in links:
                email = self.db.session.query(ProcessedEmails).filter_by(
                    id=link.email_id
                ).first()
                if email:
                    results.append({
                        "id": link.id,
                        "customer_id": link.customer_id,
                        "email_id": link.email_id,
                        "linked_by": link.linked_by,
                        "created_at": link.created_at.strftime('%Y-%m-%dT%H:%M:%S.%fZ') if link.created_at else "",
                        "email": {
                            "id": email.id,
                            "gmail_id": email.gmail_id,
                            "subject": email.subject,
                            "sender": email.sender,
                            "received_date": email.email_date.strftime('%Y-%m-%dT%H:%M:%S.%fZ') if email.email_date else "",
                            "category": email.category,
                            "extraction_summary": email.extraction_summary,
                        }
                    })

            return results

        except Exception as e:
            self.logger.error(f"Error fetching emails for customer: {str(e)}")
            raise

    def link_email(self, customer_id, email_id, linked_by='manual'):
        try:
            user_id = g.get('firebase_id')
            if not user_id:
                raise ValueError("User ID is required")

            # Verify customer belongs to user
            customer = self.db.session.query(Customer).filter_by(
                id=customer_id, user_id=user_id
            ).first()
            if not customer:
                raise ValueError("Customer not found")

            # Verify email belongs to user
            email = self.db.session.query(ProcessedEmails).filter_by(
                id=email_id, user_id=user_id
            ).first()
            if not email:
                raise ValueError("Email not found")

            link = CustomerEmail(
                customer_id=customer_id,
                email_id=email_id,
                linked_by=linked_by,
            )
            self.db.session.add(link)
            self.db.session.commit()

            return {
                "id": link.id,
                "customer_id": link.customer_id,
                "email_id": link.email_id,
                "linked_by": link.linked_by,
                "created_at": link.created_at.strftime('%Y-%m-%dT%H:%M:%S.%fZ') if link.created_at else "",
                "email": {
                    "id": email.id,
                    "subject": email.subject,
                    "sender": email.sender,
                    "received_date": email.email_date.strftime('%Y-%m-%dT%H:%M:%S.%fZ') if email.email_date else "",
                    "category": email.category,
                    "extraction_summary": email.extraction_summary,
                }
            }

        except IntegrityError:
            self.db.session.rollback()
            raise ValueError("Email is already linked to this customer")
        except Exception as e:
            self.db.session.rollback()
            self.logger.error(f"Error linking email: {str(e)}")
            raise

    def unlink_email(self, customer_id, email_id):
        try:
            user_id = g.get('firebase_id')
            if not user_id:
                raise ValueError("User ID is required")

            # Verify customer belongs to user
            customer = self.db.session.query(Customer).filter_by(
                id=customer_id, user_id=user_id
            ).first()
            if not customer:
                raise ValueError("Customer not found")

            link = self.db.session.query(CustomerEmail).filter_by(
                customer_id=customer_id, email_id=email_id
            ).first()
            if not link:
                raise ValueError("Email link not found")

            self.db.session.delete(link)
            self.db.session.commit()
            return True

        except Exception as e:
            self.db.session.rollback()
            self.logger.error(f"Error unlinking email: {str(e)}")
            raise

    def search_emails(self, query):
        try:
            user_id = g.get('firebase_id')
            if not user_id:
                raise ValueError("User ID is required")

            search_filter = or_(
                ProcessedEmails.subject.ilike(f'%{query}%'),
                ProcessedEmails.sender.ilike(f'%{query}%'),
            )

            emails = self.db.session.query(ProcessedEmails).filter(
                ProcessedEmails.user_id == user_id,
                search_filter,
            ).order_by(ProcessedEmails.email_date.desc()).limit(50).all()

            return [{
                "id": e.id,
                "subject": e.subject,
                "sender": e.sender,
                "received_date": e.email_date.strftime('%Y-%m-%dT%H:%M:%S.%fZ') if e.email_date else "",
                "category": e.category,
                "extraction_summary": e.extraction_summary,
            } for e in emails]

        except Exception as e:
            self.logger.error(f"Error searching emails: {str(e)}")
            raise

    # Generic domains that should never be used for domain-based matching
    GENERIC_DOMAINS = {
        'gmail.com', 'yahoo.com', 'outlook.com', 'hotmail.com', 'live.com',
        'icloud.com', 'aol.com', 'protonmail.com', 'zoho.com', 'mail.com',
        'yandex.com', 'rediffmail.com', 'googlemail.com',
    }

    @staticmethod
    def _extract_email(sender):
        """Extract email address from sender field like 'Name <email@domain.com>'."""
        if not sender:
            return None
        if '<' in sender and '>' in sender:
            sender = sender.split('<')[1].split('>')[0]
        return sender.strip().lower()

    @staticmethod
    def _extract_domain(email_addr):
        """Extract domain from an email address."""
        if not email_addr or '@' not in email_addr:
            return None
        return email_addr.split('@')[1].lower()

    def _matches_customer(self, email_row, customer, customer_email, customer_domain):
        """Check if an email matches a customer by domain or content reference."""
        sender = self._extract_email(email_row.sender)

        # 1. Exact email match
        if sender and customer_email and sender == customer_email:
            return True

        # 2. Domain match (skip generic domains)
        if sender and customer_domain and customer_domain not in self.GENERIC_DOMAINS:
            sender_domain = self._extract_domain(sender)
            if sender_domain == customer_domain:
                return True

        # 3. Customer name or company mentioned in subject
        subject = (email_row.subject or '').lower()
        if subject:
            name = (customer.name or '').strip().lower()
            company = (customer.company or '').strip().lower()
            if name and len(name) >= 3 and name in subject:
                return True
            if company and len(company) >= 3 and company in subject:
                return True

        return False

    def auto_link_email(self, email_row, user_id):
        """Auto-link a ProcessedEmail to matching customers by domain or content."""
        try:
            customers = self.db.session.query(Customer).filter(
                Customer.user_id == user_id,
            ).all()

            for customer in customers:
                customer_email = customer.email.strip().lower() if customer.email else None
                customer_domain = self._extract_domain(customer_email) if customer_email else None

                if self._matches_customer(email_row, customer, customer_email, customer_domain):
                    existing = self.db.session.query(CustomerEmail).filter_by(
                        customer_id=customer.id, email_id=email_row.id
                    ).first()
                    if not existing:
                        link = CustomerEmail(
                            customer_id=customer.id,
                            email_id=email_row.id,
                            linked_by='auto',
                        )
                        self.db.session.add(link)
                        self.db.session.commit()
                        self.logger.info(f"Auto-linked email {email_row.id} to customer {customer.id}")

        except Exception as e:
            self.logger.error(f"Error auto-linking email: {str(e)}")
            # Non-critical, don't re-raise

    def batch_auto_link_for_customer(self, customer_id, user_id):
        """Scan all existing processed emails and auto-link matching ones to a customer."""
        try:
            customer = self.db.session.query(Customer).filter_by(
                id=customer_id, user_id=user_id
            ).first()
            if not customer:
                return 0

            customer_email = customer.email.strip().lower() if customer.email else None
            customer_domain = self._extract_domain(customer_email) if customer_email else None

            # Need at least one matching signal
            if not customer_email and not customer.name and not customer.company:
                return 0

            emails = self.db.session.query(ProcessedEmails).filter(
                ProcessedEmails.user_id == user_id,
            ).all()

            linked_count = 0
            for email_row in emails:
                if self._matches_customer(email_row, customer, customer_email, customer_domain):
                    existing = self.db.session.query(CustomerEmail).filter_by(
                        customer_id=customer_id, email_id=email_row.id
                    ).first()
                    if not existing:
                        link = CustomerEmail(
                            customer_id=customer_id,
                            email_id=email_row.id,
                            linked_by='auto',
                        )
                        self.db.session.add(link)
                        linked_count += 1

            if linked_count > 0:
                self.db.session.commit()
                self.logger.info(f"Batch auto-linked {linked_count} emails to customer {customer_id}")

            return linked_count

        except Exception as e:
            self.db.session.rollback()
            self.logger.error(f"Error batch auto-linking emails: {str(e)}")
            return 0
