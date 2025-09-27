from flask import g
from sqlalchemy import func, extract, and_, or_
from sqlalchemy.orm import joinedload
from models.freelance_management import Invoice, Customer, InvoiceStatusEnum, InvoicePayment, CurrencyEnum
from services.Base_Service import BaseService
from utils.logger import Logger
from datetime import datetime, timedelta


class DashboardService(BaseService):
    _instance = None

    def __new__(cls):
        if cls._instance is None:
            cls._instance = super(DashboardService, cls).__new__(cls)
            cls.logger = Logger(__name__).get_logger()
        return cls._instance

    def __init__(self):
        super().__init__()

    def get_dashboard_analytics(self):
        try:
            user_id = g.get('firebase_id')
            if not user_id:
                raise ValueError("User ID is required")

            # Get all paid invoices with payments for calculation
            paid_invoices = self.db.session.query(Invoice).options(
                joinedload(Invoice.payments),
                joinedload(Invoice.customer)
            ).filter_by(
                user_id=user_id,
                status=InvoiceStatusEnum.paid
            ).all()

            # Calculate total earnings in INR (use payment amounts which are already in INR)
            total_earnings = sum(sum(float(payment.amount_received) for payment in invoice.payments)
                               for invoice in paid_invoices if invoice.payments)

            # Monthly earnings (current month paid invoices in INR)
            current_month = datetime.now().month
            current_year = datetime.now().year
            current_month_paid = [inv for inv in paid_invoices 
                                if inv.issue_date and inv.issue_date.month == current_month 
                                and inv.issue_date.year == current_year]
            
            monthly_earnings = sum(sum(float(payment.amount_received) for payment in invoice.payments)
                                 for invoice in current_month_paid if invoice.payments)

            # Pending amount - estimate all unpaid invoices in INR equivalent
            unpaid_invoices = self.db.session.query(Invoice).filter(
                Invoice.user_id == user_id,
                or_(Invoice.status == InvoiceStatusEnum.sent, Invoice.status == InvoiceStatusEnum.overdue)
            ).all()
            
            # For simplicity, assume unpaid amounts are worth their face value converted to INR
            # (in real scenario you'd use exchange rates)
            pending_amount = sum(float(invoice.total) for invoice in unpaid_invoices)

            # Completed projects = count of paid invoices
            completed_projects = len(paid_invoices)

            # Active clients = unique clients with invoices in last 90 days
            recent_date = datetime.now() - timedelta(days=90)
            active_clients = self.db.session.query(Invoice.customer_id).filter(
                Invoice.user_id == user_id,
                Invoice.issue_date >= recent_date,
                Invoice.customer_id.isnot(None)
            ).distinct().count()

            # Earnings by month (last 12 months, paid only, in INR)
            earnings_by_month = []
            current = datetime.now()
            for i in range(11, -1, -1):
                # Properly calculate months by adjusting month/year
                target_month = current.month - i
                target_year = current.year
                
                # Handle year rollover
                while target_month <= 0:
                    target_month += 12
                    target_year -= 1
                
                month_invoices = [inv for inv in paid_invoices 
                                if inv.issue_date and inv.issue_date.month == target_month 
                                and inv.issue_date.year == target_year]
                
                # Use payment amounts directly (already in INR)
                month_earnings = sum(sum(float(payment.amount_received) for payment in invoice.payments)
                                   for invoice in month_invoices if invoice.payments)
                
                earnings_by_month.append({
                    "month": f"{target_year:04d}-{target_month:02d}",
                    "earnings": float(month_earnings)
                })

            # Earnings by client (paid invoices only, using payment amounts in INR)
            all_invoices = self.db.session.query(Invoice).options(
                joinedload(Invoice.payments),
                joinedload(Invoice.customer)
            ).filter_by(user_id=user_id).all()
            
            client_earnings = {}
            for invoice in all_invoices:
                client_name = invoice.customer.name if invoice.customer else invoice.to_name
                if client_name not in client_earnings:
                    client_earnings[client_name] = 0
                
                if invoice.status == InvoiceStatusEnum.paid and invoice.payments:
                    # Use payment amounts (already in INR)
                    earnings = sum(float(payment.amount_received) for payment in invoice.payments)
                    client_earnings[client_name] += earnings

            earnings_by_client = [
                {"client": client, "earnings": earnings}
                for client, earnings in sorted(client_earnings.items(), 
                                             key=lambda x: x[1], reverse=True)[:10]
            ]

            # Earnings by client combined (paid + unpaid with currency conversion to INR)
            # Simple exchange rates (should be made dynamic in production)
            exchange_rates = {
                'USD': 83.0,  # 1 USD = 83 INR (approximate)
                'GBP': 105.0,  # 1 GBP = 105 INR (approximate)
                'INR': 1.0
            }
            
            client_earnings_combined = {}
            for invoice in all_invoices:
                client_name = invoice.customer.name if invoice.customer else invoice.to_name
                if client_name not in client_earnings_combined:
                    client_earnings_combined[client_name] = 0
                
                if invoice.status == InvoiceStatusEnum.paid and invoice.payments:
                    # For paid invoices, use payment amounts (already in INR)
                    earnings_inr = sum(float(payment.amount_received) for payment in invoice.payments)
                else:
                    # For unpaid invoices, convert using exchange rates
                    currency = invoice.currency.value if hasattr(invoice.currency, 'value') else str(invoice.currency)
                    exchange_rate = exchange_rates.get(currency, 1.0)
                    earnings_inr = float(invoice.total) * exchange_rate
                
                client_earnings_combined[client_name] += earnings_inr

            earnings_by_client_combined = [
                {"client": client, "earnings": int(earnings)}
                for client, earnings in sorted(client_earnings_combined.items(), 
                                             key=lambda x: x[1], reverse=True)[:10]
            ]

            # Recent invoices (last 5 from all statuses)
            recent_invoices = self.db.session.query(Invoice).options(
                joinedload(Invoice.payments),
                joinedload(Invoice.customer)
            ).filter_by(user_id=user_id).order_by(Invoice.created_at.desc()).limit(5).all()

            recent_invoices_formatted = []
            for invoice in recent_invoices:
                # Calculate paid amount in INR
                paid_amount = None
                if invoice.status == InvoiceStatusEnum.paid and invoice.payments:
                    paid_amount = sum(float(payment.amount_received) for payment in invoice.payments)

                recent_invoices_formatted.append({
                    "invoiceNumber": invoice.invoice_number,
                    "clientName": invoice.customer.name if invoice.customer else invoice.to_name,
                    "projectName": invoice.project_name,
                    "amount": float(invoice.total),
                    "paidAmount": paid_amount,
                    "currency": invoice.currency.value if hasattr(invoice.currency, 'value') else str(invoice.currency),
                    "status": invoice.status.value if hasattr(invoice.status, 'value') else str(invoice.status),
                    "date": invoice.issue_date.strftime('%Y-%m-%d') if invoice.issue_date else ""
                })

            # Unpaid by currency breakdown
            unpaid_by_currency = {}
            for invoice in unpaid_invoices:
                currency = invoice.currency.value if hasattr(invoice.currency, 'value') else str(invoice.currency)
                if currency not in unpaid_by_currency:
                    unpaid_by_currency[currency] = {"amount": 0, "count": 0}
                
                unpaid_by_currency[currency]["amount"] += float(invoice.total)
                unpaid_by_currency[currency]["count"] += 1

            unpaid_by_currency_formatted = [
                {
                    "currency": currency,
                    "amount": data["amount"],
                    "count": data["count"]
                }
                for currency, data in unpaid_by_currency.items()
            ]

            return {
                "totalEarnings": int(total_earnings),
                "monthlyEarnings": int(monthly_earnings),
                "completedProjects": completed_projects,
                "activeClients": active_clients,
                "earningsByMonth": earnings_by_month,
                "earningsByClient": earnings_by_client,
                "earningsByClientCombined": earnings_by_client_combined,
                "recentInvoices": recent_invoices_formatted,
                "unpaidByCurrency": unpaid_by_currency_formatted
            }

        except Exception as e:
            self.logger.error(f"Error fetching dashboard analytics: {str(e)}")
            raise

    def get_earnings_by_date_range(self, start_date, end_date):
        try:
            user_id = g.get('firebase_id')
            if not user_id:
                raise ValueError("User ID is required")

            start_dt = datetime.strptime(start_date, '%Y-%m-%d').date()
            end_dt = datetime.strptime(end_date, '%Y-%m-%d').date()

            earnings = self.db.session.query(func.sum(Invoice.total)).filter(
                Invoice.user_id == user_id,
                Invoice.status == InvoiceStatusEnum.paid,
                Invoice.issue_date.between(start_dt, end_dt)
            ).scalar() or 0

            # Get detailed breakdown by invoice
            invoices = self.db.session.query(Invoice).filter(
                Invoice.user_id == user_id,
                Invoice.status == InvoiceStatusEnum.paid,
                Invoice.issue_date.between(start_dt, end_dt)
            ).order_by(Invoice.issue_date.desc()).all()

            invoice_details = []
            for invoice in invoices:
                invoice_dict = {key: value for key, value in invoice.__dict__.items() 
                               if key != '_sa_instance_state'}
                if invoice.customer:
                    invoice_dict['customer_name'] = invoice.customer.name
                invoice_details.append(invoice_dict)

            return {
                "total_earnings": float(earnings),
                "start_date": start_date,
                "end_date": end_date,
                "invoice_count": len(invoice_details),
                "invoices": invoice_details
            }

        except Exception as e:
            self.logger.error(f"Error fetching earnings by date range: {str(e)}")
            raise