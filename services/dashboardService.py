from decimal import Decimal
from flask import g
from sqlalchemy import func, extract, and_, or_
from sqlalchemy.orm import joinedload
from models.freelance_management import Invoice, Customer, InvoiceStatusEnum, InvoicePayment, CurrencyEnum
from services.Base_Service import BaseService
from services.earningsService import (
    get_earnings_in_inr, get_earnings_by_month, get_earnings_by_client,
    _sum_inr_payments,
)
from services.money_utils import money, q2
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

            from services.currencyService import CurrencyService
            currency_service = CurrencyService()
            session = self.db.session
            now = datetime.now()
            current_month_start = datetime(now.year, now.month, 1).date()

            # ak-lvu A.3 + A.5: canonical earnings function used for every
            # money aggregate. No `int()` truncation; Decimal throughout.
            # Casts to float at the very end for JSON serialization only.

            # Total earnings: Σ payments (INR) of PAID + PARTIALLY_PAID invoices, all-time.
            total_earnings = get_earnings_in_inr(session, user_id)

            # Monthly earnings: same, current month only.
            monthly_earnings = get_earnings_in_inr(
                session, user_id,
                start_date=current_month_start, end_date=now.date(),
            )

            # Earnings by month (last 12, in INR).
            earnings_by_month_data = get_earnings_by_month(
                session, user_id, months_back=12,
            )
            earnings_by_month = [
                {"month": row["month"], "earnings": float(row["earnings"])}
                for row in earnings_by_month_data
            ]

            # Earnings by client (paid only, top 10).
            earnings_by_client_data = get_earnings_by_client(
                session, user_id, limit=10, include_unpaid=False,
            )
            earnings_by_client = [
                {"client": row["client"], "earnings": float(row["earnings"])}
                for row in earnings_by_client_data
            ]

            # Earnings by client combined (paid + unpaid, top 10).
            earnings_by_client_combined_data = get_earnings_by_client(
                session, user_id, limit=10, include_unpaid=True,
                currency_service=currency_service,
            )
            # ak-lvu A.5: NO int() truncation — was
            # `"earnings": int(earnings)` at old L142 dropping the paisa.
            earnings_by_client_combined = [
                {"client": row["client"], "earnings": float(row["earnings"])}
                for row in earnings_by_client_combined_data
            ]

            # Completed projects = count of paid + partially_paid invoices.
            completed_projects = session.query(Invoice).filter(
                Invoice.user_id == user_id,
                Invoice.status.in_((
                    InvoiceStatusEnum.paid, InvoiceStatusEnum.partially_paid,
                )),
            ).count()

            # Active clients = unique clients with invoices in last 90 days
            recent_date = now - timedelta(days=90)
            active_clients = session.query(Invoice.customer_id).filter(
                Invoice.user_id == user_id,
                Invoice.issue_date >= recent_date,
                Invoice.customer_id.isnot(None)
            ).distinct().count()

            # Recent invoices (last 5 from all statuses).
            recent_invoices = session.query(Invoice).options(
                joinedload(Invoice.payments),
                joinedload(Invoice.customer)
            ).filter_by(user_id=user_id).order_by(Invoice.created_at.desc()).limit(5).all()

            recent_invoices_formatted = []
            for invoice in recent_invoices:
                # ak-lvu A.3: canonical — payment sum in INR whenever
                # payments exist, regardless of status. FE renders this
                # as `paidAmount`; None means no payments so far.
                paid_amount = None
                if invoice.payments:
                    paid_amount = float(_sum_inr_payments(invoice.payments))

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

            # Unpaid by currency breakdown (unchanged shape — sums in ORIGINAL
            # currency per-bucket, no cross-currency mixing).
            unpaid_invoices = session.query(Invoice).filter(
                Invoice.user_id == user_id,
                Invoice.status.in_((
                    InvoiceStatusEnum.sent, InvoiceStatusEnum.overdue,
                )),
            ).all()
            unpaid_by_currency: dict = {}
            for invoice in unpaid_invoices:
                currency = invoice.currency.value if hasattr(invoice.currency, 'value') else str(invoice.currency)
                if currency not in unpaid_by_currency:
                    unpaid_by_currency[currency] = {"amount": Decimal("0"), "count": 0}
                unpaid_by_currency[currency]["amount"] += money(invoice.total)
                unpaid_by_currency[currency]["count"] += 1

            unpaid_by_currency_formatted = [
                {
                    "currency": currency,
                    "amount": float(q2(data["amount"])),
                    "count": data["count"]
                }
                for currency, data in unpaid_by_currency.items()
            ]

            # ak-lvu A.5: no int() truncation on top-level totals.
            return {
                "totalEarnings": float(total_earnings),
                "monthlyEarnings": float(monthly_earnings),
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

            # ak-lvu A.3: was `func.sum(Invoice.total)` — sums invoice
            # TOTAL in original currency treating all as INR. Now uses
            # the canonical earnings function which sums payments in INR
            # (correct on multi-currency). Includes partially_paid too
            # so the sum reflects money actually received.
            earnings = get_earnings_in_inr(
                self.db.session, user_id,
                start_date=start_dt, end_date=end_dt,
            )

            # Get detailed breakdown by invoice — paid + partially_paid.
            invoices = self.db.session.query(Invoice).options(
                joinedload(Invoice.payments), joinedload(Invoice.customer),
            ).filter(
                Invoice.user_id == user_id,
                Invoice.status.in_((
                    InvoiceStatusEnum.paid, InvoiceStatusEnum.partially_paid,
                )),
                Invoice.issue_date.between(start_dt, end_dt)
            ).order_by(Invoice.issue_date.desc()).all()

            invoice_details = []
            for invoice in invoices:
                # Per-invoice paid amount in INR (canonical).
                paid_inr = float(_sum_inr_payments(invoice.payments)) if invoice.payments else 0.0
                invoice_dict = {
                    "invoiceNumber": invoice.invoice_number,
                    "projectName": invoice.project_name,
                    "clientName": invoice.customer.name if invoice.customer else invoice.to_name,
                    "amount": float(invoice.total),
                    "paidAmountINR": paid_inr,
                    "currency": invoice.currency.value if hasattr(invoice.currency, 'value') else str(invoice.currency),
                    "status": invoice.status.value if hasattr(invoice.status, 'value') else str(invoice.status),
                    "issueDate": invoice.issue_date.strftime('%Y-%m-%d') if invoice.issue_date else "",
                    "dueDate": invoice.due_date.strftime('%Y-%m-%d') if invoice.due_date else "",
                }
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