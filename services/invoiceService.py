from decimal import Decimal
from flask import g
from sqlalchemy.exc import IntegrityError, SQLAlchemyError, OperationalError, ProgrammingError
from sqlalchemy import func, desc, asc, or_
from sqlalchemy.orm import joinedload
from models.freelance_management import (
    Invoice, InvoiceItem, Customer, InvoicePayment, CurrencyEnum,
    InvoiceStatusEnum, InvoiceCustomField,
)
from services.Base_Service import BaseService
from services.currencyService import CurrencyService, CurrencyUnavailableError
from services.money_utils import (
    money, q2, q4, within_epsilon, sum_money,
    recompute_invoice_totals, _is_migration_gap, MoneyError,
)
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
            items_data = invoice_data.get('items', [])

            # ak-lvu A.2 — server-side recompute-and-validate.
            # Ignore client-supplied subtotal/tax_amount/total; compute
            # from line items + tax_rate as the authoritative values.
            # If client sent values that DISAGREE beyond ₹0.01 epsilon,
            # log a warning so we can spot broken clients — but always
            # store server values.
            tax_rate_pct = tax_data.get('rate', 0)
            server_subtotal, server_tax_amount, server_total = recompute_invoice_totals(
                [{"quantity": item.get('quantity', 0), "rate": item.get('rate', 0)}
                 for item in items_data],
                tax_rate=tax_rate_pct,
            )
            self._warn_if_client_disagrees(
                invoice_data.get('subtotal'), server_subtotal, "subtotal",
                invoice_data.get('invoiceNumber'),
            )
            self._warn_if_client_disagrees(
                tax_data.get('amount'), server_tax_amount, "tax_amount",
                invoice_data.get('invoiceNumber'),
            )
            self._warn_if_client_disagrees(
                invoice_data.get('total'), server_total, "total",
                invoice_data.get('invoiceNumber'),
            )

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
                # ak-lvu A.2 + A.5: Decimal-quantized server-recomputed values.
                subtotal=server_subtotal,
                tax_rate=q2(tax_rate_pct) if tax_rate_pct else Decimal("0.00"),
                tax_amount=server_tax_amount,
                total=server_total,
                notes=invoice_data.get('notes'),
                terms=invoice_data.get('terms'),
                # ak-lvu A.4: client-supplied status honoured on CREATE
                # (e.g. LLM importing a historical paid invoice can set
                # status='paid'). On UPDATE, status is server-computed
                # from payments — see update_invoice.
                status=invoice_data.get('status', 'draft'),
                currency=CurrencyEnum(invoice_data.get('currency', 'USD'))
            )

            self.db.session.add(invoice)
            self.db.session.flush()

            if items_data:
                for idx, item_data in enumerate(items_data):
                    qty = money(item_data.get('quantity', 1))
                    rate = money(item_data.get('rate', 0))
                    # ak-lvu A.2: recompute per-item amount too — never
                    # trust client-supplied item.amount.
                    amount = q2(qty * rate)
                    item = InvoiceItem(
                        invoice_id=invoice.id,
                        description=item_data.get('description', ''),
                        quantity=q2(qty),
                        rate=q2(rate),
                        amount=amount,
                        item_order=idx + 1  # Use index for order
                    )
                    self.db.session.add(item)

            # Handle payment if provided
            if 'payment' in invoice_data and invoice_data['payment']:
                self._replace_payment(invoice.id, invoice_data['payment'])

            # Handle custom fields if provided
            if 'customFields' in invoice_data and invoice_data['customFields']:
                self._create_custom_fields(invoice.id, invoice_data['customFields'])

            # ak-lvu A.4 + v2 F-4 + F-12: derive status from payments
            # post-write only when a payment was supplied.
            #   * No-payment + client-declared status → preserve
            #     (historical-import case; recompute would mis-flip).
            #   * With-payment + declared status='draft' → auto-transition
            #     to sent BEFORE recompute so the money-received-in-draft
            #     invoice becomes visible in earnings + status derives
            #     properly (F-12: money received but invisible closed).
            #   * With-payment + declared status='sent'/other → recompute
            #     from Σ payments vs total (may end up partially_paid /
            #     paid).
            if 'payment' in invoice_data and invoice_data['payment']:
                if invoice.status == InvoiceStatusEnum.draft:
                    self.logger.info(
                        f"ak-lvu v2 F-12: create-with-payment on draft "
                        f"{invoice.invoice_number} — auto-transitioning "
                        f"to sent so recompute can derive final status"
                    )
                    invoice.status = InvoiceStatusEnum.sent
                self._recompute_invoice_status(invoice)
            else:
                self.logger.debug(
                    f"ak-lvu v2 F-4: skipping post-create recompute for "
                    f"{invoice.invoice_number}: no payment supplied "
                    f"(honouring client-declared status {invoice.status})"
                )

            self.db.session.commit()
            self.logger.info(f"Invoice created successfully: {invoice.id}")

            # Return formatted invoice
            return self._format_invoice(invoice)

        except Exception as e:
            self.db.session.rollback()
            self.logger.error(f"Error creating invoice: {str(e)}")
            raise

    def _warn_if_client_disagrees(self, client_value, server_value, field, invoice_number):
        """ak-lvu A.2: log — do NOT reject — when client-supplied money
        diverges from server recompute beyond the ₹0.01 epsilon.

        Never rejects (LLM / mail-pipeline may omit fields entirely,
        which is fine because server recompute fills them). Only surfaces
        the divergence so a broken client can be spotted in prod logs.
        """
        if client_value is None:
            return  # omitted is fine; server value stored
        try:
            client_d = money(client_value)
        except MoneyError:
            self.logger.warning(
                f"ak-lvu A.2: invoice={invoice_number} field={field}: "
                f"client sent non-numeric {client_value!r}, using server "
                f"value {server_value}"
            )
            return
        if not within_epsilon(client_d, server_value):
            self.logger.warning(
                f"ak-lvu A.2: invoice={invoice_number} field={field}: "
                f"client sent {client_d}, server recomputed {server_value} "
                f"(diverges beyond ₹0.01); overwriting with server value"
            )

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
            # ak-lvu A.4 + v2 F-3: client-supplied status on PUT.
            # v1 ignored every status client-side (draft-lock regression:
            # no draft→sent path). v2 accepts the SPECIFIC forward
            # transition draft→sent (a declaration server can't derive)
            # + reveals-only-then draft→partially_paid or draft→paid if
            # the same PUT supplies a payment. All other client-supplied
            # statuses are still IGNORED — `_recompute_invoice_status`
            # owns everything from sent onward.
            if 'status' in invoice_data:
                client_status = str(invoice_data['status'])
                current_status = (
                    invoice.status.value if hasattr(invoice.status, 'value')
                    else str(invoice.status)
                )
                if current_status == 'draft' and client_status == 'sent':
                    # F-3: allow explicit draft→sent transition. Recompute
                    # will still run at end and can override to
                    # partially_paid / paid if payment was also supplied.
                    invoice.status = InvoiceStatusEnum.sent
                    self.logger.info(
                        f"ak-lvu v2 F-3: draft→sent transition accepted "
                        f"for {invoice_number}"
                    )
                elif current_status == 'draft' and client_status == 'draft':
                    # No-op stay-as-draft.
                    pass
                else:
                    self.logger.info(
                        f"ak-lvu A.4: ignoring client-supplied status "
                        f"{client_status!r} on PUT for {invoice_number} "
                        f"(current={current_status}); status is "
                        f"server-computed from payments"
                    )

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

            # Handle currency FIRST so the payment path picks up the new value.
            if 'currency' in invoice_data:
                invoice.currency = CurrencyEnum(invoice_data['currency'])

            # Handle tax rate (percent) — the AMOUNT is server-recomputed if
            # items are present in this PUT (below).
            if tax_data and 'rate' in tax_data:
                invoice.tax_rate = q2(tax_data['rate'])

            # ak-lvu A.2 — server-side recompute-and-validate on PUT.
            # If items OR tax_rate are in the update, re-run the recompute
            # against the resulting item set (new if provided, else the
            # current ORM rows). Client-supplied subtotal/total/tax_amount
            # are logged-and-overwritten if they diverge beyond ₹0.01.
            items_changed = 'items' in invoice_data
            tax_changed = bool(tax_data and 'rate' in tax_data)

            if items_changed:
                # Delete existing items
                self.db.session.query(InvoiceItem).filter_by(invoice_id=invoice.id).delete()
                self.db.session.flush()
                # Add new items with Decimal-quantized values + server-recomputed amount.
                for idx, item_data in enumerate(invoice_data['items']):
                    qty = money(item_data.get('quantity', 1))
                    rate = money(item_data.get('rate', 0))
                    amount = q2(qty * rate)
                    item = InvoiceItem(
                        invoice_id=invoice.id,
                        description=item_data.get('description', ''),
                        quantity=q2(qty),
                        rate=q2(rate),
                        amount=amount,
                        item_order=idx + 1
                    )
                    self.db.session.add(item)
                self.db.session.flush()

            # ak-lvu v2 F-13: divergence warnings fire on EVERY PUT that
            # touches subtotal/tax_amount/total, not just when items/tax
            # change. A PUT that just re-sends the totals unchanged
            # against a server-computed baseline should log if the client
            # value drifted (broken client rounding, LLM hallucination).
            # Actual overwrite still only happens when items/tax change
            # (server-authoritative recompute).
            if items_changed or tax_changed:
                current_items = self.db.session.query(InvoiceItem).filter_by(
                    invoice_id=invoice.id
                ).all()
                server_subtotal, server_tax_amount, server_total = recompute_invoice_totals(
                    current_items, tax_rate=invoice.tax_rate,
                )
                invoice.subtotal = server_subtotal
                invoice.tax_amount = server_tax_amount
                invoice.total = server_total
            else:
                # No items/tax change → recompute against CURRENT stored
                # items just for divergence logging (no write-back).
                current_items = list(invoice.items or [])
                if current_items:
                    server_subtotal, server_tax_amount, server_total = recompute_invoice_totals(
                        current_items, tax_rate=invoice.tax_rate,
                    )
                else:
                    server_subtotal = money(invoice.subtotal)
                    server_tax_amount = money(invoice.tax_amount or 0)
                    server_total = money(invoice.total)

            # v2 F-13: log divergence on EVERY PUT, not just recompute path.
            self._warn_if_client_disagrees(
                invoice_data.get('subtotal'), server_subtotal, "subtotal",
                invoice.invoice_number,
            )
            self._warn_if_client_disagrees(
                tax_data.get('amount') if tax_data else None,
                server_tax_amount, "tax_amount", invoice.invoice_number,
            )
            self._warn_if_client_disagrees(
                invoice_data.get('total'), server_total, "total",
                invoice.invoice_number,
            )

            # Handle payment if provided (ak-lvu A.1: idempotent).
            if 'payment' in invoice_data and invoice_data['payment']:
                self._replace_payment(invoice.id, invoice_data['payment'])

            # Handle custom fields if provided
            if 'customFields' in invoice_data:
                # Delete existing custom fields
                self.db.session.query(InvoiceCustomField).filter_by(invoice_id=invoice.id).delete()

                # Add new custom fields
                if invoice_data['customFields']:
                    self._create_custom_fields(invoice.id, invoice_data['customFields'])

            # ak-lvu A.4: recompute status from Σ(inr_amount) vs total.
            # This runs after all mutations so partially_paid / paid /
            # sent transitions reflect the final state, never a
            # mid-update snapshot.
            self._recompute_invoice_status(invoice)

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
        """Format invoice to match InvoiceData interface.

        ak-lvu wire contract additions (per bead):
          - payment.originalAmount / originalCurrency / inrAmount / fxRate
          - payment.fxRateSource / convertedAt
          - top-level totalPaidINR + balanceDueINR
          - status may include 'partially_paid'

        Tolerant of pre-migration payment rows (NULL FX metadata) via
        `_safe_read_payment_fx`.
        """
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

        # ak-lvu A.1: payment shape carries FX audit metadata. Legacy
        # amountReceived kept as an alias of inrAmount so old FE builds
        # keep rendering while ak-awp propagates the new shape.
        payment = None
        if invoice.payments:
            single_payment = invoice.payments[0]
            fx = self._safe_read_payment_fx(single_payment)
            payment = {
                "id": single_payment.id,
                "paymentMethod": single_payment.payment_method,
                # New authoritative fields:
                "originalAmount": fx["original_amount"],
                "originalCurrency": fx["original_currency"],
                "inrAmount": fx["inr_amount"],
                "fxRate": fx["fx_rate"],
                "fxRateSource": fx["fx_rate_source"],
                "convertedAt": fx["converted_at"],
                # Legacy alias (kept for pre-ak-awp FE builds):
                "amountReceived": fx["inr_amount"] if fx["inr_amount"] is not None else float(single_payment.amount_received or 0),
                "breakdown": single_payment.breakdown or {},
                "paymentDate": single_payment.payment_date.strftime('%Y-%m-%d') if single_payment.payment_date else None,
                "notes": single_payment.notes
            }

        # ak-lvu A.4: totalPaidINR + balanceDueINR surfaced to FE so the
        # partial-payment case renders correctly. Sum is over INR values
        # so mixing currencies would be nonsensical anyway.
        paid_inr = self._sum_paid_inr_safe(invoice.payments) if invoice.payments else Decimal("0")

        # balanceDueINR = total_in_INR - paid_inr. Convert invoice.total
        # to INR via read-side (allow_fallback=True on read paths so a
        # transient FX-API outage doesn't 500 the GET). Vintage source
        # not surfaced here — FE just needs the number.
        currency_str = invoice.currency.value if hasattr(invoice.currency, 'value') else str(invoice.currency)
        if currency_str == 'INR':
            total_inr = money(invoice.total)
        else:
            try:
                total_inr = self.currency_service.convert_to_inr_with_source(
                    money(invoice.total), currency_str, allow_fallback=True,
                )["inr_amount"]
            except Exception:
                total_inr = None
        balance_due_inr = None
        if total_inr is not None:
            balance_due_inr = float(q2(total_inr - paid_inr))

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
            # ak-lvu A.4 wire contract:
            "totalPaidINR": float(q2(paid_inr)),
            "balanceDueINR": balance_due_inr,
            "currency": invoice.currency.value if hasattr(invoice.currency, 'value') else str(invoice.currency),
            "payment": payment,
            "notes": invoice.notes,
            "terms": invoice.terms,
            "status": invoice.status.value if hasattr(invoice.status, 'value') else str(invoice.status),
            "createdAt": invoice.created_at.strftime('%Y-%m-%dT%H:%M:%S.%fZ') if invoice.created_at else None,
            "updatedAt": invoice.updated_at.strftime('%Y-%m-%dT%H:%M:%S.%fZ') if invoice.updated_at else None
        }

        return formatted_invoice

    def _safe_read_payment_fx(self, payment) -> dict:
        """Read the 6 FX-audit columns tolerant of pre-migration rows.

        Returns a dict with each of the 6 fields, using None if the
        column is missing (migration gap) or the row hasn't been
        rewritten since the ALTER landed. Legacy rows: `original_amount`
        falls back to `amount_received` (the pre-ak-lvu shape), and
        `original_currency` derives from the invoice.
        """
        result = {
            "original_amount": None,
            "original_currency": None,
            "inr_amount": None,
            "fx_rate": None,
            "fx_rate_source": None,
            "converted_at": None,
        }
        try:
            result["original_amount"] = (
                float(payment.original_amount) if payment.original_amount is not None else None
            )
            result["original_currency"] = payment.original_currency
            result["inr_amount"] = (
                float(payment.inr_amount) if payment.inr_amount is not None else None
            )
            result["fx_rate"] = (
                float(payment.fx_rate) if payment.fx_rate is not None else None
            )
            result["fx_rate_source"] = payment.fx_rate_source
            result["converted_at"] = (
                payment.converted_at.strftime('%Y-%m-%dT%H:%M:%S.%fZ')
                if payment.converted_at else None
            )
        except (OperationalError, ProgrammingError) as exc:
            if not _is_migration_gap(exc):
                raise
            self.logger.debug(
                "ak-lvu: pre-migration payment row, returning None FX metadata"
            )
        # Legacy backfill: if FX metadata absent but amount_received set,
        # surface amount_received as both inr_amount + original_amount
        # (they were equal on pre-ak-lvu writes — the AC-1 shape was the
        # bug on RE-writes, not the initial shape).
        if result["inr_amount"] is None and payment.amount_received is not None:
            result["inr_amount"] = float(payment.amount_received)
            if result["original_amount"] is None:
                result["original_amount"] = float(payment.amount_received)
        return result

    def _replace_payment(self, invoice_id, payment_data):
        """ak-lvu A.1 — idempotent payment replacement.

        Semantics change vs pre-ak-lvu:
          * Payload `amountReceived` is now the ORIGINAL currency amount
            (what the user typed / the mail said), NOT a pre-converted INR
            value. Payload MAY include `originalCurrency` — if omitted,
            defaults to invoice.currency (the previous behaviour).
          * Every payment stores 6 FX-metadata cols so round-tripping
            through _format_invoice → PUT never re-converts an already-
            INR value as invoice-currency (the AC-1 CRITICAL).
          * If an existing payment already has FX metadata AND the payload
            matches (same id + originalAmount + originalCurrency), we
            preserve the row instead of delete+recreate. That preserves
            fx_rate + converted_at as historical audit values across a
            no-op PUT.
        """
        invoice = self.db.session.query(Invoice).filter_by(id=invoice_id).first()
        if not invoice:
            raise ValueError("Invoice not found")

        existing_payment = self.db.session.query(InvoicePayment).filter_by(
            invoice_id=invoice_id
        ).first()

        # Parse payload.
        payload_id = payment_data.get('id')
        payload_original_amount = payment_data.get('originalAmount')
        payload_original_currency = payment_data.get('originalCurrency')
        payload_amount_received = payment_data.get('amountReceived')

        # ── ak-lvu v2 F-1 (CRITICAL) — legacy-shape safety guard ──────
        # Pre-v2 flaw: when the caller (LLM via FREELANCE_SYSTEM_PROMPT,
        # or a legacy FE build) sends `payment: {amountReceived: X}`
        # WITHOUT `originalAmount` / `originalCurrency`, the code fell
        # back to treating `amountReceived` as invoice-currency and
        # re-converted. But `_format_invoice`'s legacy alias also
        # returns the STORED INR value under `amountReceived` — so a
        # routine PUT-back of an unchanged returned payload triggered
        # ~83× compounding on the first round-trip. AC-1 lived here.
        #
        # v2 guard: if the caller omits `originalAmount` AND there's an
        # existing payment row AND the payload's `amountReceived`
        # matches the existing INR-side value (inr_amount, or legacy
        # amount_received), treat this as a no-op / metadata-only PUT.
        # Preserve original_amount + original_currency + fx_rate +
        # fx_rate_source + converted_at from the existing row untouched.
        # If the amount VALUE differs from the existing INR-side, we
        # can't safely infer intent — reject loudly and require
        # `originalAmount` + `originalCurrency` explicitly.
        if payload_original_amount is None and existing_payment is not None:
            existing_inr = None
            try:
                existing_inr = (
                    money(existing_payment.inr_amount)
                    if existing_payment.inr_amount is not None
                    else None
                )
            except (OperationalError, ProgrammingError) as exc:
                if not _is_migration_gap(exc):
                    raise
            if existing_inr is None and existing_payment.amount_received is not None:
                existing_inr = money(existing_payment.amount_received)

            payload_inr = None
            if payload_amount_received is not None:
                try:
                    payload_inr = money(payload_amount_received)
                except MoneyError:
                    payload_inr = None

            if (
                existing_inr is not None
                and payload_inr is not None
                and within_epsilon(existing_inr, payload_inr)
            ):
                # No-op / metadata-only PUT — preserve FX metadata.
                self.logger.info(
                    f"ak-lvu v2 F-1: legacy amountReceived-only PUT "
                    f"detected (value {payload_inr} matches existing "
                    f"inr_amount) — treating as no-op, preserving FX "
                    f"metadata for {existing_payment.id}"
                )
                if 'paymentMethod' in payment_data:
                    existing_payment.payment_method = payment_data['paymentMethod']
                if 'paymentDate' in payment_data:
                    existing_payment.payment_date = (
                        datetime.strptime(payment_data['paymentDate'], '%Y-%m-%d').date()
                        if payment_data['paymentDate'] else None
                    )
                if 'notes' in payment_data:
                    existing_payment.notes = payment_data['notes']
                if 'breakdown' in payment_data:
                    existing_payment.breakdown = payment_data['breakdown']
                return existing_payment
            elif payload_inr is not None:
                # Value differs from stored INR-side but caller only sent
                # legacy shape. Cannot infer whether this is invoice-
                # currency or a new INR value — refuse.
                raise ValueError(
                    "_replace_payment (ak-lvu v2 F-1): payment payload "
                    "has `amountReceived` but no `originalAmount` / "
                    "`originalCurrency`. To CHANGE the payment amount, "
                    "provide explicit `originalAmount` + "
                    "`originalCurrency` (bead ak-lvu wire contract). "
                    "Legacy `amountReceived`-only PUTs are only accepted "
                    "as no-op metadata patches (value matches stored "
                    f"inr_amount). Got amountReceived={payload_inr}, "
                    f"stored inr_amount={existing_inr}."
                )

        # Fresh write / explicit new payment: originalAmount is the
        # authoritative field. Fall back to amountReceived ONLY when no
        # existing payment exists AND the invoice currency is INR (in
        # that case there's no double-convert risk because INR→INR is
        # identity). All other legacy-shape cases handled above.
        original_amount_raw = (
            payload_original_amount
            if payload_original_amount is not None
            else payload_amount_received
        )
        if original_amount_raw is None:
            raise ValueError("_replace_payment: originalAmount (or amountReceived) required")
        original_amount = money(original_amount_raw)
        original_currency = str(
            payload_original_currency
            or (invoice.currency.value if hasattr(invoice.currency, 'value') else str(invoice.currency))
        ).upper()

        # ── ak-lvu A.1 idempotency check ─────────────────────────────
        # If the payload matches the existing row (same id + same
        # original_amount + same original_currency), this is a no-op PUT
        # (e.g. status-change or notes-edit round-trip). Preserve the
        # row + FX metadata, only refresh mutable fields (payment_method,
        # payment_date, notes, breakdown).
        if existing_payment and payload_id and payload_id == existing_payment.id:
            same_amount = False
            try:
                same_amount = (
                    existing_payment.original_amount is not None
                    and money(existing_payment.original_amount) == q2(original_amount)
                    and (existing_payment.original_currency or "").upper() == original_currency
                )
            except (OperationalError, ProgrammingError) as exc:
                if not _is_migration_gap(exc):
                    raise
                # Pre-migration row: cannot verify FX metadata match, so
                # fall through to the delete-and-recreate path. That's
                # fine — pre-migration rows will get FX metadata on the
                # first PUT after the ALTER lands.
                self.logger.warning(
                    "ak-lvu: pre-migration payment row, delete+recreate "
                    "to gain FX metadata"
                )
            if same_amount:
                self.logger.info(
                    f"ak-lvu A.1: idempotent payment PUT for {payload_id} — "
                    f"preserving FX metadata (fx_rate stays "
                    f"{existing_payment.fx_rate}, converted_at stays "
                    f"{existing_payment.converted_at})"
                )
                existing_payment.payment_method = payment_data.get(
                    'paymentMethod', existing_payment.payment_method
                )
                if 'paymentDate' in payment_data:
                    existing_payment.payment_date = (
                        datetime.strptime(payment_data['paymentDate'], '%Y-%m-%d').date()
                        if payment_data['paymentDate'] else None
                    )
                if 'notes' in payment_data:
                    existing_payment.notes = payment_data['notes']
                if 'breakdown' in payment_data:
                    existing_payment.breakdown = payment_data['breakdown']
                return existing_payment

        # ── normal path: delete existing (if any) + write fresh row ──
        if existing_payment:
            self.db.session.delete(existing_payment)
            self.db.session.flush()

        # ak-lvu A.6: fail-closed FX conversion for writes. On API failure,
        # raises CurrencyUnavailableError which bubbles up as a clear
        # "FX rate unavailable, retry later" — no silent 2024-vintage
        # fallback that would enshrine an old rate as if it were today's.
        fx_result = self.currency_service.convert_to_inr_with_source(
            original_amount, original_currency, allow_fallback=False,
        )

        payment = InvoicePayment(
            invoice_id=invoice_id,
            payment_method=payment_data.get('paymentMethod'),
            # amount_received stays populated (legacy readers still see it)
            # AND inr_amount stays populated (new readers use the explicit
            # column). Both mirror the same value.
            amount_received=fx_result["inr_amount"],
            inr_amount=fx_result["inr_amount"],
            original_amount=q2(original_amount),
            original_currency=original_currency,
            fx_rate=fx_result["fx_rate"],
            fx_rate_source=fx_result["fx_rate_source"],
            converted_at=fx_result["converted_at"],
            payment_date=(
                datetime.strptime(payment_data['paymentDate'], '%Y-%m-%d').date()
                if payment_data.get('paymentDate') else None
            ),
            notes=payment_data.get('notes'),
            breakdown=payment_data.get('breakdown', {})
        )
        self.db.session.add(payment)
        return payment

    def _recompute_invoice_status(self, invoice):
        """ak-lvu A.4 + v2 F-2 — vintage-free status derivation.

        v1 mixed FX vintages: Σ(payment.inr_amount at capture-time rate)
        vs total_inr at TODAY's rate. Any FX drift between when the
        payment was made and when Overseer edits an unrelated field weeks
        later would spuriously flip paid → partially_paid.

        v2 policy:
          * **Same-currency path (common case)** — every payment's
            original_currency matches invoice.currency AND all
            original_amount values are populated → compare in that
            original currency (Σ original_amount vs invoice.total). No
            FX involved; vintage-free by construction.
          * **Cross-currency / pre-migration fallback** — compare in
            INR space using the sum of PAYMENT-TIME fx_rate-derived
            inr_amount vs a total_inr computed from the WEIGHTED
            payment-time rate (approximated as the rate on the most
            recent payment). If no payments exist, invoice.total is
            converted at today's rate (allow_fallback) but that only
            affects the "still sent / already overdue" branch which
            doesn't depend on the paid_inr threshold anyway.
        """
        # Draft stays draft. Everything else is dynamic.
        if invoice.status == InvoiceStatusEnum.draft:
            return

        # Refresh payment collection from DB so any deletes / adds in
        # the current transaction are visible.
        payments = self.db.session.query(InvoicePayment).filter_by(
            invoice_id=invoice.id
        ).all()

        invoice_currency_str = (
            invoice.currency.value if hasattr(invoice.currency, 'value')
            else str(invoice.currency)
        )

        if not payments:
            # No payments — sent or overdue based on due date.
            if invoice.due_date and invoice.due_date < datetime.now().date():
                invoice.status = InvoiceStatusEnum.overdue
            else:
                invoice.status = InvoiceStatusEnum.sent
            return

        # v2 F-2: same-currency fast path — vintage-free.
        all_same_currency = all(
            (p.original_currency or invoice_currency_str).upper() == invoice_currency_str
            and p.original_amount is not None
            for p in payments
        )

        if all_same_currency:
            paid_in_currency = sum_money(p.original_amount for p in payments)
            invoice_total = money(invoice.total)
            if paid_in_currency <= Decimal("0"):
                if invoice.due_date and invoice.due_date < datetime.now().date():
                    invoice.status = InvoiceStatusEnum.overdue
                else:
                    invoice.status = InvoiceStatusEnum.sent
            elif (
                paid_in_currency < invoice_total
                and not within_epsilon(paid_in_currency, invoice_total)
            ):
                invoice.status = InvoiceStatusEnum.partially_paid
            else:
                invoice.status = InvoiceStatusEnum.paid
            return

        # v2 F-2: cross-currency / pre-migration fallback path.
        # Sum inr_amount (payment-time vintage) and compare against
        # invoice.total converted at THE MOST-RECENT PAYMENT'S vintage
        # fx_rate — same vintage on both sides. If invoice is INR,
        # direct compare with no FX.
        paid_inr = self._sum_paid_inr_safe(payments)
        if invoice_currency_str == 'INR':
            total_inr = money(invoice.total)
        else:
            # Pick the most recent payment's stored fx_rate as the
            # comparison-vintage anchor.
            latest = max(
                (p for p in payments if p.fx_rate is not None),
                key=lambda p: p.converted_at or datetime.min,
                default=None,
            )
            if latest is not None and latest.original_currency:
                # Convert invoice.total from invoice_currency to INR via
                # the latest payment's INR-per-original-currency rate.
                # If invoice_currency doesn't match the payment's
                # original_currency, we don't have a clean rate — fall
                # back to today's rate (documented degradation).
                if latest.original_currency.upper() == invoice_currency_str:
                    total_inr = money(invoice.total) * money(latest.fx_rate)
                else:
                    # Mixed-original-currencies genuinely need today's
                    # rate. Documented: this narrow case may show FX
                    # drift; self-heals on next mutation. Kept as the
                    # least-bad option — the alternative is refusing
                    # to compute status at all.
                    try:
                        fx = self.currency_service.convert_to_inr_with_source(
                            money(invoice.total), invoice_currency_str,
                            allow_fallback=True,
                        )
                        total_inr = fx["inr_amount"]
                    except Exception:
                        self.logger.warning(
                            f"ak-lvu v2 F-2: mixed-currency status recompute "
                            f"for {invoice.invoice_number}: no consistent "
                            f"vintage anchor + FX unavailable; leaving "
                            f"status as {invoice.status}"
                        )
                        return
            else:
                # No fx_rate on any payment (pre-migration). Best-effort
                # today's-rate compare, degraded semantics documented.
                try:
                    fx = self.currency_service.convert_to_inr_with_source(
                        money(invoice.total), invoice_currency_str,
                        allow_fallback=True,
                    )
                    total_inr = fx["inr_amount"]
                except Exception as exc:
                    self.logger.warning(
                        f"ak-lvu A.4: status recompute for {invoice.invoice_number}: "
                        f"total-in-INR conversion failed ({exc}); leaving status "
                        f"as {invoice.status}"
                    )
                    return

        if paid_inr <= Decimal("0"):
            if invoice.due_date and invoice.due_date < datetime.now().date():
                invoice.status = InvoiceStatusEnum.overdue
            else:
                invoice.status = InvoiceStatusEnum.sent
        elif paid_inr < total_inr and not within_epsilon(paid_inr, total_inr):
            invoice.status = InvoiceStatusEnum.partially_paid
        else:
            invoice.status = InvoiceStatusEnum.paid

    def _sum_paid_inr_safe(self, payments):
        """Sum payment.inr_amount, tolerant of pre-migration NULL rows.

        Pre-migration rows have inr_amount=NULL and only amount_received
        populated. Fall back to amount_received in that case (matches
        the historical shape where amount_received was already the INR
        value — the AC-1 double-convert bug hurt writes, not reads).
        """
        total = Decimal("0")
        for p in payments:
            try:
                inr = p.inr_amount
            except (OperationalError, ProgrammingError) as exc:
                if _is_migration_gap(exc):
                    inr = None
                else:
                    raise
            if inr is not None:
                total += money(inr)
            elif p.amount_received is not None:
                # Legacy row — treat amount_received as INR (matches
                # pre-ak-lvu storage semantics).
                total += money(p.amount_received)
        return total

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