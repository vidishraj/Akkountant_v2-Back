from sqlalchemy import Column, String, Text, DECIMAL, Integer, DateTime, Boolean, Enum, ForeignKey, JSON, Date, func
from sqlalchemy.dialects.mysql import CHAR, MEDIUMTEXT
from sqlalchemy.orm import relationship
from .Base import Base
import enum
import uuid

def generate_uuid():
    return str(uuid.uuid4())

class InvoiceStatusEnum(enum.Enum):
    draft = 'draft'
    sent = 'sent'
    paid = 'paid'
    overdue = 'overdue'
    # ak-lvu A.4: server-computed intermediate state — payments exist but
    # Σ(inr_amount) < invoice.total. Written by invoiceService's status
    # recompute helper on any payment insert/update/delete. Migration:
    # ALTER TABLE invoices MODIFY status ENUM('draft','sent','paid','overdue','partially_paid') NOT NULL DEFAULT 'draft';
    partially_paid = 'partially_paid'

class CurrencyEnum(enum.Enum):
    USD = 'USD'
    EUR = 'EUR'
    GBP = 'GBP'
    INR = 'INR'
    CAD = 'CAD'
    AUD = 'AUD'
    JPY = 'JPY'

class Customer(Base):
    __tablename__ = 'customers'
    id = Column(CHAR(36), primary_key=True, default=generate_uuid)
    user_id = Column(CHAR(36), ForeignKey('users.userID', ondelete='CASCADE'), nullable=False)
    name = Column(String(255), nullable=False)
    email = Column(String(255), nullable=True)
    company = Column(String(255))
    address = Column(Text)
    phone = Column(String(50))
    total_earnings = Column(DECIMAL(12, 2), default=0.00)
    project_count = Column(Integer, default=0)
    last_invoice_date = Column(DateTime, nullable=True)
    created_at = Column(DateTime, default=func.now())
    updated_at = Column(DateTime, default=func.now(), onupdate=func.now())
    invoices = relationship('Invoice', back_populates='customer', cascade='all, delete')
    invoice_templates = relationship('InvoiceTemplate', back_populates='customer', cascade='all, delete')
    linked_emails = relationship('CustomerEmail', back_populates='customer', cascade='all, delete-orphan')

class InvoiceTemplate(Base):
    __tablename__ = 'invoice_templates'
    id = Column(CHAR(36), primary_key=True, default=generate_uuid)
    user_id = Column(CHAR(36), ForeignKey('users.userID', ondelete='CASCADE'), nullable=False)
    customer_id = Column(CHAR(36), ForeignKey('customers.id', ondelete='CASCADE'), nullable=True)
    name = Column(String(255), nullable=False)
    template_data = Column(JSON, nullable=False)
    is_customer_default = Column(Boolean, default=False)
    created_at = Column(DateTime, default=func.now())
    updated_at = Column(DateTime, default=func.now(), onupdate=func.now())
    customer = relationship('Customer', back_populates='invoice_templates')
    
    def make_default_for_customer(self, session):
        """
        Make this template the default for its customer.
        Updates naming convention and removes default status from other templates.
        """
        if not self.customer_id:
            return
            
        # Remove default status and "(default)" suffix from other templates for this customer
        existing_defaults = session.query(InvoiceTemplate).filter(
            InvoiceTemplate.customer_id == self.customer_id,
            InvoiceTemplate.is_customer_default == True,
            InvoiceTemplate.id != self.id
        ).all()
        
        for template in existing_defaults:
            template.is_customer_default = False
            if template.name.endswith(" (default)"):
                template.name = template.name[:-10]  # Remove " (default)"
        
        # Set this template as default and update name if needed
        self.is_customer_default = True
        if not self.name.endswith(" (default)"):
            self.name = f"{self.name} (default)"
    
    def remove_default_status(self):
        """
        Remove default status from this template and update the name.
        """
        self.is_customer_default = False
        if self.name.endswith(" (default)"):
            self.name = self.name[:-10]  # Remove " (default)"
    
    @staticmethod
    def set_customer_default(session, template_id, customer_id):
        """
        Static method to set a template as default for a customer.
        Handles all the logic for updating names and default statuses.
        """
        template = session.query(InvoiceTemplate).filter(
            InvoiceTemplate.id == template_id,
            InvoiceTemplate.customer_id == customer_id
        ).first()
        
        if template:
            template.make_default_for_customer(session)

class Invoice(Base):
    __tablename__ = 'invoices'
    id = Column(CHAR(36), primary_key=True, default=generate_uuid)
    user_id = Column(CHAR(36), ForeignKey('users.userID', ondelete='CASCADE'), nullable=False)
    customer_id = Column(CHAR(36), ForeignKey('customers.id', ondelete='SET NULL'), nullable=True)
    invoice_number = Column(String(50), nullable=False)
    project_name = Column(String(255), nullable=False)
    issue_date = Column(Date, nullable=False)
    due_date = Column(Date, nullable=False)
    from_name = Column(String(255), nullable=False)
    from_email = Column(String(255), nullable=False)
    from_address = Column(Text, nullable=False)
    from_phone = Column(String(50))
    to_name = Column(String(255), nullable=False)
    to_email = Column(String(255), nullable=False)
    to_address = Column(Text, nullable=False)
    to_company = Column(String(255))
    currency = Column(Enum(CurrencyEnum), default=CurrencyEnum.INR, nullable=False)
    subtotal = Column(DECIMAL(12, 2), default=0.00, nullable=False)
    tax_rate = Column(DECIMAL(5, 2), default=0.00)
    tax_amount = Column(DECIMAL(12, 2), default=0.00)
    total = Column(DECIMAL(12, 2), default=0.00, nullable=False)
    notes = Column(Text)
    terms = Column(Text)
    status = Column(Enum(InvoiceStatusEnum), default=InvoiceStatusEnum.draft, nullable=False)
    is_signed = Column(Boolean, default=False)
    signed_at = Column(DateTime, nullable=True)
    created_at = Column(DateTime, default=func.now())
    updated_at = Column(DateTime, default=func.now(), onupdate=func.now())
    customer = relationship('Customer', back_populates='invoices')
    items = relationship('InvoiceItem', back_populates='invoice', cascade='all, delete')
    invoice_signatures = relationship('InvoiceSignature', back_populates='invoice', cascade='all, delete')
    payments = relationship('InvoicePayment', back_populates='invoice', cascade='all, delete')
    custom_fields = relationship('InvoiceCustomField', back_populates='invoice', cascade='all, delete')


class InvoiceCustomField(Base):
    __tablename__ = 'invoice_custom_fields'
    id = Column(CHAR(36), primary_key=True, default=generate_uuid)
    invoice_id = Column(CHAR(36), ForeignKey('invoices.id', ondelete='CASCADE'), nullable=False)
    field_key = Column(String(50), nullable=False)
    field_value = Column(String(200), nullable=False)
    is_hidden = Column(Boolean, default=False)
    sort_order = Column(Integer, default=0)
    created_at = Column(DateTime, default=func.now())
    invoice = relationship('Invoice', back_populates='custom_fields')

class InvoicePayment(Base):
    __tablename__ = 'invoice_payments'
    id = Column(CHAR(36), primary_key=True, default=generate_uuid)
    invoice_id = Column(CHAR(36), ForeignKey('invoices.id', ondelete='CASCADE'), nullable=False)
    payment_method = Column(String(100))
    # LEGACY (ak-lvu retained for backward compat): historically stored
    # the INR-converted value. New writes populate this + inr_amount
    # identically so old readers keep working during the deploy window.
    # After migration lands + old readers switched over, this column is
    # a candidate for drop in a follow-up cleanup bead (NOT here).
    amount_received = Column(DECIMAL(12, 2), nullable=False)
    payment_date = Column(Date, nullable=True)
    breakdown = Column(JSON, nullable=True)
    notes = Column(Text)
    # ak-lvu A.1 FX idempotency — five new columns pin the currency
    # semantics so round-tripping an invoice never re-converts an
    # already-INR value as invoice-currency again (the AC-1 CRITICAL).
    # All nullable so `db.create_all()` picks up the model diff without
    # requiring a data backfill for pre-migration rows; service reads
    # tolerate NULL via money_utils / migration-gap helper.
    #
    # Deploy prereq (documented in commit body):
    #   ALTER TABLE invoice_payments
    #     ADD COLUMN original_amount    DECIMAL(12,2) NULL,
    #     ADD COLUMN original_currency  VARCHAR(3)    NULL,
    #     ADD COLUMN inr_amount         DECIMAL(12,2) NULL,
    #     ADD COLUMN fx_rate            DECIMAL(12,4) NULL,
    #     ADD COLUMN fx_rate_source     VARCHAR(100)  NULL,
    #     ADD COLUMN converted_at       DATETIME      NULL;
    original_amount = Column(DECIMAL(12, 2), nullable=True)
    original_currency = Column(String(3), nullable=True)
    inr_amount = Column(DECIMAL(12, 2), nullable=True)
    fx_rate = Column(DECIMAL(12, 4), nullable=True)
    fx_rate_source = Column(String(100), nullable=True)
    converted_at = Column(DateTime, nullable=True)
    created_at = Column(DateTime, default=func.now())
    updated_at = Column(DateTime, default=func.now(), onupdate=func.now())
    invoice = relationship('Invoice', back_populates='payments')


class PendingPaymentClaim(Base):
    """ak-lvu A.8 — claim-not-fact record for inbound mail "paid" signals.

    Before ak-lvu, `mark_invoice_paid` flipped invoice.status='paid' on
    email say-so alone — no reconciliation, no confirmation, no audit
    trail. That's the MP-3 CRITICAL from the super-review.

    New semantics: an inbound mail signal creates a PendingPaymentClaim
    row here and does NOT touch invoice.status. The claim is confirmed
    (and the real payment written + status flipped) via either
      (a) bank-credit auto-match — a future bank-transaction ingest hook
          will match Σ credited within an N-day window against
          unresolved claims (follow-up BE bead, out of scope here),
      (b) manual confirm — Overseer clicks a per-claim confirm button
          in a claims-review UI (follow-up FE bead, out of scope here).

    Only after confirmation does invoiceService write the actual
    InvoicePayment row + trigger the status recompute helper.

    Migration prereq (auto-picked-up by db.create_all() since this is a
    brand-new table, not a column add):
      No manual step needed — table appears on next boot.
    """
    __tablename__ = 'pending_payment_claims'
    id = Column(CHAR(36), primary_key=True, default=generate_uuid)
    user_id = Column(CHAR(36), ForeignKey('users.userID', ondelete='CASCADE'), nullable=False)
    invoice_id = Column(CHAR(36), ForeignKey('invoices.id', ondelete='CASCADE'), nullable=False)
    # source_email_id: the email that surfaced the claim. Nullable so a
    # manual "I got paid" flow (no email source) could also file a claim
    # in future. Text (not FK) so email-thread schema churn doesn't cascade.
    source_email_id = Column(String(255), nullable=True)
    # Claimed amount + currency as the mail said. The LLM extracts these
    # from the mail body; server-side reconciliation compares against
    # bank credits or Overseer confirmation. Stored as Decimal to match
    # the money-safety invariant even before confirmation.
    claimed_amount = Column(DECIMAL(12, 2), nullable=True)
    claimed_currency = Column(String(3), nullable=True)
    # Free-form context from the mail LLM: sender, subject snippet,
    # payment_method / payment_date if extracted. Not authoritative
    # until confirmation.
    claim_metadata = Column(JSON, nullable=True)
    # Lifecycle: created_at set on insert; resolved_at + resolution set
    # when the claim is either confirmed (→ real payment row written) or
    # rejected (Overseer decides it was a mis-classified mail). NULL
    # resolved_at means "still pending review".
    created_at = Column(DateTime, nullable=False, server_default=func.now())
    resolved_at = Column(DateTime, nullable=True)
    # 'pending' | 'confirmed' | 'rejected' — string not enum so future
    # values (e.g. 'auto_matched') don't require another ALTER.
    resolution = Column(String(30), nullable=False, default='pending')
    # If confirmed, the InvoicePayment row that was written. NULL for
    # pending / rejected. Nullable FK with SET NULL on delete so removing
    # the payment doesn't cascade-delete the historical claim record.
    resolved_payment_id = Column(
        CHAR(36),
        ForeignKey('invoice_payments.id', ondelete='SET NULL'),
        nullable=True,
    )
    resolution_note = Column(Text, nullable=True)

class InvoiceItem(Base):
    __tablename__ = 'invoice_items'
    id = Column(CHAR(36), primary_key=True, default=generate_uuid)
    invoice_id = Column(CHAR(36), ForeignKey('invoices.id', ondelete='CASCADE'), nullable=False)
    description = Column(Text, nullable=False)
    quantity = Column(DECIMAL(10, 2), default=1.00, nullable=False)
    rate = Column(DECIMAL(10, 2), default=0.00, nullable=False)
    amount = Column(DECIMAL(12, 2), default=0.00, nullable=False)
    item_order = Column(Integer, default=1, nullable=False)
    created_at = Column(DateTime, default=func.now())
    invoice = relationship('Invoice', back_populates='items')

class Signature(Base):
    __tablename__ = 'signatures'
    id = Column(CHAR(36), primary_key=True, default=generate_uuid)
    user_id = Column(CHAR(36), ForeignKey('users.userID', ondelete='CASCADE'), nullable=False)
    name = Column(String(255), nullable=False)
    signature_data = Column(MEDIUMTEXT, nullable=False)
    signature_type = Column(String(20), default='image')
    is_default = Column(Boolean, default=False)
    created_at = Column(DateTime, default=func.now())
    updated_at = Column(DateTime, default=func.now(), onupdate=func.now())
    invoice_signatures = relationship('InvoiceSignature', back_populates='signature', cascade='all, delete')

class InvoiceSignature(Base):
    __tablename__ = 'invoice_signatures'
    id = Column(CHAR(36), primary_key=True, default=generate_uuid)
    invoice_id = Column(CHAR(36), ForeignKey('invoices.id', ondelete='CASCADE'), nullable=False)
    signature_id = Column(CHAR(36), ForeignKey('signatures.id', ondelete='RESTRICT'), nullable=False)
    signature_position_x = Column(Integer, nullable=False)
    signature_position_y = Column(Integer, nullable=False)
    signature_width = Column(Integer, nullable=False)
    signature_height = Column(Integer, nullable=False)
    signed_at = Column(DateTime, default=func.now())
    signed_pdf_path = Column(Text)
    invoice = relationship('Invoice', back_populates='invoice_signatures')
    signature = relationship('Signature', back_populates='invoice_signatures')