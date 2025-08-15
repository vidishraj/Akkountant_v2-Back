from sqlalchemy import Column, String, Text, DECIMAL, Integer, DateTime, Boolean, Enum, ForeignKey, JSON, Date, func
from sqlalchemy.dialects.mysql import CHAR
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

class CurrencyEnum(enum.Enum):
    USD = 'USD'
    EUR = 'EUR'
    GBP = 'GBP'
    INR = 'INR'
    CAD = 'CAD'
    AUD = 'AUD'
    JPY = 'JPY'

class PaymentStatusEnum(enum.Enum):
    pending = 'pending'
    completed = 'completed'
    failed = 'failed'
    cancelled = 'cancelled'

class Customer(Base):
    __tablename__ = 'customers'
    id = Column(CHAR(36), primary_key=True, default=generate_uuid)
    user_id = Column(CHAR(36), nullable=False)
    name = Column(String(255), nullable=False)
    email = Column(String(255), nullable=False)
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

class InvoiceTemplate(Base):
    __tablename__ = 'invoice_templates'
    id = Column(CHAR(36), primary_key=True, default=generate_uuid)
    user_id = Column(CHAR(36), nullable=False)
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
    user_id = Column(CHAR(36), nullable=False)
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
    amount_received = Column(DECIMAL(12, 2), nullable=False)
    payment_date = Column(Date, nullable=True)
    breakdown = Column(JSON, nullable=True)
    notes = Column(Text)
    created_at = Column(DateTime, default=func.now())
    updated_at = Column(DateTime, default=func.now(), onupdate=func.now())
    invoice = relationship('Invoice', back_populates='payments')

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
    user_id = Column(CHAR(36), nullable=False)
    name = Column(String(255), nullable=False)
    signature_data = Column(Text, nullable=False)
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