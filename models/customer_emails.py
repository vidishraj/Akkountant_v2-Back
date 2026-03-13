from sqlalchemy import Column, String, Integer, DateTime, ForeignKey, UniqueConstraint, Index
from sqlalchemy.orm import relationship
from sqlalchemy.sql import func
from sqlalchemy.dialects.mysql import CHAR
from .Base import Base


class CustomerEmail(Base):
    __tablename__ = 'customer_emails'

    id = Column(Integer, primary_key=True, autoincrement=True)
    customer_id = Column(CHAR(36), ForeignKey('customers.id', ondelete='CASCADE'), nullable=False)
    email_id = Column(Integer, ForeignKey('processedEmails.id', ondelete='CASCADE'), nullable=False)
    linked_by = Column(String(20), default='auto')  # 'auto' | 'manual'
    created_at = Column(DateTime, default=func.now())

    __table_args__ = (
        UniqueConstraint('customer_id', 'email_id', name='uq_customer_email'),
        Index('idx_ce_customer', 'customer_id'),
        Index('idx_ce_email', 'email_id'),
    )

    customer = relationship('Customer', back_populates='linked_emails')
    email = relationship('ProcessedEmails')
