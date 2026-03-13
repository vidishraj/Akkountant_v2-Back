from sqlalchemy import Column, String, Integer, DateTime, Text, JSON, ForeignKey, UniqueConstraint, Index
from sqlalchemy.orm import relationship
from sqlalchemy.sql import func
from models.Base import Base


class ProcessedEmails(Base):
    __tablename__ = 'processedEmails'

    id = Column(Integer, primary_key=True, autoincrement=True)
    gmail_id = Column(String(200), nullable=False)
    user_id = Column(String(100), ForeignKey('users.userID', ondelete='CASCADE'), nullable=False)
    sender = Column(String(300), nullable=True)
    subject = Column(String(500), nullable=True)
    email_date = Column(DateTime, nullable=True)
    category = Column(String(50), nullable=True)
    processing_type = Column(String(20), nullable=True)
    status = Column(String(20), nullable=False, default='processed')
    items_extracted = Column(Integer, default=0)
    extraction_summary = Column(JSON, nullable=True)
    error_message = Column(Text, nullable=True)
    pdf_filename = Column(String(500), nullable=True)
    processed_at = Column(DateTime, default=func.now())

    __table_args__ = (
        UniqueConstraint('user_id', 'gmail_id', name='uq_user_gmail'),
        Index('idx_pe_user_gmail', 'user_id', 'gmail_id'),
        Index('idx_pe_user_date', 'user_id', 'processed_at'),
    )

    user_relationship = relationship('User', back_populates='processed_emails')
