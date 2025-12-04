from sqlalchemy import Column, String, DateTime, Index
from models.Base import Base
from datetime import datetime


class ProcessedEmail(Base):
    __tablename__ = 'processed_emails'
    
    gmail_id = Column(String(100), primary_key=True)
    user_id = Column(String(100), nullable=False)
    processed_at = Column(DateTime, default=datetime.utcnow, nullable=False)
    
    # Add index for efficient lookups by user
    __table_args__ = (
        Index('idx_user_processed', 'user_id', 'gmail_id'),
    )