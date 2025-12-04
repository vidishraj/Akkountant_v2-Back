from sqlalchemy import Column, String, Integer, DateTime, Text, Boolean, JSON
from models.Base import Base
from datetime import datetime


class JobEmail(Base):
    __tablename__ = 'job_emails'
    
    id = Column(Integer, primary_key=True)
    gmail_id = Column(String(100), unique=True, nullable=False)
    sender = Column(String(200), nullable=False)
    subject = Column(String(500), nullable=True)
    email_body = Column(Text, nullable=False)
    company_name = Column(String(200), nullable=True)
    job_title = Column(String(200), nullable=True)
    application_status = Column(String(100), nullable=True, default='unknown')
    application_type = Column(String(50), nullable=True, default='status_update')  # new_application, status_update
    date_received = Column(DateTime, nullable=False)
    created_at = Column(DateTime, default=datetime.utcnow, nullable=False)
    user_id = Column(String(100), nullable=False)
    is_read = Column(Boolean, default=False, nullable=False)
    gmail_link = Column(String(500), nullable=True)
    extracted_metadata = Column(JSON, nullable=True)