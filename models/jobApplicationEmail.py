from sqlalchemy import Column, Integer, String, DateTime, Text
from models.Base import Base
from datetime import datetime

class JobApplicationEmail(Base):
    __tablename__ = 'job_application_emails'
    id = Column(Integer, primary_key=True)
    email_date = Column(DateTime, nullable=False)
    employer = Column(String(255), nullable=True)
    role = Column(String(255), nullable=True)
    email_subject = Column(String(255), nullable=True)
    email_body = Column(Text, nullable=False)
    created_at = Column(DateTime, default=datetime.utcnow)