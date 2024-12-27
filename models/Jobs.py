from sqlalchemy import Column, String, ForeignKey, Integer, DateTime
from sqlalchemy.orm import relationship
from models.Base import Base


class Job(Base):
    __tablename__ = 'jobs'
    id = Column(Integer, primary_key=True)
    title = Column(String(100), nullable=False)
    result = Column(String(900), nullable=True)
    priority = Column(String(10))  # e.g., Low, Medium, High
    status = Column(String(20), default="Pending")  # Pending, Completed, Overdue
    due_date = Column(DateTime, nullable=False)
    # Can be null for global jobs
    user_id = Column(String(100), nullable=True)
