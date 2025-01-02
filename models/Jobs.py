from sqlalchemy import Column, String, ForeignKey, Integer, DateTime, CheckConstraint
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
    # Updated failures column
    failures = Column(Integer, default=0, nullable=False)
    user_id = Column(String(100), nullable=True)  # Can be null for global jobs

    # Adding a check constraint to enforce max value for failures
    __table_args__ = (
        CheckConstraint('failures >= 0 AND failures <= 10', name='check_failures_range'),
    )
