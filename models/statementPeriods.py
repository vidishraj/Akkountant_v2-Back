from sqlalchemy import Column, String, Date, Integer, ForeignKey, DateTime, UniqueConstraint, Index
from sqlalchemy.orm import relationship
from models.Base import Base
from datetime import datetime


class StatementPeriod(Base):
    __tablename__ = 'statementPeriods'

    id = Column(Integer, primary_key=True, autoincrement=True)
    bank = Column(String(25), nullable=False)
    period_start = Column(Date, nullable=False)
    period_end = Column(Date, nullable=False)
    file_id = Column(String(100), ForeignKey('fileDetails.fileID'), nullable=True)
    user = Column(String(100), ForeignKey('users.userID', ondelete='CASCADE'), nullable=False)
    gmail_message_id = Column(String(500), nullable=True)
    transaction_count = Column(Integer, default=0)
    email_txns_replaced = Column(Integer, default=0)
    created_at = Column(DateTime, default=datetime.utcnow)

    __table_args__ = (
        UniqueConstraint('bank', 'period_start', 'period_end', 'user', name='uq_statement_period'),
        Index('ix_statement_periods_user_bank', 'user', 'bank'),
    )

    user_relationship = relationship('User', back_populates='statement_periods')
    file_details = relationship('FileDetails', backref='statement_periods')
