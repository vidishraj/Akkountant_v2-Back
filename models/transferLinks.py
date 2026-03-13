from sqlalchemy import Column, String, Integer, Date, DateTime, ForeignKey, Index
from sqlalchemy.orm import relationship
from sqlalchemy.types import DECIMAL as Decimal
from models.Base import Base
from datetime import datetime


class TransferLink(Base):
    __tablename__ = 'transferLinks'

    id = Column(Integer, primary_key=True, autoincrement=True)
    transfer_group_id = Column(String(64), nullable=False)
    debit_reference_id = Column(String(64), ForeignKey('transactions.referenceID'), nullable=False)
    credit_reference_id = Column(String(64), ForeignKey('transactions.referenceID'), nullable=False)
    debit_bank = Column(String(25), nullable=False)
    credit_bank = Column(String(25), nullable=False)
    amount = Column(Decimal(10, 2), nullable=False)
    match_confidence = Column(String(20), nullable=False)
    match_method = Column(String(50), nullable=True)
    user = Column(String(100), ForeignKey('users.userID', ondelete='CASCADE'), nullable=False)
    period_start = Column(Date, nullable=True)
    period_end = Column(Date, nullable=True)
    created_at = Column(DateTime, default=datetime.utcnow)

    __table_args__ = (
        Index('ix_transfer_links_group_id', 'transfer_group_id'),
        Index('ix_transfer_links_user_period', 'user', 'period_start', 'period_end'),
    )

    user_relationship = relationship('User', back_populates='transfer_links')
    debit_transaction = relationship('Transactions', foreign_keys=[debit_reference_id])
    credit_transaction = relationship('Transactions', foreign_keys=[credit_reference_id])
