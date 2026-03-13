from sqlalchemy import Column, String, Date, Integer, Numeric, ForeignKey, UniqueConstraint, Index
from sqlalchemy.orm import relationship
from models.Base import Base


class InvestmentSnapshot(Base):
    __tablename__ = 'investmentSnapshots'

    id = Column(Integer, primary_key=True, autoincrement=True)
    date = Column(Date, nullable=False)
    user = Column(String(100), ForeignKey('users.userID', ondelete='CASCADE'), nullable=False)
    investment_type = Column(String(20), nullable=False)
    total_invested = Column(Numeric(14, 2), nullable=False, default=0)
    current_value = Column(Numeric(14, 2), nullable=False, default=0)
    profit = Column(Numeric(14, 2), nullable=False, default=0)
    profit_percent = Column(Numeric(8, 4), nullable=False, default=0)

    __table_args__ = (
        UniqueConstraint('date', 'user', 'investment_type', name='uq_snapshot_date_user_type'),
        Index('ix_snapshot_user_date', 'user', 'date'),
    )

    user_relationship = relationship('User', back_populates='investment_snapshots')
