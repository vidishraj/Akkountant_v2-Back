from sqlalchemy import Column, String, Date, ForeignKey, PrimaryKeyConstraint
from models.Base import Base
from sqlalchemy.orm import relationship
from sqlalchemy import Integer


class InvestmentHistory(Base):
    __tablename__ = 'investmentHistory'

    date = Column(Date, nullable=False)
    stocks = Column(Integer, nullable=False)
    mf = Column(Integer, nullable=False)
    nps = Column(Integer, nullable=False)
    epf = Column(Integer, nullable=False)
    ppf = Column(Integer, nullable=False)
    gold = Column(Integer, nullable=False)
    user = Column(String(100), ForeignKey('users.userID', ondelete='CASCADE'), nullable=False)
    __table_args__ = (
        PrimaryKeyConstraint('date', 'user'),
    )
    user_relationship = relationship('User', back_populates='investment_history')
