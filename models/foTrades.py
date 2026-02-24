import enum

from sqlalchemy import Column, String, Date, Integer, Enum, DateTime, ForeignKey, DECIMAL
from sqlalchemy.orm import relationship
from models.Base import Base


class OptionType(enum.Enum):
    CE = "CE"
    PE = "PE"


class TradeType(enum.Enum):
    buy = "buy"
    sell = "sell"


class FOTrade(Base):
    __tablename__ = 'fo_trades'

    trade_id = Column(String(30), primary_key=True)
    user_id = Column(String(100), ForeignKey('users.userID', ondelete='CASCADE'), nullable=False)
    symbol = Column(String(100), nullable=False)
    underlying = Column(String(50), nullable=False)
    expiry_date = Column(Date, nullable=False)
    strike_price = Column(DECIMAL(12, 2), nullable=False)
    option_type = Column(Enum(OptionType), nullable=False)
    trade_date = Column(Date, nullable=False)
    trade_type = Column(Enum(TradeType), nullable=False)
    quantity = Column(Integer, nullable=False)
    price = Column(DECIMAL(12, 2), nullable=False)
    order_id = Column(String(30), nullable=True)
    order_execution_time = Column(DateTime, nullable=True)
    exchange = Column(String(10), default='NSE')

    user_relationship = relationship('User', back_populates='fo_trades')
