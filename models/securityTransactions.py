from sqlalchemy import Column, Integer, Date, Enum, and_, String, ForeignKey
from sqlalchemy.types import DECIMAL as Decimal
from sqlalchemy.orm import relationship, foreign
from models.Base import Base


class SecurityTransactions(Base):
    __tablename__ = 'securityTransactions'

    transactionId = Column(Integer, primary_key=True, autoincrement=True)
    date = Column(Date, nullable=False)
    quant = Column(Decimal(15, 5), nullable=False)
    price = Column(Decimal(10, 2), nullable=False)
    transactionType = Column(Enum('buy', 'sell', name='transaction_type_enum'), nullable=False)
    userID = Column(String(100), ForeignKey('users.userID', ondelete='CASCADE'), nullable=False)
    securityType = Column(String(20), nullable=False)
    buyId = Column(String(30), ForeignKey('purchasedSecurities.buyID', ondelete='CASCADE'), nullable=False)

    user_relationship = relationship('User', back_populates='transaction_relationship')
