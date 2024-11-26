from sqlalchemy import Column, String, ForeignKey, Integer, Date
from sqlalchemy.types import DECIMAL as Decimal
from models.Base import Base
from sqlalchemy.orm import relationship


class PurchasedSecurities(Base):
    __tablename__ = 'purchasedSecurities'

    buyID = Column(Integer, primary_key=True, autoincrement=True)  # Auto-incrementing primary key
    date = Column(Date, nullable=False)
    securityCode = Column(String(250), nullable=False)
    buyQuant = Column(Integer, nullable=False)
    buyPrice = Column(Decimal(10, 2), nullable=False)
    userID = Column(String(100), ForeignKey('users.userID'), nullable=False)
    securityType = Column(String(10), nullable=False)

    user_relationship = relationship('User', back_populates='purchased_securities')
    sold_securities = relationship('SoldSecurities', back_populates='purchase_relationship')
