from sqlalchemy import Column, String, ForeignKey, Integer
from sqlalchemy.types import DECIMAL as Decimal
from models.Base import Base
from sqlalchemy.orm import relationship


class SoldSecurities(Base):
    __tablename__ = 'soldSecurities'

    sellID = Column(Integer, primary_key=True, autoincrement=True)  # Auto-incrementing primary key
    buyID = Column(Integer, ForeignKey('purchasedSecurities.buyID'), nullable=False)
    sellQuant = Column(Integer, nullable=False)
    sellPrice = Column(Decimal(10, 2), nullable=False)
    profit = Column(Decimal(10, 2), nullable=True)

    purchase_relationship = relationship('PurchasedSecurities', back_populates='sold_securities')
