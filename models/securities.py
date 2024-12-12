from sqlalchemy import Column, Integer, Date, Enum, and_, String
from sqlalchemy.types import DECIMAL as Decimal
from sqlalchemy.orm import relationship, foreign
from models.Base import Base


class SoldSecurities(Base):
    __tablename__ = 'soldSecurities'

    sellID = Column(Integer, primary_key=True, autoincrement=True)
    buyID = Column(String(30), nullable=False)  # References either PurchasedSecurities or DepositSecurities
    source_type = Column(Enum('purchased', 'deposit', name='source_type_enum'), nullable=False)

    date = Column(Date, nullable=False)
    sellQuant = Column(Integer, nullable=False)
    sellPrice = Column(Decimal(10, 2), nullable=False)
    profit = Column(Decimal(10, 2), nullable=True)

    purchase_relationship = relationship(
        "PurchasedSecurities",
        primaryjoin="and_(foreign(SoldSecurities.buyID) == PurchasedSecurities.buyID, SoldSecurities.source_type == "
                    "'purchased')",
        back_populates="sold_securities",
        overlaps="deposit_relationship,sold_securities"
    )

    deposit_relationship = relationship(
        "DepositSecurities",
        primaryjoin="and_(foreign(SoldSecurities.buyID) == DepositSecurities.buyID, SoldSecurities.source_type == "
                    "'deposit')",
        back_populates="sold_securities",
        overlaps="purchase_relationship,sold_securities"
    )