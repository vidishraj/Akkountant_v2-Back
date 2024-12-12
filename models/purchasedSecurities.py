from sqlalchemy import Column, String, ForeignKey, Integer, Date
from sqlalchemy.types import DECIMAL as Decimal
from sqlalchemy.orm import relationship
from models.Base import Base


class PurchasedSecurities(Base):
    __tablename__ = 'purchasedSecurities'

    buyID = Column(String(30), primary_key=True)
    date = Column(Date, nullable=False)
    securityCode = Column(String(250), nullable=False)
    buyQuant = Column(Integer, nullable=False)
    buyPrice = Column(Decimal(10, 2), nullable=False)
    userID = Column(String(100), ForeignKey('users.userID'), ondelete='CASCADE', nullable=False)
    securityType = Column(String(10), nullable=False)

    user_relationship = relationship('User', back_populates='purchased_securities')

    # Use string references to avoid circular import
    sold_securities = relationship(
        "SoldSecurities",
        primaryjoin="and_(PurchasedSecurities.buyID == foreign(SoldSecurities.buyID),SoldSecurities.source_type == "
                    "'purchased')",
        back_populates="purchase_relationship",
        overlaps="deposit_relationship,sold_securities"
    )
