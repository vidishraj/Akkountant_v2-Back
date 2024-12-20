from sqlalchemy import Column, String, ForeignKey, Integer, Date, and_

from sqlalchemy.orm import relationship
from models.Base import Base


class DepositSecurities(Base):
    __tablename__ = 'depositSecurities'

    buyID = Column(String(30), primary_key=True)
    date = Column(Date, nullable=False)
    depositDescription = Column(String(250), nullable=False)
    depositAmount = Column(Integer, nullable=False)
    userID = Column(String(100), ForeignKey('users.userID', ondelete='CASCADE'), nullable=False)
    securityType = Column(String(10), nullable=False)

    user_relationship = relationship('User', back_populates='deposit_relationship')

    # Use string references to avoid circular import
    sold_securities = relationship(
        "SoldSecurities",
        primaryjoin="and_(DepositSecurities.buyID == foreign(SoldSecurities.buyID), SoldSecurities.source_type == "
                    "'deposit')",
        back_populates="deposit_relationship",
        overlaps="purchase_relationship,sold_securities",
        cascade="all, delete-orphan")
    goldDetails = relationship('GoldDetails', back_populates='buyIdRel')
