from sqlalchemy import Column, DECIMAL, String, ForeignKey, Enum
from sqlalchemy.orm import relationship

from models.Base import Base


class GoldDetails(Base):
    __tablename__ = 'goldDetails'

    buyID = Column(String(30), ForeignKey('depositSecurities.buyID', ondelete="CASCADE"), nullable=False, primary_key=True)
    quantity = Column(DECIMAL(10, 2))
    goldType = Column(Enum('22', '24', '18', name='gold_type_enum'), nullable=False)

    buyIdRel = relationship('DepositSecurities', back_populates='goldDetails')