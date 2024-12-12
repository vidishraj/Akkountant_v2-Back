from sqlalchemy import Column, String, ForeignKey, UniqueConstraint
from sqlalchemy.orm import relationship

from models.Base import Base


class TradeAssociation(Base):
    __tablename__ = 'trade_association'

    tradeID = Column(String(30), primary_key=True)  # tradeID as primary key
    buyID = Column(String(30), ForeignKey('purchasedSecurities.buyID'), ondelete="CASCADE", unique=False, nullable=False)

    # Constraints to enforce one-to-one relationship explicitly
    UniqueConstraint('tradeID', 'buyID', name='uq_trade_buy')

