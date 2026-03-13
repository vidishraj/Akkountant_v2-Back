from sqlalchemy import Column, String, Date, ForeignKey, Enum, UniqueConstraint, Index
from models.Base import Base
from sqlalchemy.orm import relationship
from sqlalchemy.types import DECIMAL as Decimal
import enum


class ProcessingMethod(enum.Enum):
    PATTERN_MATCH = "PATTERN_MATCH"
    CLAUDE_CODE = "CLAUDE_CODE"


class Transactions(Base):
    __tablename__ = 'transactions'

    referenceID = Column(String(64), primary_key=True, nullable=False)
    date = Column(Date, nullable=False)
    details = Column(String(500), nullable=False)
    amount = Column(Decimal(10, 2), nullable=False)
    tag = Column(String(100))
    fileID = Column(String(100), ForeignKey('fileDetails.fileID'), nullable=True)
    source = Column(String(10), nullable=False)
    bank = Column(String(25), nullable=False)
    user = Column(String(100), ForeignKey('users.userID', ondelete='CASCADE'), nullable=False)
    processed_via = Column(Enum(ProcessingMethod), default=ProcessingMethod.PATTERN_MATCH)
    gmail_message_id = Column(String(500), nullable=True, unique=True)  # Email Message-Id header for unique identification
    transfer_group_id = Column(String(64), nullable=True, index=True)

    file_details = relationship('FileDetails', back_populates='transactions')
    user_relationship = relationship('User', back_populates='transactions')
