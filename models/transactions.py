from sqlalchemy import Column, String, DateTime, ForeignKey, Enum, UniqueConstraint, Index
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
    date = Column(DateTime, nullable=False)
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
    # ak-8l5: reference-aware dedup. When the LLM extracts a per-tx
    # bank-native identifier (UPI ref / IMPS ref / NEFT UTR / MBSF
    # number / cheque number / etc.), it lands here. When present,
    # the referenceID PK is derived from (bank, bank_reference_id) —
    # so chunk re-reads collapse but legitimate same-tuple tx with
    # different refs stay distinct. NULL is legal and expected for
    # ref-less rows (cash deposits, interest posts, etc.); those
    # fall through to a positional-fallback hash — see
    # utils/reference_id.py:generate_reference_v2*.
    bank_reference_id = Column(String(128), nullable=True)

    file_details = relationship('FileDetails', back_populates='transactions')
    user_relationship = relationship('User', back_populates='transactions')

    __table_args__ = (
        Index('idx_txn_user_date', 'user', 'date'),
        # ak-8l5: (user, bank, bank_reference_id) — supports the
        # "look up whether a tx with this ref already exists for the
        # user on this bank" query the executor issues before insert
        # to short-circuit a duplicate content re-read. Non-unique
        # (NULL bank_reference_id is legal per PK-fallback path).
        Index('idx_txn_user_bank_ref', 'user', 'bank', 'bank_reference_id'),
    )
