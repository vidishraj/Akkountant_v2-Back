from sqlalchemy import Column, String, Date, ForeignKey, Boolean, Index, Integer
from models.Base import Base
from sqlalchemy.orm import relationship


class FileDetails(Base):
    __tablename__ = 'fileDetails'

    fileID = Column(String(100), primary_key=True)
    uploadDate = Column(Date, nullable=False)
    fileName = Column(String(100), nullable=False)
    fileSize = Column(Integer, nullable=False)
    statementCount = Column(Integer, nullable=False)
    bank = Column(String(100), nullable=False)
    user = Column(String(100), ForeignKey('users.userID', ondelete='CASCADE'), nullable=False)
    gmail_message_id = Column(String(500), nullable=True, unique=True)  # Email Message-Id header for statement emails
    deleted = Column(Boolean, nullable=False, default=False, server_default='0')  # Soft delete flag
    # ak-ifc v3 MINOR 1: when the file-level savings-summary
    # reconciliation backstop detects divergence between extracted
    # savings totals and the PDF's own summary, it falls back to
    # unfiltered extraction (over-parse trade for zero-loss). This
    # flag lets a follow-up sweep find these files for manual review
    # of the acknowledged non-savings contamination.
    #
    # MIGRATION: infra needs an ALTER TABLE fileDetails ADD COLUMN
    # reconciliation_fallback BOOLEAN NOT NULL DEFAULT 0; before this
    # deploys. Until then, TransactionService.mark_reconciliation_fallback
    # swallows the ORM error so the fallback re-run still lands the
    # data.
    reconciliation_fallback = Column(
        Boolean, nullable=False, default=False, server_default='0',
    )

    transactions = relationship('Transactions', back_populates='file_details')
    user_relationship = relationship('User', back_populates='file_details')

    __table_args__ = (
        Index('idx_fd_user_bank', 'user', 'bank'),
    )
