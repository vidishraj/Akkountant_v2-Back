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

    transactions = relationship('Transactions', back_populates='file_details')
    user_relationship = relationship('User', back_populates='file_details')

    __table_args__ = (
        Index('idx_fd_user_bank', 'user', 'bank'),
    )
