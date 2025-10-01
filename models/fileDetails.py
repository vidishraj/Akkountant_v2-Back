from sqlalchemy import Column, String, Date, ForeignKey
from models.Base import Base
from sqlalchemy.orm import relationship
from sqlalchemy import Integer


class FileDetails(Base):
    __tablename__ = 'fileDetails'

    fileID = Column(String(100), primary_key=True)
    uploadDate = Column(Date, nullable=False)
    fileName = Column(String(100), nullable=False)
    fileSize = Column(String(64), nullable=False)
    statementCount = Column(Integer, nullable=False)
    bank = Column(String(100), nullable=False)
    user = Column(String(100), ForeignKey('users.userID', ondelete='CASCADE'), nullable=False)
    gmail_message_id = Column(String(500), nullable=True, unique=True)  # Email Message-Id header for statement emails

    transactions = relationship('Transactions', back_populates='file_details')
    user_relationship = relationship('User', back_populates='file_details')
