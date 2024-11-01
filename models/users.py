from sqlalchemy import Column, String
from models.Base import Base
from sqlalchemy.orm import relationship


# Not sure about this at the moment.

class User(Base):
    __tablename__ = 'users'

    userID = Column(String(100), primary_key=True)
    email = Column(String(100), nullable=True)
    optedBanks = Column(String(500), nullable=True)

    saved_tags = relationship('SavedTags', back_populates='user_relationship')
    statement_passwords = relationship('StatementPasswords', back_populates='user_relationship')
    transactions = relationship('Transactions', back_populates='user_relationship')
    file_details = relationship('FileDetails', back_populates='user_relationship')
    transaction_reviews = relationship('TransactionForReview', back_populates='user_relationship')
    user_tokens = relationship('UserToken', back_populates='user_relationship')
