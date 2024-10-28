from sqlalchemy import Column, Integer, String, ForeignKey
from Base import Base
from sqlalchemy.orm import relationship


class TransactionForReview(Base):
    __tablename__ = 'transactionForReview'

    conflictID = Column(Integer, primary_key=True, autoincrement=True)
    user = Column(String(100), ForeignKey('users.userID'), nullable=False)
    conflict = Column(String(700), nullable=False)

    user_relationship = relationship('User', back_populates='transaction_reviews')
