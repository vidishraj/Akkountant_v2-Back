from sqlalchemy import Column, String, ForeignKey, Integer, PrimaryKeyConstraint
from sqlalchemy.orm import relationship
from models.Base import Base


class TransactionForReview(Base):
    __tablename__ = 'transactionForReview'

    user = Column(String(100), ForeignKey('users.userID'), ondelete='CASCADE', nullable=False)
    conflict = Column(String(500), nullable=False)

    # Define a composite primary key using both user and conflict
    __table_args__ = (
        PrimaryKeyConstraint('user', 'conflict'),
    )

    user_relationship = relationship('User', back_populates='transaction_reviews')
