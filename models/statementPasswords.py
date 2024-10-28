from sqlalchemy import Column, String, ForeignKey, Integer
from Base import Base
from sqlalchemy.orm import relationship

from sqlalchemy import Column, String, ForeignKey, PrimaryKeyConstraint
from sqlalchemy.orm import relationship


class StatementPasswords(Base):
    __tablename__ = 'statementPasswords'

    bank = Column(String(100), nullable=False)
    password_hash = Column(String(256), nullable=False)  # Hashing password
    user = Column(String(100), ForeignKey('users.userID'), nullable=False)

    # Define composite primary key
    __table_args__ = (
        PrimaryKeyConstraint('bank', 'user'),
    )

    # Relationship with User model
    user_relationship = relationship('User', back_populates='statement_passwords')
