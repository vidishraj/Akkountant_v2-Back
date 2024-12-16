from models.Base import Base
from sqlalchemy import Column, String, ForeignKey, PrimaryKeyConstraint
from sqlalchemy.orm import relationship


class StatementPasswords(Base):
    __tablename__ = 'statementPasswords'

    bank = Column(String(100), nullable=False)
    password_hash = Column(String(256), nullable=False)
    user = Column(String(100), ForeignKey('users.userID', ondelete='CASCADE'), nullable=False)

    __table_args__ = (
        PrimaryKeyConstraint('bank', 'user'),
    )

    user_relationship = relationship('User', back_populates='statement_passwords')
