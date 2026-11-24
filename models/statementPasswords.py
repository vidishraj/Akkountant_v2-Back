from models.Base import Base
from sqlalchemy import Column, String, ForeignKey, PrimaryKeyConstraint
from sqlalchemy.orm import relationship


class StatementPasswords(Base):
    __tablename__ = 'statementPasswords'

    bank = Column(String(100), nullable=False)
    # ak-ouu2: despite the name this holds the PLAINTEXT statement-PDF
    # password (PDF unlock needs the raw value; hashing is impossible).
    # Read/write only via utils/statement_password_store (ak-3rwh);
    # never log, echo or return it. Encryption at rest is held for the
    # Overseer (ak-3rwh option A).
    password_hash = Column(String(256), nullable=False)
    user = Column(String(100), ForeignKey('users.userID', ondelete='CASCADE'), nullable=False)

    __table_args__ = (
        PrimaryKeyConstraint('bank', 'user'),
    )

    user_relationship = relationship('User', back_populates='statement_passwords')
