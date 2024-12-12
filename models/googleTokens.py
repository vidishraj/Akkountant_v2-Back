from models.Base import Base
from sqlalchemy import Column, String, ForeignKey, DateTime, PrimaryKeyConstraint, Integer
from sqlalchemy.orm import relationship
from sqlalchemy.ext.declarative import declared_attr


class UserToken(Base):
    __tablename__ = 'user_tokens'

    user_id = Column(String(100), ForeignKey('users.userID'), ondelete='CASCADE', nullable=False)  # Foreign key to users table
    access_token = Column(String(256), nullable=False)
    refresh_token = Column(String(256), nullable=False)
    client_id = Column(String(256), nullable=False)
    client_secret = Column(String(256), nullable=False)
    expiry = Column(Integer, nullable=False)
    service_type = Column(String(20), nullable=False)  # To specify 'gmail' or 'drive'

    # Define primary key constraint
    __table_args__ = (
        PrimaryKeyConstraint('user_id', 'service_type'),  # Ensure unique user-token pairs
    )

    user_relationship = relationship('User', back_populates='user_tokens')  # Assuming User model has a back
    # reference

    def __repr__(self):
        return f"<UserToken(user_id={self.user_id}, service_type={self.service_type}, expiry={self.expiry})>"
