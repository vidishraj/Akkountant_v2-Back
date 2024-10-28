from sqlalchemy import Column, String, Date, ForeignKey, Integer
from sqlalchemy.orm import relationship
from Base import Base


class SavedTags(Base):
    __tablename__ = 'savedTags'

    id = Column(Integer, primary_key=True, autoincrement=True)
    details = Column(String(100), nullable=False)
    tag = Column(Date, nullable=False)
    user = Column(String(100), ForeignKey('users.userID'), nullable=False)  # Assuming there’s a Users table

    user_relationship = relationship('User', back_populates='saved_tags')
