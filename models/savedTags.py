from sqlalchemy import Column, String, Date, ForeignKey, Integer
from sqlalchemy.orm import relationship
from models.Base import Base


class SavedTags(Base):
    __tablename__ = 'savedTags'

    id = Column(Integer, primary_key=True, autoincrement=True)
    details = Column(String(100), nullable=False)
    tag = Column(Date, nullable=False)
    user = Column(String(100), ForeignKey('users.userID'), ondelete='CASCADE', nullable=False)
    user_relationship = relationship('User', back_populates='saved_tags')
