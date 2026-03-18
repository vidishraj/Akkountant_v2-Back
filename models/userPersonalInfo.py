from sqlalchemy import Column, String, ForeignKey, Date
from models.Base import Base


class UserPersonalInfo(Base):
    __tablename__ = 'userPersonalInfo'

    user_id = Column(String(100), ForeignKey('users.userID', ondelete='CASCADE'), primary_key=True)
    first_name = Column(String(100), nullable=True)
    last_name = Column(String(100), nullable=True)
    date_of_birth = Column(Date, nullable=True)
    pan_number = Column(String(10), nullable=True)       # ABCDE1234F
    phone_number = Column(String(15), nullable=True)     # 10-digit mobile
    phone_number_2 = Column(String(15), nullable=True)   # Alternate mobile
    uan_number = Column(String(12), nullable=True)       # EPF UAN
    customer_id_hdfc = Column(String(20), nullable=True) # HDFC Customer ID
