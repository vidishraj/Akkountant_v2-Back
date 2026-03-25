from sqlalchemy import Column, String, Integer, DateTime, Text, DECIMAL, Index
from models.Base import Base


class PortfolioVisitor(Base):
    __tablename__ = 'portfolio_visitors'

    id = Column(Integer, primary_key=True, autoincrement=True)
    ip = Column(String(45), nullable=False)
    city = Column(String(100), nullable=True)
    region = Column(String(100), nullable=True)
    country = Column(String(100), nullable=True)
    country_code = Column(String(10), nullable=True)
    lat = Column(DECIMAL(10, 7), nullable=True)
    lon = Column(DECIMAL(10, 7), nullable=True)
    isp = Column(String(200), nullable=True)
    user_agent = Column(Text, nullable=True)
    referrer = Column(String(500), nullable=True)
    page_url = Column(String(500), nullable=True)
    visited_at = Column(DateTime, nullable=False)
    is_backfill = Column(Integer, default=0)

    __table_args__ = (
        Index('idx_ip', 'ip'),
        Index('idx_visited_at', 'visited_at'),
        {'extend_existing': True},
    )
