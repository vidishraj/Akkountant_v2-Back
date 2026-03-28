from flask import g
from sqlalchemy import func, distinct, case, text
from models.portfolioVisitors import PortfolioVisitor
from utils.logger import Logger


class PortfolioVisitorService:

    def __init__(self):
        self.logger = Logger(__name__).get_logger()

    @property
    def db(self):
        return g.db

    def get_visitors(self, page=1, per_page=50):
        """Fetch recent visitors with pagination."""
        query = self.db.session.query(PortfolioVisitor).order_by(
            PortfolioVisitor.visited_at.desc()
        )
        total = query.count()
        visitors = query.offset((page - 1) * per_page).limit(per_page).all()

        return {
            "visitors": [self._serialize(v) for v in visitors],
            "pagination": {
                "page": page,
                "per_page": per_page,
                "total": total,
                "pages": (total + per_page - 1) // per_page,
            }
        }

    def get_stats(self):
        """Get visitor statistics: daily unique, by country, live vs backfill."""
        session = self.db.session

        # Total counts
        total = session.query(func.count(PortfolioVisitor.id)).scalar() or 0
        unique_ips = session.query(func.count(distinct(PortfolioVisitor.ip))).scalar() or 0

        # Live vs backfill
        live_count = session.query(func.count(PortfolioVisitor.id)).filter(
            PortfolioVisitor.is_backfill == 0
        ).scalar() or 0
        backfill_count = total - live_count

        # Unique visitors per day (last 30 days)
        # Convert UTC visited_at to IST (+05:30) before grouping by date
        # Use ADDTIME instead of CONVERT_TZ to avoid needing MySQL timezone tables
        ist_date = func.date(func.addtime(PortfolioVisitor.visited_at, text("'05:30:00'")))
        daily_query = session.query(
            ist_date.label('day'),
            func.count(distinct(PortfolioVisitor.ip)).label('unique_visitors'),
            func.count(PortfolioVisitor.id).label('total_visits'),
        ).group_by(
            ist_date
        ).order_by(
            text('day DESC')
        ).limit(30).all()

        daily = [
            {"day": str(row.day), "unique_visitors": row.unique_visitors, "total_visits": row.total_visits}
            for row in daily_query
        ]

        # Visitors by country (only live with geo data)
        country_query = session.query(
            PortfolioVisitor.country,
            PortfolioVisitor.country_code,
            func.count(PortfolioVisitor.id).label('visits'),
            func.count(distinct(PortfolioVisitor.ip)).label('unique_ips'),
        ).filter(
            PortfolioVisitor.country.isnot(None),
            PortfolioVisitor.country != '',
        ).group_by(
            PortfolioVisitor.country, PortfolioVisitor.country_code
        ).order_by(text('visits DESC')).all()

        countries = [
            {"country": row.country, "country_code": row.country_code,
             "visits": row.visits, "unique_ips": row.unique_ips}
            for row in country_query
        ]

        # Top cities
        city_query = session.query(
            PortfolioVisitor.city,
            PortfolioVisitor.country,
            func.count(PortfolioVisitor.id).label('visits'),
        ).filter(
            PortfolioVisitor.city.isnot(None),
            PortfolioVisitor.city != '',
        ).group_by(
            PortfolioVisitor.city, PortfolioVisitor.country
        ).order_by(text('visits DESC')).limit(15).all()

        cities = [
            {"city": row.city, "country": row.country, "visits": row.visits}
            for row in city_query
        ]

        return {
            "total_visits": total,
            "unique_visitors": unique_ips,
            "live_visits": live_count,
            "backfill_visits": backfill_count,
            "daily": daily,
            "countries": countries,
            "cities": cities,
        }

    @staticmethod
    def _serialize(v: PortfolioVisitor) -> dict:
        return {
            "id": v.id,
            "ip": v.ip,
            "city": v.city,
            "region": v.region,
            "country": v.country,
            "country_code": v.country_code,
            "lat": float(v.lat) if v.lat else None,
            "lon": float(v.lon) if v.lon else None,
            "isp": v.isp,
            "user_agent": v.user_agent,
            "referrer": v.referrer,
            "page_url": v.page_url,
            "visited_at": v.visited_at.isoformat() if v.visited_at else None,
            "is_backfill": bool(v.is_backfill),
        }
