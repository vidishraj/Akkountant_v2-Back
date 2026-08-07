import os

from utils.DotDict import DotDict
from utils.GDriveServiceUtils import GdriveServiceUtils
from utils.GenericUtils import GenericUtil
from utils.GmailServiceUtils import GmailServiceUtils
from flask import g, current_app
from utils.DateTimeUtil import DateTimeUtil
from utils.logger import Logger as _AppLogger

from flask_sqlalchemy import SQLAlchemy
from logging import Logger

from sqlalchemy import create_engine
from sqlalchemy.orm import sessionmaker, scoped_session


# ak-ojd Path B: module-level logger for the property's warn-on-
# legacy-fallback branch. BaseService itself doesn't init a logger
# (subclasses do), so we can't rely on self.logger inside the
# @property. Module-level bind is cheap.
_logger = _AppLogger(__name__).get_logger()


class BaseService:
    gmailService: GmailServiceUtils
    genericUtil: GenericUtil
    db: SQLAlchemy
    logger: Logger

    @property
    def db(self):
        """Retrieve the database session.

        Preference order (post ak-ojd Path B):
          1. `g.db` — set by reprocess_*.py / retry_*.py standalone
             scripts + by TaskScheduler (defense-in-depth for
             scheduler-driven background tasks).
          2. `current_app.db` — Flask-SQLAlchemy singleton bound at
             Akkountant._setup_database time. Stable engine + pool
             for the whole app lifetime. This is the ak-ojd Path B
             fix: closes ak-6si / ak-m6k class fleet-wide by making
             the primitive itself return a stable engine instead of
             materializing a fresh create_engine + scoped_session on
             every attribute access.
          3. **LEGACY FALLBACK** — fresh create_engine + scoped_session
             per access. Only reached if there's NO Flask app context
             at all (rare: broken test setup, out-of-band utility).
             Loud WARN so misconfigured callers surface. This branch
             leaks connections + is the exact pre-Path-B shape that
             caused ak-6si + ak-m6k; new callers should NOT rely on
             it.

        Pre-Path-B: every `self.db` access materialized a fresh
        `create_engine(DATABASE_URL)` + `scoped_session(sessionmaker
        (bind=engine))` + `DotDict({'session': ...})`. That meant:
          * NEW connection pool per access (never disposed) →
            resource leak.
          * Cross-method read+write on the same instance hit DIFFERENT
            engines → snapshot inconsistency (INSTANCE 1 ak-6si
            create-side, INSTANCE 2 ak-m6k read-side).
        Post-Path-B: single app-level Flask-SQLAlchemy engine, pool
        reused across all self.db accesses, snapshot consistency
        preserved by construction.
        """
        # 1. Explicit g.db wins (reprocess_*.py / scheduler convention).
        if g.get('db') is not None:
            return g.db
        # 2. Fall back to current_app.db (Flask-SQLAlchemy singleton).
        # try_current_app: current_app is a proxy; touching any
        # attribute outside an app_context raises RuntimeError. That
        # tells us to fall through to the legacy branch.
        try:
            app_db = current_app.db
        except RuntimeError:
            app_db = None
        except AttributeError:
            # current_app resolves but .db isn't set — this would be
            # a pre-_setup_database boot path (unlikely at runtime;
            # only during startup before Akkountant._setup_database
            # completes). Fall through.
            app_db = None
        if app_db is not None:
            return app_db
        # 3. Legacy fallback — historically-working callers outside
        # an app context. Leaks connections; loud WARN.
        _logger.warning(
            "BaseService.db: called with no g.db AND no Flask app "
            "context — returning legacy fresh-engine + scoped_session. "
            "This branch leaks connections (create_engine per access) "
            "and was the root of ak-6si/ak-m6k. Wrap the caller in "
            "app.app_context() so current_app.db is reachable, or "
            "set g.db = app.db explicitly (see reprocess_*.py pattern)."
        )
        DATABASE_URL = os.getenv('DATABASE_URL')
        engine = create_engine(DATABASE_URL)
        db_session = scoped_session(sessionmaker(autocommit=False,
                                                 autoflush=False,
                                                 bind=engine))
        return DotDict({'session': db_session})

    def __init__(self):
        self.gmailService = GmailServiceUtils()
        self.driveService = GdriveServiceUtils()
        self.genericUtil = GenericUtil()
        self.dateTimeUtil = DateTimeUtil()
