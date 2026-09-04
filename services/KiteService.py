import datetime
import os
from dotenv import load_dotenv
from kiteconnect import KiteConnect
from kiteconnect import exceptions as kite_exceptions
from services.Base_Service import BaseService
from models.googleTokens import UserToken
from utils.DateTimeUtil import IST
from utils.logger import Logger

load_dotenv()

# Zerodha invalidates every access token at 6:00 AM IST the day after it was
# issued -- "will expire at 6 AM on the next day (regulatory requirement)"
# (https://kite.trade/docs/connect/v3/user/#login-flow).
#
# There is NO programmatic renewal available to us: `renew_access_token` exists
# in the SDK but the refresh_token it needs is "only available to certain
# approved platforms", which this app is not. The only way to get a fresh token
# is for the user to walk the login flow again, which is why an expired token
# has to surface as a "reconnect" prompt rather than being retried.
KITE_TOKEN_EXPIRY_HOUR_IST = 6


class KiteAuthError(Exception):
    """Base class for Kite authentication problems that require user action."""

    #: Consumers use this to decide whether to show a "Reconnect Kite" prompt.
    reconnect_required = True


class KiteTokenMissing(KiteAuthError):
    """No Kite token stored for this user -- they have never connected."""


class KiteTokenExpired(KiteAuthError):
    """Stored Kite token is past its 6 AM IST expiry, or Kite rejected it."""


def next_kite_token_expiry(issued_at=None):
    """Epoch seconds of the 6:00 AM IST boundary at which a token issued at
    ``issued_at`` stops working.

    Kite expires tokens at 6 AM IST *the next day*, so a token issued at 3 AM
    IST is only good for three hours while one issued at 7 AM IST lasts ~23.
    Both cases are just "the next 6 AM IST boundary strictly after issue", so
    that is what we compute -- never `now + 24h`, which would overstate the
    lifetime of an early-morning token.
    """
    now_ist = (issued_at or datetime.datetime.now(IST)).astimezone(IST)
    boundary = now_ist.replace(
        hour=KITE_TOKEN_EXPIRY_HOUR_IST, minute=0, second=0, microsecond=0
    )
    if boundary <= now_ist:
        boundary += datetime.timedelta(days=1)
    return int(boundary.timestamp())


class KiteService(BaseService):

    def __init__(self):
        super().__init__()
        self.logger = Logger(__name__).get_logger()
        self.api_key = os.getenv('KITE_API_KEY')
        self.api_secret = os.getenv('KITE_API_SECRET')
        self.kite = None

        if not self.api_key or not self.api_secret:
            self.logger.error("Kite API credentials not found in environment variables")
            return

        self.kite = KiteConnect(api_key=self.api_key)

    def _require_client(self):
        """Fail loudly (and typed) when the SDK client was never constructed.

        Previously `self.kite` simply did not exist when credentials were
        missing, so every call died with an opaque AttributeError.
        """
        if self.kite is None:
            raise KiteAuthError(
                "Kite API credentials are not configured (KITE_API_KEY / KITE_API_SECRET)."
            )
        return self.kite

    def _get_user_access_token(self, user_id):
        """Get user's Kite access token from database.

        Returns None when there is no usable token; use :meth:`get_token_status`
        when you need to tell "never connected" apart from "expired".
        """
        try:
            user_token = self.db.session.query(UserToken).filter_by(
                user_id=user_id,
                service_type='kite'
            ).first()

            if not user_token or not user_token.access_token:
                return None

            if self._is_token_expired(user_token):
                self.logger.warning(
                    f"Kite access token for user {user_id} expired at "
                    f"{user_token.expiry} (6 AM IST boundary); reconnect required"
                )
                return None

            return user_token.access_token
        except Exception as e:
            self.logger.error(f"Error fetching user access token: {str(e)}")
            return None

    @staticmethod
    def _is_token_expired(user_token, now_epoch=None):
        """True when a stored token is past its expiry.

        `expiry == 0` means the row was written by the pre-fix code, which
        wrongly assumed Kite tokens never expire. We cannot know when such a
        token was issued, so we treat it as expired and make the user
        reconnect once -- safer than handing a dead token to Kite forever.
        """
        if not user_token.expiry:
            return True
        now_epoch = now_epoch if now_epoch is not None else datetime.datetime.now(IST).timestamp()
        return user_token.expiry <= now_epoch

    def _authed_kite(self, user_id):
        """Return the SDK client with this user's token applied, or raise.

        Every data call goes through here so the expiry check and the typed
        errors cannot be forgotten at a new call site.
        """
        kite = self._require_client()
        access_token = self._get_user_access_token(user_id)
        if not access_token:
            raise KiteTokenExpired(
                "Kite session has expired or is not set up. Please reconnect Kite."
            )
        kite.set_access_token(access_token)
        return kite

    def get_token_status(self, user_id):
        """Describe the user's Kite connection for the 'Reconnect Kite' prompt."""
        try:
            user_token = self.db.session.query(UserToken).filter_by(
                user_id=user_id,
                service_type='kite'
            ).first()
        except Exception as e:
            self.logger.error(f"Error reading Kite token status: {str(e)}")
            return {"connected": False, "reconnect_required": True, "reason": "lookup_failed"}

        if not user_token or not user_token.access_token:
            return {"connected": False, "reconnect_required": True, "reason": "never_connected"}

        if self._is_token_expired(user_token):
            reason = "unknown_expiry" if not user_token.expiry else "expired"
            return {"connected": False, "reconnect_required": True, "reason": reason,
                    "expires_at": self._expiry_iso(user_token.expiry)}

        return {"connected": True, "reconnect_required": False,
                "expires_at": self._expiry_iso(user_token.expiry)}

    @staticmethod
    def _expiry_iso(expiry_epoch):
        if not expiry_epoch:
            return None
        return datetime.datetime.fromtimestamp(expiry_epoch, IST).isoformat()

    def _save_user_access_token(self, user_id, access_token):
        """Save user's Kite access token to database, with its real expiry."""
        try:
            expiry = next_kite_token_expiry()
            existing_token = self.db.session.query(UserToken).filter_by(
                user_id=user_id,
                service_type='kite'
            ).first()

            if existing_token:
                existing_token.access_token = access_token
                # Must be refreshed too: a re-login issues a NEW token with a
                # NEW expiry, and leaving the old value here would keep the
                # session looking stale (or falsely fresh) forever.
                existing_token.expiry = expiry
                existing_token.client_id = self.api_key
            else:
                new_token = UserToken(
                    user_id=user_id,
                    access_token=access_token,
                    # Kite does not issue refresh tokens to unapproved platforms,
                    # so there is genuinely nothing to store here -- but the token
                    # itself very much does expire (see next_kite_token_expiry).
                    refresh_token="",
                    client_id=self.api_key,
                    client_secret="",  # Don't store secret
                    expiry=expiry,
                    service_type='kite'
                )
                self.db.session.add(new_token)

            self.db.session.commit()
            self.logger.info(
                f"Saved Kite access token for user {user_id} "
                f"(expires {self._expiry_iso(expiry)})"
            )
            return True

        except Exception as e:
            self.db.session.rollback()
            self.logger.error(f"Error saving user access token: {str(e)}")
            return False

    def generate_session(self, user_id, request_token):
        """Generate session using request token from Kite login flow"""
        try:
            kite = self._require_client()
            data = kite.generate_session(request_token, api_secret=self.api_secret)
            access_token = data["access_token"]

            # Save the access token for this user
            if self._save_user_access_token(user_id, access_token):
                self.logger.info(f"Kite session generated successfully for user {user_id}")
                return {
                    "access_token": access_token,
                    "user_type": data.get("user_type"),
                    "user_id": data.get("user_id"),
                    "user_name": data.get("user_name"),
                    "email": data.get("email")
                }
            else:
                raise Exception("Failed to save access token")

        except Exception as e:
            self.logger.error(f"Error generating Kite session: {str(e)}")
            raise

    def _call(self, user_id, description, operation):
        """Run a Kite SDK call with the user's token and uniform error handling.

        Kite can reject a token before our own expiry clock says it should --
        the user may have logged out, or done a master-logout from Kite Web.
        Both paths have to end at the same place: a typed KiteTokenExpired that
        consumers turn into a "Reconnect Kite" prompt, never a bare 500.
        """
        kite = self._authed_kite(user_id)
        try:
            return operation(kite)
        except kite_exceptions.TokenException as e:
            self.logger.warning(
                f"Kite rejected the stored token while fetching {description} "
                f"for user {user_id}: {str(e)}"
            )
            self._mark_token_expired(user_id)
            raise KiteTokenExpired(
                "Kite session is no longer valid. Please reconnect Kite."
            ) from e
        except Exception as e:
            self.logger.error(f"Error fetching {description}: {str(e)}")
            raise

    def _mark_token_expired(self, user_id):
        """Zero the stored expiry so later calls fail fast without hitting Kite.

        Best-effort: a failure here must not mask the original auth error, so
        it is logged and swallowed.
        """
        try:
            user_token = self.db.session.query(UserToken).filter_by(
                user_id=user_id,
                service_type='kite'
            ).first()
            if user_token and user_token.expiry:
                user_token.expiry = 0
                self.db.session.commit()
        except Exception as e:
            self.db.session.rollback()
            self.logger.error(f"Could not mark Kite token expired for {user_id}: {str(e)}")

    def check_token_liveness(self, user_id):
        """Cheapest possible 'is this token still good?' probe.

        `profile()` is the lightest authenticated endpoint Kite exposes, so
        long-running consumers (the scheduler) can check once up front instead
        of discovering the expiry midway through a batch.
        """
        try:
            self._call(user_id, "profile (token liveness probe)", lambda k: k.profile())
            return True
        except KiteAuthError:
            return False

    def get_holdings(self, user_id):
        """Fetch user's holdings from Kite"""
        holdings = self._call(user_id, "holdings", lambda k: k.holdings())
        self.logger.info(f"Fetched {len(holdings)} holdings from Kite for user {user_id}")
        return holdings

    def get_positions(self, user_id):
        """Fetch user's positions from Kite"""
        positions = self._call(user_id, "positions", lambda k: k.positions())
        self.logger.info(f"Fetched positions from Kite for user {user_id}")
        return positions

    def get_profile(self, user_id):
        """Get user profile from Kite"""
        profile = self._call(user_id, "profile", lambda k: k.profile())
        self.logger.info(f"Fetched user profile from Kite for user {user_id}")
        return profile

    def get_login_url(self):
        """Get Kite Connect login URL"""
        try:
            return self._require_client().login_url()
        except Exception as e:
            self.logger.error(f"Error generating login URL: {str(e)}")
            raise

    def get_all_instruments(self, user_id):
        """Fetch all instruments from Kite (CSV format) - Used by background task"""
        instruments = self._call(user_id, "instruments", lambda k: k.instruments())
        self.logger.info(f"Fetched {len(instruments)} instruments from Kite for user {user_id}")
        return instruments

    def get_trades(self, user_id):
        """Fetch today's executed trades from Kite"""
        trades = self._call(user_id, "trades", lambda k: k.trades())
        self.logger.info(f"Fetched {len(trades)} trades from Kite for user {user_id}")
        return trades

    def get_orders(self, user_id):
        """Fetch today's orders from Kite"""
        orders = self._call(user_id, "orders", lambda k: k.orders())
        self.logger.info(f"Fetched {len(orders)} orders from Kite for user {user_id}")
        return orders

    def get_margins(self, user_id):
        """Fetch user's fund/margin details from Kite"""
        margins = self._call(user_id, "margins", lambda k: k.margins())
        self.logger.info(f"Fetched margins from Kite for user {user_id}")
        return margins

    def get_mf_holdings(self, user_id):
        """Fetch mutual fund holdings from Kite (Coin)"""
        holdings = self._call(user_id, "MF holdings", lambda k: k.mf_holdings())
        self.logger.info(f"Fetched {len(holdings)} MF holdings from Kite for user {user_id}")
        return holdings

    def get_mf_orders(self, user_id):
        """Fetch mutual fund orders from Kite (last 7 days)"""
        orders = self._call(user_id, "MF orders", lambda k: k.mf_orders())
        self.logger.info(f"Fetched {len(orders)} MF orders from Kite for user {user_id}")
        return orders

    def get_mf_sips(self, user_id):
        """Fetch mutual fund SIPs from Kite"""
        sips = self._call(user_id, "MF SIPs", lambda k: k.mf_sips())
        self.logger.info(f"Fetched {len(sips)} MF SIPs from Kite for user {user_id}")
        return sips