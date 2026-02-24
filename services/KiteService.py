import os
from dotenv import load_dotenv
from kiteconnect import KiteConnect
from services.Base_Service import BaseService
from models.googleTokens import UserToken
from utils.logger import Logger

load_dotenv()


class KiteService(BaseService):
    
    def __init__(self):
        super().__init__()
        self.logger = Logger(__name__).get_logger()
        self.api_key = os.getenv('KITE_API_KEY')
        self.api_secret = os.getenv('KITE_API_SECRET')
        
        if not self.api_key or not self.api_secret:
            self.logger.error("Kite API credentials not found in environment variables")
            return
            
        self.kite = KiteConnect(api_key=self.api_key)

    def _get_user_access_token(self, user_id):
        """Get user's Kite access token from database"""
        try:
            user_token = self.db.session.query(UserToken).filter_by(
                user_id=user_id, 
                service_type='kite'
            ).first()
            
            if user_token:
                return user_token.access_token
            return None
        except Exception as e:
            self.logger.error(f"Error fetching user access token: {str(e)}")
            return None

    def _save_user_access_token(self, user_id, access_token):
        """Save user's Kite access token to database"""
        try:
            existing_token = self.db.session.query(UserToken).filter_by(
                user_id=user_id,
                service_type='kite'
            ).first()
            
            if existing_token:
                existing_token.access_token = access_token
            else:
                new_token = UserToken(
                    user_id=user_id,
                    access_token=access_token,
                    refresh_token="",  # Kite doesn't use refresh tokens
                    client_id=self.api_key,
                    client_secret="",  # Don't store secret
                    expiry=0,  # Kite access tokens don't expire
                    service_type='kite'
                )
                self.db.session.add(new_token)
            
            self.db.session.commit()
            self.logger.info(f"Saved Kite access token for user {user_id}")
            return True
            
        except Exception as e:
            self.db.session.rollback()
            self.logger.error(f"Error saving user access token: {str(e)}")
            return False

    def generate_session(self, user_id, request_token):
        """Generate session using request token from Kite login flow"""
        try:
            data = self.kite.generate_session(request_token, api_secret=self.api_secret)
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

    def get_holdings(self, user_id):
        """Fetch user's holdings from Kite"""
        try:
            access_token = self._get_user_access_token(user_id)
            if not access_token:
                raise ValueError("Access token not found. Please authenticate first.")
            
            self.kite.set_access_token(access_token)
            holdings = self.kite.holdings()
            self.logger.info(f"Fetched {len(holdings)} holdings from Kite for user {user_id}")
            return holdings
        except Exception as e:
            self.logger.error(f"Error fetching holdings: {str(e)}")
            raise

    def get_positions(self, user_id):
        """Fetch user's positions from Kite"""
        try:
            access_token = self._get_user_access_token(user_id)
            if not access_token:
                raise ValueError("Access token not found. Please authenticate first.")
            
            self.kite.set_access_token(access_token)
            positions = self.kite.positions()
            self.logger.info(f"Fetched positions from Kite for user {user_id}")
            return positions
        except Exception as e:
            self.logger.error(f"Error fetching positions: {str(e)}")
            raise

    def get_instruments(self, user_id, exchange=None):
        """Fetch instruments from Kite"""
        try:
            access_token = self._get_user_access_token(user_id)
            if not access_token:
                raise ValueError("Access token not found. Please authenticate first.")
            
            self.kite.set_access_token(access_token)
            instruments = self.kite.instruments(exchange)
            self.logger.info(f"Fetched instruments from Kite for user {user_id}, exchange: {exchange}")
            return instruments
        except Exception as e:
            self.logger.error(f"Error fetching instruments: {str(e)}")
            raise

    def get_profile(self, user_id):
        """Get user profile from Kite"""
        try:
            access_token = self._get_user_access_token(user_id)
            if not access_token:
                raise ValueError("Access token not found. Please authenticate first.")
            
            self.kite.set_access_token(access_token)
            profile = self.kite.profile()
            self.logger.info(f"Fetched user profile from Kite for user {user_id}")
            return profile
        except Exception as e:
            self.logger.error(f"Error fetching profile: {str(e)}")
            raise

    def get_login_url(self):
        """Get Kite Connect login URL"""
        try:
            return self.kite.login_url()
        except Exception as e:
            self.logger.error(f"Error generating login URL: {str(e)}")
            raise

    def get_all_instruments(self, user_id):
        """Fetch all instruments from Kite (CSV format) - Used by background task"""
        try:
            access_token = self._get_user_access_token(user_id)
            if not access_token:
                raise ValueError("Access token not found. Please authenticate first.")

            self.kite.set_access_token(access_token)
            instruments = self.kite.instruments()
            self.logger.info(f"Fetched {len(instruments)} instruments from Kite for user {user_id}")
            return instruments
        except Exception as e:
            self.logger.error(f"Error fetching instruments: {str(e)}")
            raise

    def get_trades(self, user_id):
        """Fetch today's executed trades from Kite"""
        try:
            access_token = self._get_user_access_token(user_id)
            if not access_token:
                raise ValueError("Access token not found. Please authenticate first.")

            self.kite.set_access_token(access_token)
            trades = self.kite.trades()
            self.logger.info(f"Fetched {len(trades)} trades from Kite for user {user_id}")
            return trades
        except Exception as e:
            self.logger.error(f"Error fetching trades: {str(e)}")
            raise

    def get_orders(self, user_id):
        """Fetch today's orders from Kite"""
        try:
            access_token = self._get_user_access_token(user_id)
            if not access_token:
                raise ValueError("Access token not found. Please authenticate first.")

            self.kite.set_access_token(access_token)
            orders = self.kite.orders()
            self.logger.info(f"Fetched {len(orders)} orders from Kite for user {user_id}")
            return orders
        except Exception as e:
            self.logger.error(f"Error fetching orders: {str(e)}")
            raise

    def get_margins(self, user_id):
        """Fetch user's fund/margin details from Kite"""
        try:
            access_token = self._get_user_access_token(user_id)
            if not access_token:
                raise ValueError("Access token not found. Please authenticate first.")

            self.kite.set_access_token(access_token)
            margins = self.kite.margins()
            self.logger.info(f"Fetched margins from Kite for user {user_id}")
            return margins
        except Exception as e:
            self.logger.error(f"Error fetching margins: {str(e)}")
            raise

    def get_mf_holdings(self, user_id):
        """Fetch mutual fund holdings from Kite (Coin)"""
        try:
            access_token = self._get_user_access_token(user_id)
            if not access_token:
                raise ValueError("Access token not found. Please authenticate first.")

            self.kite.set_access_token(access_token)
            holdings = self.kite.mf_holdings()
            self.logger.info(f"Fetched {len(holdings)} MF holdings from Kite for user {user_id}")
            return holdings
        except Exception as e:
            self.logger.error(f"Error fetching MF holdings: {str(e)}")
            raise

    def get_mf_orders(self, user_id):
        """Fetch mutual fund orders from Kite (last 7 days)"""
        try:
            access_token = self._get_user_access_token(user_id)
            if not access_token:
                raise ValueError("Access token not found. Please authenticate first.")

            self.kite.set_access_token(access_token)
            orders = self.kite.mf_orders()
            self.logger.info(f"Fetched {len(orders)} MF orders from Kite for user {user_id}")
            return orders
        except Exception as e:
            self.logger.error(f"Error fetching MF orders: {str(e)}")
            raise

    def get_mf_sips(self, user_id):
        """Fetch mutual fund SIPs from Kite"""
        try:
            access_token = self._get_user_access_token(user_id)
            if not access_token:
                raise ValueError("Access token not found. Please authenticate first.")

            self.kite.set_access_token(access_token)
            sips = self.kite.mf_sips()
            self.logger.info(f"Fetched {len(sips)} MF SIPs from Kite for user {user_id}")
            return sips
        except Exception as e:
            self.logger.error(f"Error fetching MF SIPs: {str(e)}")
            raise