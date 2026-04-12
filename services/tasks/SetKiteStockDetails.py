import os
from services.tasks.baseTask import BaseTask
from services.KiteService import KiteService
from dotenv import load_dotenv
from utils.logger import Logger

load_dotenv()


class SetKiteStockDetails(BaseTask):
    _instance = None

    def __new__(cls, *args, **kwargs):
        if not cls._instance:
            cls._instance = super(SetKiteStockDetails, cls).__new__(cls)
        return cls._instance

    def __init__(self, title, priority):
        if not hasattr(self, 'initialized'):  # Prevent multiple initializations
            super().__init__(title, priority)
            self.logger = Logger(__name__).get_logger()
            self.kite_service = KiteService()
            # 4 hours
            self.interval = 320

    def run(self):
        try:
            # Get a user ID to fetch instruments (you might need to adjust this logic)
            # For now, we'll need to get this from the first available user with Kite access
            user_id = self.get_first_kite_user()
            
            if not user_id:
                return 'No user found with Kite access token', "Failed", self.interval
            
            # Fetch instruments from Kite
            instruments = self.kite_service.get_all_instruments(user_id)
            
            # Filter for equity instruments from NSE and BSE
            equity_instruments = []
            for instrument in instruments:
                if (instrument.get('exchange') in ['NSE', 'BSE'] and 
                    instrument.get('instrument_type') == 'EQ'):
                    equity_instruments.append({
                        'stockCode': instrument.get('tradingsymbol'),
                        'exchange': instrument.get('exchange'),
                        'instrument_token': instrument.get('instrument_token'),
                        'exchange_token': instrument.get('exchange_token'),
                        'name': instrument.get('name'),
                        'lot_size': instrument.get('lot_size'),
                        'tick_size': instrument.get('tick_size'),
                        'segment': instrument.get('segment')
                    })
            
            jsonData = {'data': equity_instruments}
            
            filePath = os.path.join(self.tmp_dir, 'StockDetails.json')
            try:
                os.remove(filePath)
            except OSError:
                pass
            self.save_json(jsonData, filePath)

            ok, err = self.safe_replace_file(filePath, self.jsonService.StockListPrefix, self.jsonService.listType)
            if not ok:
                return err, "Failed", self.interval

            self.logger.info(f"Successfully processed {len(equity_instruments)} equity instruments from Kite")
            return 'Completed successfully', "Completed", self.interval
        except Exception as ex:
            return ex.__str__(), "Failed", self.interval

    def get_first_kite_user(self):
        """Get the first user ID that has a Kite access token"""
        try:
            from models.googleTokens import UserToken
            from sqlalchemy import create_engine
            from sqlalchemy.orm import sessionmaker
            import os
            
            # Create database session
            DATABASE_URL = os.getenv('DATABASE_URL')
            engine = create_engine(DATABASE_URL)
            Session = sessionmaker(bind=engine)
            session = Session()
            
            try:
                user_token = session.query(UserToken).filter_by(
                    service_type='kite'
                ).first()
                
                if user_token and user_token.access_token:
                    return user_token.user_id
                return None
            finally:
                session.close()
                
        except Exception as e:
            self.logger.error(f"Error getting Kite user: {str(e)}")
            return None