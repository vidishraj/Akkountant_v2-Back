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
            
            try:
                filePath = os.path.join(self.tmp_dir, 'StockDetails.json')
                # delete file if it exists
                try:
                    os.remove(filePath)
                except OSError:
                    pass
                self.save_json(jsonData, filePath)

                # get the latest rate file in assets
                latestFile = self.jsonService.getLatestFile(self.jsonService.listType, self.jsonService.StockListPrefix)

                latestFilePath = self.jsonService.getFilePath(self.jsonService.StockListPrefix,
                                                              self.jsonService.listType)

                fileMoved = self.move_file(filePath, latestFilePath)

                if fileMoved:
                    # delete old file
                    self.jsonService.deleteFile(latestFile)
                else:
                    return 'Failed to move file', "Failed", self.interval
                    
                self.logger.info(f"Successfully processed {len(equity_instruments)} equity instruments from Kite")
                return 'Completed successfully', "Completed", self.interval
            except Exception as ex:
                return ex.__str__(), "Failed", self.interval
        except Exception as ex:
            return ex.__str__(), "Failed", self.interval

    def get_first_kite_user(self):
        """Get the first user ID that has a Kite access token"""
        try:
            from models.googleTokens import UserToken
            
            user_token = self.db.session.query(UserToken).filter_by(
                service_type='kite'
            ).first()
            
            if user_token and user_token.access_token:
                return user_token.user_id
            return None
        except Exception as e:
            self.logger.error(f"Error getting Kite user: {str(e)}")
            return None