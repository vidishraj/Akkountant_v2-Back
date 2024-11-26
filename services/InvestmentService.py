from flask_sqlalchemy import SQLAlchemy

from enums.MsnEnum import MSNENUM
from services.MfService import MfService
from services.NpsService import NPSService
from services.StocksService import StocksService
from utils.logger import Logger


class InvestmentService:
    db: SQLAlchemy
    logger: Logger

    def __init__(self):
        self.logger = Logger(__name__).get_logger()
        self.StockService = StocksService()
        self.NPSService = NPSService()
        self.MFService = MfService()

    def fetchAllSecurities(self, securityType: MSNENUM):
        if securityType == MSNENUM.Stocks:
            return self.StockService.fetchAllSecurities()
        elif securityType == MSNENUM.Mutual_Funds:
            return self.MFService.fetchAllSecurities()
        elif securityType == MSNENUM.NPS:
            return self.NPSService.fetchAllSecurities()

    def fetchSecuritySchemeRate(self, securityType: MSNENUM, schemeCode: str):
        if securityType == MSNENUM.Stocks:
            return self.StockService.findSecurity(schemeCode)
        elif securityType == MSNENUM.Mutual_Funds:
            return self.MFService.findSecurity(schemeCode)
        elif securityType == MSNENUM.NPS:
            return self.NPSService.findSecurity(schemeCode)

    def processFilesForMSN(self, serviceType, file_path, userId):
        # Route the file processing based on the service type
        if serviceType == MSNENUM.Stocks:
            self.StockService.readFromStatement(file_path, userId)
        elif serviceType == MSNENUM.NPS:
            # @TODO
            pass
