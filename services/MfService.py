from abc import ABC

import requests
from sqlalchemy.exc import NoResultFound
from werkzeug.routing import ValidationError

from models.purchasedSecurities import PurchasedSecurities
from models.soldSecurities import SoldSecurities
from services.Base_MSN import Base_MSN


class MfService(Base_MSN, ABC):

    def __init__(self):
        super().__init__()
        self.baseAPIURL = "https://api.mfapi.in/"

    def fetchAllSecurities(self):
        return self.JsonDownloadService.getMfList()

    def findSecurity(self, securityCode):
        return self.JsonDownloadService.getMFRate(securityCode)

    def buySecurity(self, security_data, userId):
        pass

    def sellSecurity(self, securityCode, userId):
        pass
