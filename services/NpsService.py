from abc import ABC

import requests
from sqlalchemy.exc import NoResultFound
from werkzeug.routing import ValidationError

from models.purchasedSecurities import PurchasedSecurities
from models.soldSecurities import SoldSecurities
from services.Base_MSN import Base_MSN


class NPSService(Base_MSN, ABC):

    def __init__(self):
        super().__init__()
        self.baseAPIURL = "https://nps.purifiedbytes.com/api/"

    def fetchAllSecurities(self):
        return self.JsonDownloadService.getNPSList()

    def findSecurity(self, securityCode):
        return  self.JsonDownloadService.getNPSRate(securityCode)

    def buySecurity(self, security_data, userId):
        try:

            # Proceed with insertion if validation passes
            new_purchase = PurchasedSecurities(
                securityCode=security_data['securityCode'],
                buyQuant=security_data['buyQuant'],
                buyPrice=security_data['buyPrice'],
                userID=security_data['userID'],
                securityType=security_data['securityType']
            )

            self.db.session.add(new_purchase)
            self.db.session.commit()
            return {"message": "Security purchased successfully", "buyID": new_purchase.buyID}

        except ValidationError as e:
            return {"error": str(e)}

    def sellSecurity(self, sell_data, userId):
        try:
            # Fetch the corresponding purchase record
            purchase = self.db.session.query(PurchasedSecurities).filter_by(buyID=sell_data['buyID']).one()

            if sell_data['sellQuant'] > purchase.buyQuant:
                return {"error": "Sell quantity exceeds available quantity"}

            # Calculate profit
            profit = (sell_data['sellQuant'] * sell_data['sellPrice']) - (sell_data['sellQuant'] * purchase.buyPrice)

            # Insert into SoldSecurities
            new_sale = SoldSecurities(
                buyID=sell_data['buyID'],
                sellQuant=sell_data['sellQuant'],
                sellPrice=sell_data['sellPrice'],
                profit=profit
            )

            self.db.session.add(new_sale)
            self.db.session.commit()
            return {"message": "Security sold successfully", "sellID": new_sale.sellID, "profit": profit}

        except NoResultFound:
            return {"error": "Purchase record not found for the given buyID"}

    def readFromStatement(self, file_path: str, userId):
        """
        We will be reading the NPS statement here
        :return:
        """
