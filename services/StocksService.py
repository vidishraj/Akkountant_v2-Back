import uuid
from abc import ABC

from sqlalchemy.exc import NoResultFound
from werkzeug.routing import ValidationError

from models.purchasedSecurities import PurchasedSecurities
from models.soldSecurities import SoldSecurities
from services.Base_MSN import Base_MSN


class StocksService(Base_MSN, ABC):

    def __init__(self):
        super().__init__()
        self.baseAPIURL = "https://api.mfapi.in/"

    # Implement with UpStox API
    # def fetchAllSecurities(self):
    #     return self.check_and_update_file(self.baseDirectory + 'assets', 'MFList', self.baseAPIURL + 'mf/')

    def buySecurity(self, security_data, filePath, key, userId):
        try:
            # Validate the securityCode using the separate function
            self.validate_security_in_json(filePath, key, security_data['securityCode'])

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
