from abc import ABC

from sqlalchemy.exc import NoResultFound
from werkzeug.routing import ValidationError

from enums.MsnEnum import MSNENUM

from models.purchasedSecurities import PurchasedSecurities
from models.securities import SoldSecurities
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
        try:
            # Validate the securityCode using the separate function
            if not self.checkIfSecurityExists(security_data['securityCode']):
                return {"error": "Invalid code"}
            # Check if the user has the same security bought already. If yes add
            existingRow: PurchasedSecurities = self.findIdIfSecurityBought(userId, security_data['securityCode'])
            # Manage Date
            date = security_data.get('date')
            if date is None:
                date = self.dateTimeUtil.getCurrentDatetimeSqlFormat()
            transactionObject = dict(date=date, quant=security_data['buyQuant'], price=security_data['buyPrice'],
                                     transactionType="buy", userID=userId, securityType=MSNENUM.Mutual_Funds.value)
            self.insert_security_transaction(transactionObject)

            if existingRow is None:
                # Proceed with insertion if validation passes and not existing
                new_purchase = PurchasedSecurities(
                    securityCode=security_data['securityCode'],
                    date=date,
                    buyQuant=security_data['buyQuant'],
                    buyPrice=security_data['buyPrice'],
                    userID=userId,
                    securityType=MSNENUM.Mutual_Funds
                )

                self.db.session.add(new_purchase)
            else:
                # We update the old purchase by finding average of price
                newQuant = existingRow.buyQuant + security_data['buyQuant']
                newPrice = (existingRow.buyPrice * existingRow.buyQuant) + (
                        security_data['buyQuant'] * security_data['buyPrice']) / newQuant
                self.updatePriceAndQuant(newPrice, newQuant, existingRow.buyID)
            self.db.session.commit()
            return {"message": "Security purchased successfully"}

        except ValidationError as e:
            return {"error": str(e)}

    def sellSecurity(self, sell_data, userId):
        try:
            # Fetch the corresponding purchase record
            purchase = self.findIdIfSecurityBought(userId, sell_data['securityCode'])

            if sell_data['sellQuant'] > purchase.buyQuant:
                return {"error": "Sell quantity exceeds available quantity"}

            # Calculate profit
            profit = (sell_data['sellQuant'] * sell_data['sellPrice']) - (sell_data['sellQuant'] * purchase.buyPrice)

            # Reduce quantity purchased
            purchase.buyQuant -= sell_data['sellQuant']

            # Manage date
            date = sell_data.get('date')
            if date is None:
                date = self.dateTimeUtil.getCurrentDatetimeSqlFormat()

            # Insert transaction into separate table
            transactionObject = dict(date=date, quant=sell_data['sellQuant'], price=sell_data['sellPrice'],
                                     transactionType="sell", userID=userId, securityType=MSNENUM.Mutual_Funds.value)
            self.insert_security_transaction(transactionObject)

            # Insert into SoldSecurities
            new_sale = SoldSecurities(
                buyID=purchase.buyID,
                date=date,
                sellQuant=sell_data['sellQuant'],
                sellPrice=sell_data['sellPrice'],
                profit=profit
            )

            self.db.session.add(new_sale)
            self.db.session.commit()
            return {"message": "Security sold successfully", "sellID": new_sale.sellID, "profit": profit}
        except NoResultFound:
            return {"error": "Purchase record not found for the given buyID"}

    def checkIfSecurityExists(self, symbol):
        mfList = self.JsonDownloadService.getMfList()
        mfList = mfList['data']
        for scheme in mfList:
            if symbol == scheme:
                return True
        return False
