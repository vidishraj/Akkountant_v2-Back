from abc import ABC

import requests
from sqlalchemy.exc import NoResultFound
from werkzeug.routing import ValidationError

from enums.MsnEnum import MSNENUM
from models.purchasedSecurities import PurchasedSecurities
from models.soldSecurities import SoldSecurities
from services.Base_MSN import Base_MSN
from services.parsers.NPS_Statement import NPSParser


class NPSService(Base_MSN, ABC):

    def __init__(self):
        super().__init__()
        self.baseAPIURL = "https://nps.purifiedbytes.com/api/"
        self.parser = NPSParser()

    def fetchAllSecurities(self):
        return self.JsonDownloadService.getNPSList()

    def findSecurity(self, securityCode):
        return self.JsonDownloadService.getNPSRate(securityCode)

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
            if existingRow is None:
                # Proceed with insertion if validation passes and not existing
                new_purchase = PurchasedSecurities(
                    securityCode=security_data['securityCode'],
                    date=date,
                    buyQuant=security_data['buyQuant'],
                    buyPrice=security_data['buyPrice'],
                    userID=userId,
                    securityType=MSNENUM.NPS
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

    def readFromStatement(self, file_path: str, userId):
        """
        We will be reading the NPS statement here
        :return:
        """

        self.parser.setPath(file_path)
        npsTransactions = self.parser.parseFile()
        for npsTransaction in npsTransactions:
            schemeCode = self.JsonDownloadService.getNpsSchemeCodeSchemeName(npsTransactions['schemeName'])
            self.buySecurity({
                'securityCode': schemeCode,
                'buyQuant': npsTransaction['buyQuant'],
                'buyPrice': npsTransaction['buyPrice']
            }, userId)

    def checkIfSecurityExists(self, symbol):
        npsList = self.JsonDownloadService.getNPSList()
        npsList = npsList['data']
        for scheme in npsList:
            if symbol == scheme:
                return True
        return False
