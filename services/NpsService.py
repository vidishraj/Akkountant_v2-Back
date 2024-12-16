from abc import ABC
from decimal import Decimal, ROUND_DOWN

import requests
from sqlalchemy.exc import NoResultFound
from werkzeug.routing import ValidationError

from enums.MsnEnum import MSNENUM
from models import PurchasedSecurities
from models import SoldSecurities
from services.Base_MSN import Base_MSN
from services.parsers.NPS_Statement import NPSParser
from utils.logger import Logger


class NPSService(Base_MSN, ABC):

    def __init__(self):
        super().__init__()
        self.logger = Logger(__name__).get_logger()
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
            transactionObject = dict(date=date, quant=security_data['buyQuant'], price=security_data['buyPrice'],
                                     transactionType="buy", userID=userId, securityType=MSNENUM.NPS.value)
            if existingRow is None:
                # Proceed with insertion if validation passes and not existing
                randomBuyId = self.genericUtil.generate_custom_buyID()
                transactionObject['buyId'] = randomBuyId
                new_purchase = PurchasedSecurities(
                    buyID=randomBuyId,
                    securityCode=security_data['securityCode'],
                    date=date,
                    buyQuant=security_data['buyQuant'],
                    buyPrice=security_data['buyPrice'],
                    userID=userId,
                    securityType=MSNENUM.NPS.value
                )
                self.db.session.add(new_purchase)
            else:
                # We update the old purchase by finding average of price
                transactionObject['buyId'] = existingRow.buyID
                newQuant = existingRow.buyQuant + Decimal(security_data['buyQuant'])
                newPrice = (((existingRow.buyPrice * existingRow.buyQuant) +
                             (Decimal(security_data['buyQuant']) * Decimal(security_data['buyPrice'])))
                            / newQuant).quantize(Decimal('0.00001'), rounding=ROUND_DOWN)
                self.updatePriceAndQuant(newPrice, newQuant, existingRow.buyID)
            self.insert_security_transaction(transactionObject)
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
                                     transactionType="sell", userID=userId, securityType=MSNENUM.NPS.value,
                                     buyId=purchase.buyID)
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

    def readFromStatement(self, file_path: str, userId):
        """
        We will be reading the NPS statement here
        :return:
        """
        self.parser.setPath(file_path)
        npsTransactions = self.parser.parseFile()
        rowsInserted = 0
        for npsTransaction in npsTransactions:
            schemeCode = self.JsonDownloadService.getNpsSchemeCodeSchemeName(npsTransaction['name'])
            self.logger.info(f"Inserting security {schemeCode}")
            status = self.buySecurity({
                'securityCode': schemeCode,
                'buyQuant': npsTransaction['quantity'],
                'buyPrice': npsTransaction['nav']
            }, userId)
            if status.get('error') is None:
                rowsInserted += 1
            self.logger.info(f"Inserted security {schemeCode}")
        self.logger.info("Finished processing file and inserting statements")
        return {"readFromStatement": {'buy': len(npsTransactions), 'sold': 0},
                "inserted": {'buy': rowsInserted, 'sold': 0}}

    def checkIfSecurityExists(self, symbol):
        npsList = self.JsonDownloadService.getNPSList()
        npsList = npsList['data']
        for scheme in npsList:
            if symbol == scheme['id']:
                return True
        return False
