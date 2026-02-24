from abc import ABC
from decimal import Decimal

from sqlalchemy.exc import NoResultFound

from enums.MsnEnum import MSNENUM

from models.purchasedSecurities import PurchasedSecurities
from models.securities import SoldSecurities
from services.Base_MSN import Base_MSN
from utils.logger import Logger


class MfService(Base_MSN, ABC):

    def __init__(self):
        super().__init__()
        self.baseAPIURL = "https://api.mfapi.in/"
        self.logger = Logger(__name__).get_logger()

    def fetchAllSecurities(self):
        return self.JsonDownloadService.getMfList()

    def findSecurity(self, securityCode):
        securityItem = self.JsonDownloadService.getMFRate(securityCode)
        if not securityItem:
            self.logger.warning(f"No rate data found for MF scheme: {securityCode}")
            return {'error': 'RATE_NOT_FOUND', 'scheme_id': securityCode}
        secName = self.JsonDownloadService.getMfNameForSchemeId(securityCode)
        securityItem['companyName'] = secName
        return securityItem

    def buySecurity(self, security_data, userId):
        try:
            # Validate quantity and price are positive
            if Decimal(security_data['buyQuant']) <= 0 or Decimal(security_data['buyPrice']) <= 0:
                return {"error": "Quantity and price must be positive"}
            # Validate the securityCode using the separate function
            if not self.checkIfSecurityExists(str(security_data['securityCode'])):
                return {"error": "Invalid code"}
            # Check if the user has the same security bought already. If yes add
            existingRow: PurchasedSecurities = self.findIdIfSecurityBought(userId, security_data['securityCode'])
            # Manage Date
            date = security_data.get('date')
            if date is None:
                date = self.dateTimeUtil.getCurrentDatetimeSqlFormat()
            transactionObject = dict(date=date, quant=security_data['buyQuant'], price=security_data['buyPrice'],
                                     transactionType="buy", userID=userId, securityType="Mutual_Funds")

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
                    securityType=MSNENUM.Mutual_Funds.value
                )

                self.db.session.add(new_purchase)
            else:
                # We update the old purchase by finding average of price
                transactionObject['buyId'] = existingRow.buyID
                newQuant = existingRow.buyQuant + Decimal(security_data['buyQuant'])
                newPrice = ((existingRow.buyPrice * existingRow.buyQuant) + (
                        Decimal(security_data['buyQuant']) * Decimal(security_data['buyPrice']))) / newQuant
                self.updatePriceAndQuant(newPrice, newQuant, existingRow.buyID)
            self.insert_security_transaction(transactionObject)
            self.db.session.commit()
            return {"message": "Security purchased successfully"}

        except Exception as e:
            self.logger.error(f"Error buying MF security: {e}")
            return {"error": str(e)}

    def sellSecurity(self, sell_data, userId):
        try:
            # Validate quantity and price are positive
            if Decimal(sell_data['sellQuant']) <= 0 or Decimal(sell_data['sellPrice']) <= 0:
                return {"error": "Quantity and price must be positive"}
            # Fetch the corresponding purchase record
            purchase = self.findIdIfSecurityBought(userId, sell_data['securityCode'])
            if purchase is None:
                return {"error": "Purchase record not found"}

            if sell_data['sellQuant'] > purchase.buyQuant:
                return {"error": "Sell quantity exceeds available quantity"}

            # Calculate profit using the averaged buyPrice from the record
            profit = (Decimal(sell_data['sellQuant']) * Decimal(sell_data['sellPrice'])) - (
                    Decimal(sell_data['sellQuant']) * purchase.buyPrice)

            # Reduce quantity purchased
            purchase.buyQuant -= sell_data['sellQuant']

            # Manage date
            date = sell_data.get('date')
            if date is None:
                date = self.dateTimeUtil.getCurrentDatetimeSqlFormat()

            # Insert transaction into separate table
            transactionObject = dict(date=date, quant=sell_data['sellQuant'], price=sell_data['sellPrice'],
                                     transactionType="sell", userID=userId, securityType=MSNENUM.Mutual_Funds.value,
                                     buyId=purchase.buyID)
            self.insert_security_transaction(transactionObject)

            # Insert into SoldSecurities
            new_sale = SoldSecurities(
                buyID=purchase.buyID,
                date=date,
                sellQuant=sell_data['sellQuant'],
                sellPrice=sell_data['sellPrice'],
                profit=profit,
                source_type='purchased'
            )

            self.db.session.add(new_sale)
            self.db.session.commit()
            return {"message": "Security sold successfully", "sellID": new_sale.sellID, "profit": profit}
        except NoResultFound:
            return {"error": "Purchase record not found for the given buyID"}

    def checkIfSecurityExists(self, symbol):
        mfList = self.JsonDownloadService.getMfList()
        mfList = mfList['data']
        symbol_str = str(symbol)
        for scheme in mfList:
            if symbol_str == str(scheme['schemeCode']):
                return True
        return False
