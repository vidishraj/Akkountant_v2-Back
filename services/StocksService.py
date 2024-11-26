
from abc import ABC

from sqlalchemy.exc import NoResultFound
from werkzeug.routing import ValidationError

from enums.MsnEnum import MSNENUM
from models.purchasedSecurities import PurchasedSecurities
from models.soldSecurities import SoldSecurities
from services.Base_MSN import Base_MSN
import pandas as pd
import nsepython

from utils.logger import Logger


class StocksService(Base_MSN, ABC):

    def __init__(self):
        super().__init__()
        self.baseAPIURL = "https://api.mfapi.in/"
        self.logger = Logger(__name__).get_logger()

    def getSecurityList(self):
        self.JsonDownloadService.getStockList()

    def findSecurity(self, securityCode):
        try:
            quote = nsepython.nse_eq(securityCode)
            self.logger.info(f"Successfully fetched live data for {quote}")
            # @TODO Decide the format of the stocks for frontend
            return {}
        except Exception as ex:
            self.logger.error(f"Error while fetching symbol from NSEPYTHON {ex}")
        pass

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
                    securityType=MSNENUM.Stocks
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

    def readFromStatement(self, file_path, userId):
        """
        We will be reading the Zerodha trade book here
        :return:
        """
        df = pd.read_excel(file_path, header=14)

        # Columns to extract
        columns_to_extract = ["Symbol", "ISIN", "Trade Date", "Exchange", "Trade Type", "Quantity", "Price"]

        # Extract the specified columns and convert to a list of dictionaries
        trade_data = df[columns_to_extract].to_dict(orient='records')
        # Print the result
        buyList = []
        sellList = []
        for trade in trade_data:
            if trade['Trade Type'] == 'buy':
                buyList.append({
                    'securityCode': trade['Symbol'],
                    'date': trade['Trade Date'],
                    'buyQuant': trade['Quantity'],
                    'buyPrice': trade['Price']
                })
            else:
                sellList.append({
                    'securityCode': trade['Symbol'],
                    'date': trade['Trade Date'],
                    'sellQuant': trade['Quantity'],
                    'sellPrice': trade['Price']
                })
        for item in buyList:
            self.buySecurity(item, userId)
        for item in sellList:
            self.sellSecurity(item, userId)
        self.logger.info("Finished processing file and inserting statements")

    def checkIfSecurityExists(self, symbol):
        stockList = self.JsonDownloadService.getStockList()
        stockList = stockList['data']
        for stock in stockList:
            if symbol == stock:
                return True
        return False
