
from abc import ABC

from sqlalchemy.exc import NoResultFound, SQLAlchemyError
from werkzeug.routing import ValidationError

from enums.MsnEnum import MSNENUM
from models import PurchasedSecurities
from models import SoldSecurities
from models.stockTrade import TradeAssociation
from services.Base_MSN import Base_MSN
import pandas as pd
import nsepython
from decimal import Decimal, ROUND_DOWN
from utils.logger import Logger


class StocksService(Base_MSN, ABC):

    def __init__(self):
        super().__init__()
        self.baseAPIURL = "https://api.mfapi.in/"
        self.logger = Logger(__name__).get_logger()

    def buySecurity(self, security_data, userId):
        try:
            # Validate the securityCode using the separate function
            if not self.checkIfSecurityExists(security_data['securityCode']):
                return {"error": "Invalid code"}

            # Check if the specific trade has been inserted before
            if self.tradeExists(security_data['tradeID']):
                return {"error": "Trade exists"}

            # Check if the user has the same security bought already. If yes add
            existingRow: PurchasedSecurities = self.findIfSameSecurityTransactionExists(userId, security_data['buyID'])
            # Manage Date
            date = security_data.get('date')
            if date is None:
                date = self.dateTimeUtil.getCurrentDatetimeSqlFormat()
            transactionObject = dict(date=date, quant=security_data['buyQuant'], price=security_data['buyPrice'],
                                     transactionType="buy", userID=userId, securityType=MSNENUM.Stocks.value)
            if existingRow is None:
                # Proceed with insertion if validation passes and not existing
                buyID = security_data.get('buyID')
                # Insert Trade
                transactionObject['buyId'] = buyID
                newTrade = TradeAssociation(
                    tradeID=security_data['tradeID'],
                    buyID=buyID,
                )
                new_purchase = PurchasedSecurities(
                    buyID=buyID,
                    securityCode=security_data['securityCode'],
                    date=date,
                    buyQuant=security_data['buyQuant'],
                    buyPrice=security_data['buyPrice'],
                    userID=userId,
                    securityType=MSNENUM.Stocks.value
                )
                self.db.session.add(new_purchase)
            else:
                # Adding the new trade for existing security
                newTrade = TradeAssociation(
                    tradeID=security_data['tradeID'],
                    buyID=existingRow.buyID,
                )
                transactionObject['buyId'] = existingRow.buyID
                # We update the old purchase by finding average of price
                newQuant = existingRow.buyQuant + security_data['buyQuant']
                newPrice = (((existingRow.buyPrice * existingRow.buyQuant) +
                             (Decimal(security_data['buyQuant']) * Decimal(security_data['buyPrice'])))
                            / newQuant).quantize(Decimal('0.00001'), rounding=ROUND_DOWN)
                self.updatePriceAndQuant(newPrice, newQuant, existingRow.buyID)
            # Finally adding the trade
            self.insert_security_transaction(transactionObject)
            self.db.session.add(newTrade)
            return {"message": "Security purchased successfully"}

        except ValidationError as e:
            return {"error": str(e)}

    def sellSecurity(self, sell_data, userId):
        try:

            # Check if the specific trade has been inserted before
            if self.tradeExists(sell_data['tradeID']):
                return {"error": "Trade exists"}

            # Fetch the corresponding purchase record
            purchase = self.findIdIfSecurityBought(userId, sell_data['securityCode'])
            if purchase is None:
                return {'error': "Chronology error"}
            # Insert Trade
            newTrade = TradeAssociation(
                buyID=purchase.buyID,
                tradeID=sell_data['tradeID'],

            )
            self.db.session.add(newTrade)
            if sell_data['sellQuant'] > purchase.buyQuant:
                return {"error": "Sell quantity exceeds available quantity"}

            # Calculate profit
            profit = (Decimal(sell_data['sellQuant'] * sell_data['sellPrice']) - (
                    sell_data['sellQuant'] * Decimal(purchase.buyPrice))).quantize(Decimal('0.00001'),
                                                                                   rounding=ROUND_DOWN)

            # Reduce quantity purchased
            purchase.buyQuant = (Decimal(purchase.buyQuant) - sell_data['sellQuant'])

            # Deleting if quantity has become 0
            if purchase.buyQuant == 0:
                self.deleteSecurity(purchase.buyID)

            # Manage date
            date = sell_data.get('date')
            if date is None:
                date = self.dateTimeUtil.getCurrentDatetimeSqlFormat()

            # Insert transaction into separate table
            transactionObject = dict(date=date, quant=sell_data['sellQuant'], price=sell_data['sellPrice'],
                                     transactionType="sell", userID=userId, securityType=MSNENUM.Stocks.value,
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
            return {"message": "Security sold successfully", "sellID": new_sale.sellID, "profit": profit}
        except NoResultFound:
            return {"error": "Purchase record not found for the given buyID"}

    def deleteSecurity(self, buyId):
        security = self.db.session.query(PurchasedSecurities).filter(
            PurchasedSecurities.buyID == buyId).first()
        # Setting quant and price to 0 is equivalent
        security.buyQuant = 0
        security.buyPrice = 0

    def readFromStatement(self, file_path, userId):
        """
        We will be reading the Zerodha trade book here
        :return:
        """
        df = pd.read_excel(file_path, header=14)

        # Columns to extract
        columns_to_extract = ["Symbol", "ISIN", "Trade Date", "Exchange", "Trade Type", "Quantity", "Price", "Trade ID"]

        # Extract the specified columns and convert to a list of dictionaries
        trade_data = df[columns_to_extract].to_dict(orient='records')
        buyList = []
        sellList = []
        for trade in trade_data:
            if trade['Trade Type'] == 'buy':
                buyList.append({
                    'buyID': trade['ISIN'],
                    'securityCode': trade['Symbol'],
                    'date': trade['Trade Date'],
                    'buyQuant': trade['Quantity'],
                    'buyPrice': trade['Price'],
                    'tradeID': trade['Trade ID']
                })
            else:
                sellList.append({
                    'securityCode': trade['Symbol'],
                    'date': trade['Trade Date'],
                    'sellQuant': trade['Quantity'],
                    'sellPrice': trade['Price'],
                    'tradeID': trade['Trade ID']
                })
        self.logger.info(f"Read {len(buyList)} purchases and {len(sellList)} sells in the tradebook")
        boughtInserted = 0
        soldInserted = 0
        session = self.db.session
        try:
            with session.begin():  # Start an outer transaction
                # Process buying items
                for item in buyList:
                    insertedResult = self.buySecurity(item, userId)
                    if insertedResult.get('error') is None:
                        boughtInserted += 1
                    self.logger.info(f"Buying: {item.get('securityCode')} - {insertedResult}")
                session.flush()  # Ensure buying changes are sent to the database
                # Process selling items
                for item in sellList:
                    insertedResult = self.sellSecurity(item, userId)
                    if insertedResult.get('error') is None:
                        soldInserted += 1
                    elif insertedResult['error'] == 'Chronology error':
                        raise Exception("Add statements in order!")
                    self.logger.info(f"Selling: {item.get('securityCode')} - {insertedResult}")

            # The commit happens automatically at the end of 'with session.begin()' if no errors occur
            self.logger.info("Transaction committed successfully.")

        except SQLAlchemyError as e:
            session.rollback()  # Roll back the entire transaction if any error occurs
            self.logger.error(f"Transactions failed. Rolled back. Error: {e}")
            raise Exception(e.__str__())  # Reraise the exception after logging

        self.db.session.commit()
        self.logger.info("Finished processing file and inserting statements")
        return {"readFromStatement": {'buy': len(buyList), 'sold': len(sellList)},
                "inserted": {'buy': boughtInserted, 'sold': soldInserted}}

    def getSecurityList(self):
        self.JsonDownloadService.getStockList()

    def findSecurity(self, securityCode):
        try:
            quote = nsepython.nse_eq(securityCode)
            self.logger.info(f"Successfully fetched live data for {securityCode}")
            # @TODO Decide the format of the stocks for frontend
            return quote
        except Exception as ex:
            self.logger.error(f"Error while fetching symbol from NSEPYTHON {ex}")
        pass

    def tradeExists(self, tradeId: str):
        tradeRow = self.db.session.query(TradeAssociation).filter(TradeAssociation.tradeID == tradeId).first()
        if tradeRow:
            return True
        return False

    def checkIfSecurityExists(self, symbol):
        stockList = self.JsonDownloadService.getStockList()
        stockList = stockList['data']
        # @TODO CORNER CASE? How to manage changed codes?
        if symbol == "SUZLON-BE":
            return True
        for stock in stockList:
            if stock.get('stockCode') == symbol:
                return True
        return False
