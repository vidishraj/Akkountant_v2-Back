from abc import ABC
from decimal import Decimal, ROUND_DOWN

from flask import jsonify
from sqlalchemy import and_

from enums.DateFormatEnum import DateStatementEnum
from enums.EPGEnum import EPGEnum
from models import DepositSecurities, PurchasedSecurities
from models.GoldDetails import GoldDetails
from services.BaseEPG import Base_EPG
from services.Base_MSN import Base_MSN
from utils.logger import Logger


class GoldService(Base_EPG, ABC):

    def __init__(self):
        super().__init__()
        self.logger = Logger(__name__).get_logger()

    def insertDeposit(self, data, userId):
        try:
            buyId = self.genericUtil.generate_custom_buyID()
            deposit_security = DepositSecurities(
                buyID=buyId,
                date=self.dateTimeUtil.convert_to_sql_datetime(data['date'], DateStatementEnum.EPF_STATEMENT.name),
                depositDescription=data['description'],
                depositAmount=data['amount'],
                userID=userId,
                securityType=EPGEnum.Gold.name
            )
            insertionObject = self.insertDepositFinal(deposit_security)
            self.insertTransactionType(buyId, data['quantity'], data['goldType'])
            self.logger.info("Inserted gold type details to gold details")
            if 'error' in insertionObject:
                return jsonify({"Error": "Error in Gold entry"}), 406
            return jsonify({"Message": "Gold Transaction inserted successfully"}), 200
        except Exception as ex:
            self.logger.error(f"Failed while inserting Gold Transaction {ex}")
            return jsonify({"error": "Failed while inserting Gold Transaction"}), 500

    def insertTransactionType(self, buyId, quantity, goldType):
        goldDetails = GoldDetails(
            buyID=buyId,
            quantity=quantity,
            goldType=goldType
        )
        self.db.session.add(goldDetails)
        self.db.session.commit()

    def fetchComplete(self, userId):
        # Fetch all the deposits
        deposits = self.get_securities(userId, EPGEnum.Gold.value)
        depositDict = []
        for deposit in deposits:
            depositDict.append({
                'buyId': deposit.buyID,
                "date": deposit.date,
                "description": deposit.depositDescription,
                "amount": deposit.depositAmount,
            })
        transactions = []
        netProfit = 0
        net = 0
        for deposit in deposits:
            goldDetails = self.db.session.query(GoldDetails).filter(
                and_(GoldDetails.buyID == deposit.buyID)).first()
            goldType = goldDetails.goldType
            quantity = goldDetails.quantity
            # v5 (hq-wisp-a0byf, bead ak-4tu): wrap to stop dashboard 500s
            # when the Gold rate file is missing (real prod symptom — the
            # rate fetcher's been broken for ~48d on this account before
            # v4). On FNF, treat the rate as 0 so the row still renders
            # with zero profit instead of bombing the whole endpoint.
            try:
                rate = self.JsonDownloadService.getGoldRate(f"{goldType} Carat")
            except FileNotFoundError:
                self.logger.warning(
                    f"Gold rate file missing for {goldType} Carat; "
                    "treating rate as 0 — fetcher likely broken"
                )
                rate = 0
            profit = quantity * Decimal(rate / 100)
            netProfit += profit
            net += deposit.depositAmount + Decimal(profit)
            transactions.append({
                'date': deposit.date,
                'description': deposit.depositDescription,
                'amount': deposit.depositAmount,
                'quant': quantity,
                'goldType': goldType,
                'interest': profit})
            # calculate the profits based on the deposits
        return {
            'transactions': transactions,
            'deposits': depositDict,
            'netProfit': Decimal(netProfit).quantize(Decimal('0.01'), rounding=ROUND_DOWN),
            'net': Decimal(net).quantize(Decimal('0.01'), rounding=ROUND_DOWN),
        }

    def fetchRates(self):
        return {"data": self.JsonDownloadService.getGoldList()}
