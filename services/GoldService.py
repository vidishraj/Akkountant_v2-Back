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

    # ak-bgc fix: required-field allowlist for Gold inserts. Reported to
    # the agent on missing-field rejections so it can fix the tool call
    # without trial and error. goldType must be one of the IBJA-supported
    # purities — downstream rate lookup keys on "{goldType} Carat" strings
    # like "24 Carat" / "22 Carat" / "18 Carat" (see GoldService.fetchComplete
    # line 77 / SetIBJAGoldRate); anything else silently misses the rate
    # file and renders zero profit.
    _GOLD_REQUIRED_FIELDS = ('date', 'description', 'amount', 'quantity', 'goldType')
    _GOLD_ALLOWED_TYPES = ('18', '22', '24')

    def insertDeposit(self, data, userId):
        # ak-bgc fix: validate up-front instead of letting a missing
        # field raise as a generic KeyError that the catch-all below
        # would mask as "Failed while inserting Gold Transaction" (no
        # hint to the agent or user what went wrong). Match the EPF /
        # MSN pattern: ValueError with a verbatim list of missing fields.
        missing = [k for k in self._GOLD_REQUIRED_FIELDS if not data.get(k)]
        if missing:
            raise ValueError(
                f"Gold insert requires {list(self._GOLD_REQUIRED_FIELDS)}; "
                f"missing: {missing}. Got fields: {sorted(data.keys())}."
            )

        # goldType must match the IBJA rate-file keys exactly.
        gold_type = str(data['goldType']).strip()
        if gold_type not in self._GOLD_ALLOWED_TYPES:
            raise ValueError(
                f"Gold insert: goldType must be one of "
                f"{list(self._GOLD_ALLOWED_TYPES)} (18/22/24 carat); "
                f"got {gold_type!r}. The downstream rate lookup keys on "
                f"this exact string — any other value silently produces "
                f"zero-profit rows."
            )

        # Quantity must be positive — zero grams of gold is nonsense and
        # would skew portfolio totals.
        try:
            quantity = float(data['quantity'])
            amount = float(data['amount'])
        except (TypeError, ValueError) as exc:
            raise ValueError(
                f"Gold insert: quantity and amount must be numeric; "
                f"got quantity={data['quantity']!r}, amount={data['amount']!r} ({exc})"
            ) from exc
        if quantity <= 0:
            raise ValueError(
                f"Gold insert: quantity (grams) must be > 0; got {quantity}."
            )
        if amount <= 0:
            raise ValueError(
                f"Gold insert: amount must be > 0; got {amount}."
            )

        try:
            buyId = self.genericUtil.generate_custom_buyID()
            deposit_security = DepositSecurities(
                buyID=buyId,
                date=self.dateTimeUtil.convert_to_sql_datetime(data['date'], DateStatementEnum.EPF_STATEMENT.name),
                depositDescription=data['description'],
                depositAmount=amount,
                userID=userId,
                securityType=EPGEnum.Gold.name
            )
            insertionObject = self.insertDepositFinal(deposit_security)
            self.insertTransactionType(buyId, quantity, gold_type)
            self.logger.info("Inserted gold type details to gold details")
            if 'error' in insertionObject:
                return jsonify({"Error": "Error in Gold entry"}), 406
            return jsonify({"Message": "Gold Transaction inserted successfully"}), 200
        except ValueError:
            # ak-bgc fix: don't swallow ValueError. Re-raise so the
            # caller (agent tool path) surfaces the message verbatim.
            raise
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
