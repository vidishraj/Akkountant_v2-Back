import datetime
from abc import ABC
from decimal import Decimal

from flask import jsonify

from enums.DateFormatEnum import DateStatementEnum
from enums.EPGEnum import EPGEnum
from models import DepositSecurities
from services.BaseEPG import Base_EPG


class PPFService(Base_EPG, ABC):

    def __init__(self):
        super().__init__()

    def _get_fy_total(self, userId, deposit_date):
        """Get total PPF deposits in the financial year containing deposit_date."""
        if deposit_date.month >= 4:
            fy_start = datetime.date(deposit_date.year, 4, 1)
            fy_end = datetime.date(deposit_date.year + 1, 3, 31)
        else:
            fy_start = datetime.date(deposit_date.year - 1, 4, 1)
            fy_end = datetime.date(deposit_date.year, 3, 31)

        deposits = self.get_securities(userId, EPGEnum.PF.value)
        fy_total = sum(
            float(d.depositAmount) for d in deposits
            if fy_start <= d.date <= fy_end
        )
        return fy_total, fy_start.year, fy_end.year

    def insertDeposit(self, data, userId):
        parsed_date = self.dateTimeUtil.convert_to_sql_datetime(data['date'], DateStatementEnum.EPF_STATEMENT.name)
        amount = float(data['amount'])

        # Validate PPF annual limit of ₹1,50,000
        fy_total, fy_start_year, fy_end_year = self._get_fy_total(userId, parsed_date)
        if fy_total + amount > 150000:
            return jsonify({
                "Error": f"PPF annual limit exceeded. FY {fy_start_year}-{fy_end_year} deposits: ₹{fy_total:,.2f}. "
                         f"Adding ₹{amount:,.2f} would total ₹{fy_total + amount:,.2f} (limit: ₹1,50,000)."
            }), 406

        deposit_security = DepositSecurities(
            buyID=self.genericUtil.generate_custom_buyID(),
            date=parsed_date,
            depositDescription=data['description'],
            depositAmount=amount,
            userID=userId,
            securityType=EPGEnum.PF.value
        )
        status = self.insertDepositFinal(deposit_security)
        if 'error' in status:
            return jsonify({"Message": "Duplicate PF entry"}), 406
        return jsonify({"Message": "PPF Transaction inserted successfully"}), 200

    def calculateTransactionTable(self, deposits: [DepositSecurities]):  # Get the deposits here in sorted order
        """
        PPF interest calculation per PPF Scheme 2019, Paragraph 7:
        - Interest is calculated monthly on the lowest balance between the close of the 5th
          and the end of that month. Since we only track deposits (no withdrawals), the minimum
          balance equals the balance at close of the 5th.
        - Deposits on days 1-5: count in current month (included at close of 5th).
        - Deposits on days 6-31: count from next month onwards.
        - Interest is accumulated monthly but credited annually on March 31.
        - Annual interest is rounded to nearest rupee (50 paise rounds up) per the scheme.
        :param deposits: Row of DepositSecurities sorted by date
        :return: (transactions, netProfit, runningTotal, runningInterest)
        """
        depositMap = {}
        if len(deposits) == 0:
            return [], 0, 0, 0
        try:
            for deposit in deposits:
                dateString = self.dateTimeUtil.convert_format_for_epf(deposit.date.__str__())
                if depositMap.get(dateString) is None:
                    depositMap[dateString] = []
                depositMap[dateString].append(deposit)
            runningTotal = 0
            nextMonth = 0
            transactions = []
            netProfit = 0
            runningInterest = 0
            for month in self.dateTimeUtil.iterate_months(deposits[0].date.__str__()):
                runningTotal += nextMonth
                nextMonth = 0
                if depositMap.get(month) is not None:
                    currentMonth = 0
                    for deposit in depositMap[month]:
                        date = datetime.datetime.strptime(deposit.date.__str__(), '%Y-%m-%d')
                        day = date.day
                        if day > 5:
                            nextMonth += deposit.depositAmount
                        else:
                            currentMonth += deposit.depositAmount
                    runningTotal += currentMonth
                rate = self.JsonDownloadService.getRateForMonth(month, EPGEnum.PF.value)
                interest = runningTotal * (Decimal(str(rate)) / Decimal('1200'))
                netProfit += interest
                runningInterest += interest
                if month.endswith("03", len(month) - 2, len(month)):
                    # Round annual interest to nearest rupee (50 paise rounds up) per PPF Scheme 2019
                    rounded = round(runningInterest)
                    netProfit += (rounded - runningInterest)  # Adjust total profit for rounding
                    nextMonth = rounded  # Credit rounded interest to principal for compounding
                    runningInterest = 0
                transactions.append({
                    'date': month,
                    'amount': runningTotal,
                    'interest': interest
                })
            return transactions, netProfit, runningTotal, runningInterest
        except Exception as ex:
            self.logger.error(f"Error while calculating transaction table for PF {ex}")
            return [], 0, 0, 0

    def fetchComplete(self, userId):
        # Fetch all the deposits
        deposits = self.get_securities(userId, EPGEnum.PF.value)
        depositDict = []
        for deposit in deposits:
            depositDict.append({
                'buyId': deposit.buyID,
                "date": deposit.date,
                "description": deposit.depositDescription,
                "amount": deposit.depositAmount,
            })
        transactions, netProfit, netInvestment, unaccountProfit = self.calculateTransactionTable(deposits)
        return {
            'transactions': transactions,
            'deposits': depositDict,
            'netProfit': netProfit,
            'unAccountedProfit': unaccountProfit,
            'net': netInvestment,
        }

    def fetchRates(self):
        return {"data": self.JsonDownloadService.getPPFRateFile()}
