import datetime
from abc import ABC

from dateutil.relativedelta import relativedelta
from flask import jsonify

from enums.DateFormatEnum import DateStatementEnum
from enums.EPGEnum import EPGEnum
from models import DepositSecurities
from services.BaseEPG import Base_EPG


class PPFService(Base_EPG, ABC):

    def __init__(self):
        super().__init__()

    def insertDeposit(self, data, userId):
        deposit_security = DepositSecurities(
            buyID=self.genericUtil.generate_custom_buyID(),
            date=self.dateTimeUtil.convert_to_sql_datetime(data['date'], DateStatementEnum.EPF_STATEMENT.name),
            depositDescription=data['description'],
            depositAmount=data['amount'],
            userID=userId,
            securityType=EPGEnum.PF.value
        )
        status = self.insertDepositFinal(deposit_security)
        if 'error' in status:
            return jsonify({"Message": "Duplicate PF entry"}), 406
        return jsonify({"Message": "PPF Transaction inserted successfully"}), 406

    def calculateTransactionTable(self, deposits: [DepositSecurities]):  # Get the deposits here in sorted order
        """
        For each month from the month of first deposit, the lowest balance between 5th of that month
        and the end of that month is used to calculate the interest for that month.
        (Any deposit made between the 5th-4th of the next month will be used for the next month's calculation)

        The interest is credited at the end of the financial year but computed monthly.
        :param deposits: Row of DepositSecurities
        :return:
        """
        depositMap = {}
        if len(deposits) == 0:
            return None, None, None
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
            for month in self.iterate_months(deposits[0].date.__str__()):
                runningTotal += nextMonth
                nextMonth = 0
                if depositMap.get(month) is not None:
                    currentMonth = 0
                    for deposit in depositMap[month]:
                        date = datetime.datetime.strptime(deposit.date.__str__(), '%Y-%m-%d')
                        day = date.day
                        if 4 < day <= 31:
                            nextMonth += deposit.depositAmount
                        else:
                            currentMonth += deposit.depositAmount
                    runningTotal += currentMonth
                rate = self.JsonDownloadService.getRateForMonth(month, EPGEnum.PF.value)
                interest = runningTotal * (rate / 1200)
                netProfit += interest
                runningInterest += interest
                if month.endswith("03", len(month) - 2, len(month)):
                    nextMonth = runningInterest  # Adding interest for the year to compound
                    runningInterest = 0
                transactions.append({
                    'month': month,
                    'depositTotal': runningTotal,
                    'interest': interest
                })
            return transactions, netProfit, runningTotal, runningInterest
        except Exception as ex:
            self.logger.error(f"Error while calculating transaction table for PF {ex}")
            return None, None

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
        # calculate the profits based on the deposits
        if transactions is not None:
            # Create json response
            return {
                'transactions': transactions,
                'deposits': depositDict,
                'netProfit': netProfit,
                'unAccountedProfit': unaccountProfit,
                'net': netInvestment,
            }
        else:
            return {}

    @staticmethod
    def iterate_months(start_date: str):
        """
        Iterates from a given start date to the current month.

        Args:
            start_date (str): The starting date in '%Y-%m-%d' format.

        Yields:
            str: The current month in iteration in '%Y-%m' format.
        """
        try:
            start = datetime.datetime.strptime(start_date, "%Y-%m-%d")
            current = datetime.datetime.now()

            while start <= current:
                yield start.strftime("%Y-%m")
                start += relativedelta(months=1)
        except ValueError as e:
            raise ValueError(f"Invalid date format: {e}")

    def fetchRates(self):
        return {"data": self.JsonDownloadService.getPPFRateFile()}
