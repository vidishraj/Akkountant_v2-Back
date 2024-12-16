import datetime
from abc import ABC

from flask import jsonify

from enums.DateFormatEnum import DateStatementEnum
from enums.EPGEnum import EPGEnum
from models import DepositSecurities
from services.BaseEPG import Base_EPG
from services.parsers.EPF_Statement import EPFStatementParser


class EPFService(Base_EPG, ABC):

    def __init__(self):
        super().__init__()
        self.parser = EPFStatementParser()

    def insertDeposit(self, data, userId):
        deposit_security = DepositSecurities(
            buyID=self.genericUtil.generate_custom_buyID(),
            date=self.dateTimeUtil.convert_to_sql_datetime(data['date'], DateStatementEnum.EPF_STATEMENT.name),
            depositDescription=data['description'],
            depositAmount=data['amount'],
            userID=userId,
            securityType=EPGEnum.EPF.value
        )
        status= self.insertDepositFinal(deposit_security)
        if 'error' in status:
            return jsonify({"Error": "Error in EPF entry"}), 406
        return jsonify({"Message": "EPF Transaction inserted successfully"}), 200

    def readFromStatement(self, file_path: str, userId):
        """
        We will be reading the EPF statement here
        :return:
        """
        self.parser.setPath(file_path)
        epfTransactions = self.parser.parseFile()
        rowsInserted = 0
        for transaction in epfTransactions:
            status = self.insertDeposit(transaction, userId)
            if status.get('error') is None:
                rowsInserted += 1
        self.logger.info("Finished processing file and inserting statements")
        return {"readFromStatement": {'buy': len(epfTransactions), 'sold': 0},
                "inserted": {'buy': rowsInserted, 'sold': 0}}

    def calculateTransactionTable(self, deposits: [DepositSecurities]):
        # Get the deposits here in sorted order
        self.logger.info("Starting EPF calculations")
        transactions = []
        try:
            # no interest for the year
            running = 0
            runningInterest = 0
            interest = 0
            netProfit = 0
            for index, deposit in enumerate(deposits):
                dateString = self.dateTimeUtil.convert_format_for_epf(deposit.date.__str__())
                date = dateString
                description = deposit.depositDescription
                amount = deposit.depositAmount

                if index != 0:
                    rate = self.JsonDownloadService.getRateForMonth(dateString, EPGEnum.EPF.value)
                    interest = running * (rate / 1200)
                    runningInterest += interest
                netProfit += interest
                if dateString.endswith("03", len(dateString) - 2, len(dateString)):
                    # END OF FINANCIAL YEAR
                    # Add interest here, set local variables to zero
                    running += runningInterest
                    runningInterest = 0
                running += amount
                transactions.append({
                    'date': date,
                    'description': description,
                    'amount': amount,
                    'interest': interest,
                })
            return transactions, netProfit, running, runningInterest
        except Exception as ex:
            self.logger.error(f"Error while calculating transaction table for EPF {ex}")
            return None, None, None

    def fetchComplete(self, userId):
        # Fetch all the deposits
        deposits = self.get_securities(userId, EPGEnum.EPF.value)
        depositDict = []
        for deposit in deposits:
            depositDict.append({
                'buyId': deposit.buyID,
                "date": deposit.date,
                "description": deposit.depositDescription,
                "amount": deposit.depositAmount,
            })
        # calculate the profits based on the deposits
        transaction, netProfit, netInvestment, unaccountedProfit = self.calculateTransactionTable(deposits)
        if transaction is not None:
            # Create json response
            return {
                'transactions': transaction,
                'netProfit': self.genericUtil.convertToDecimal(netProfit),
                'net': self.genericUtil.convertToDecimal(netInvestment),
                'deposits': depositDict,
                'unAccountedProfit': self.genericUtil.convertToDecimal(unaccountedProfit)
            }
        else:
            return {}

    def fetchRates(self):
        return {"data": self.JsonDownloadService.getEPFRateFile()}
