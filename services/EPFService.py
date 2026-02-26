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
        # Handle different date formats from parser
        date_str = data['date']
        if '/' in date_str and len(date_str.split('/')) == 3:
            # DD/MM/YYYY format (interest entries)
            date_obj = self.dateTimeUtil.convert_to_sql_datetime(date_str, DateStatementEnum.EPF_STATEMENT.name)
        else:
            # MM/YYYY format (contribution entries) - convert to 01/MM/YYYY
            month, year = date_str.split('/')
            date_str = f"01/{month}/{year}"
            date_obj = self.dateTimeUtil.convert_to_sql_datetime(date_str, DateStatementEnum.EPF_STATEMENT.name)
        
        # Extract employee and employer amounts from different data formats
        if 'employee_deposit' in data and 'employer_deposit' in data:
            # Parser data format (from EPF passbook PDF)
            employee_amount = data['employee_deposit']
            employer_amount = data['employer_deposit']
        elif 'employee_amount' in data and 'employer_amount' in data:
            # Agent data format (from AI chat)
            employee_amount = data['employee_amount']
            employer_amount = data['employer_amount']
        else:
            # Legacy fallback: single 'amount' → split 50/50
            employee_amount = data['amount'] / 2
            employer_amount = data['amount'] / 2

        deposit_security = DepositSecurities(
            buyID=self.genericUtil.generate_custom_buyID(),
            date=date_obj,
            depositDescription=data['description'],
            depositAmount=int(employee_amount),
            employerAmount=int(employer_amount),
            userID=userId,
            securityType=EPGEnum.EPF.value
        )
        status = self.insertDepositFinal(deposit_security)
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
            if status[1] == 200:
                rowsInserted += 1
        self.logger.info("Finished processing file and inserting statements")
        return {"readFromStatement": {'buy': len(epfTransactions), 'sold': 0},
                "inserted": {'buy': rowsInserted, 'sold': 0}}

    def calculateTransactionTable(self, deposits: [DepositSecurities]):
        # Get the deposits here in sorted order
        self.logger.info("Starting EPF calculations")
        transactions = []
        try:
            running = 0  # Running total (contributions + compounded interest)
            runningInterest = 0  # Accumulated interest for current financial year
            totalContributions = 0  # Total contributions made (for profit calculation)
            
            for index, deposit in enumerate(deposits):
                dateString = self.dateTimeUtil.convert_format_for_epf(deposit.date.__str__())
                date = dateString
                description = deposit.depositDescription
                employee = float(deposit.depositAmount)
                employer = float(deposit.employerAmount) if deposit.employerAmount is not None else float(deposit.depositAmount)
                amount = employee + employer
                totalContributions += amount

                # Calculate interest for this month (except for first deposit)
                interest = 0
                if index != 0:
                    rate = self.JsonDownloadService.getRateForMonth(dateString, EPGEnum.EPF.value)
                    interest = running * (rate / 1200)
                    runningInterest += interest
                
                # Add contribution to running total
                running += amount
                
                # At end of financial year, add accumulated interest to running total
                if dateString.endswith("03", len(dateString) - 2, len(dateString)):
                    running += runningInterest
                    runningInterest = 0
                
                transactions.append({
                    'date': date,
                    'description': description,
                    'amount': amount,
                    'interest': interest,
                })
            
            # Generate transactions for months after last deposit
            if len(deposits) > 0:
                lastEmployee = float(deposits[-1].depositAmount)
                lastEmployer = float(deposits[-1].employerAmount) if deposits[-1].employerAmount is not None else float(deposits[-1].depositAmount)
                lastDeposit = lastEmployee + lastEmployer
                lastDescription = deposits[-1].depositDescription
                lastMonth = transactions[-1]['date']
                for month in self.dateTimeUtil.iterate_months(deposits[-1].date.__str__()):
                    if month != lastMonth:
                        date = month
                        description = lastDescription
                        amount = lastDeposit
                        totalContributions += amount

                        rate = self.JsonDownloadService.getRateForMonth(date, EPGEnum.EPF.value)
                        interest = running * (rate / 1200)
                        runningInterest += interest
                        
                        running += amount
                        
                        if date.endswith("03", len(date) - 2, len(date)):
                            running += runningInterest
                            runningInterest = 0
                            
                        transactions.append({
                            'date': date,
                            'description': f"GENERATED ROW {description}",
                            'amount': amount,
                            'interest': interest,
                        })
            
            # Calculate final values
            finalBalance = running + runningInterest  # Current total value
            netProfit = finalBalance - totalContributions  # Interest earned
            
            return transactions, netProfit, finalBalance, runningInterest
        except Exception as ex:
            self.logger.error(f"Error while calculating transaction table for EPF {ex}")
            return None, None, None, None

    def fetchComplete(self, userId):
        # Fetch all the deposits
        deposits = self.get_securities(userId, EPGEnum.EPF.value)
        depositDict = []
        for deposit in deposits:
            employee = float(deposit.depositAmount)
            employer = float(deposit.employerAmount) if deposit.employerAmount is not None else float(deposit.depositAmount)
            depositDict.append({
                'buyId': deposit.buyID,
                "date": deposit.date,
                "description": deposit.depositDescription,
                "amount": employee + employer,
                "employeeAmount": employee,
                "employerAmount": employer,
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
