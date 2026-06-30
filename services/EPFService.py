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
        # ak-bgc fix: strict required-field check up-front. Description and
        # date are non-negotiable; employee+employer is the only valid pair.
        # Bare-amount silent 50/50 split was the original bug (Overseer
        # transcript hq-wisp-qmtj3): the agent would call with `amount`
        # only, fallback split would render employer == employee on the
        # dashboard, user never knew. Now we raise — InvestmentService's
        # caller surfaces this as a clear tool error to the agent.
        if 'date' not in data or not data['date']:
            raise ValueError(
                "EPF insert requires 'date' (MM/YYYY or DD/MM/YYYY)"
            )
        if 'description' not in data or not data['description']:
            raise ValueError(
                "EPF insert requires 'description' (e.g. 'Contribution for MM/YYYY')"
            )

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

        # Extract employee and employer amounts. Two acceptable shapes:
        #   - parser format (PDF passbook): {employee_deposit, employer_deposit}
        #   - agent format (AI chat): {employee_amount, employer_amount}
        # The legacy bare-amount → 50/50 fallback was REMOVED — it was the
        # silent bug that motivated this audit (ak-bgc / hq-wisp-qmtj3).
        # Partial input (one of two amounts) is also rejected since it can't
        # represent the user's intent unambiguously.
        if 'employee_deposit' in data and 'employer_deposit' in data:
            employee_amount = data['employee_deposit']
            employer_amount = data['employer_deposit']
        elif 'employee_amount' in data and 'employer_amount' in data:
            employee_amount = data['employee_amount']
            employer_amount = data['employer_amount']
        else:
            # Surface the missing fields explicitly so the agent (and the
            # caller) can act on the message verbatim. Lists both accepted
            # name conventions so the agent can fix its tool call on retry.
            got = sorted(k for k in data.keys() if k not in {'date', 'description'})
            raise ValueError(
                "EPF insert requires both employee_amount and "
                "employer_amount (or employee_deposit / employer_deposit). "
                f"Got fields: {got}. Do NOT call with a single 'amount' — "
                "split was previously assumed 50/50 silently and corrupted "
                "data; provide both halves explicitly."
            )

        # Defensive: both amounts must be non-negative numbers. Coercion
        # to float surfaces a clear TypeError on malformed input rather
        # than letting it propagate as a DB cast failure later.
        try:
            employee_amount = float(employee_amount)
            employer_amount = float(employer_amount)
        except (TypeError, ValueError) as exc:
            raise ValueError(
                f"EPF insert: employee_amount and employer_amount must be "
                f"numeric; got employee={data.get('employee_amount') or data.get('employee_deposit')!r}, "
                f"employer={data.get('employer_amount') or data.get('employer_deposit')!r} ({exc})"
            ) from exc
        if employee_amount < 0 or employer_amount < 0:
            raise ValueError(
                f"EPF insert: employee_amount and employer_amount must be "
                f"non-negative; got employee={employee_amount}, "
                f"employer={employer_amount}"
            )

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
                    # v5 (hq-wisp-a0byf, bead ak-4tu): defensive wrap so
                    # missing EPF rate file degrades to zero-interest rows
                    # instead of bombing the whole dashboard with a 500.
                    # The rate fetcher has historically been the most
                    # fragile piece in this stack.
                    try:
                        rate = self.JsonDownloadService.getRateForMonth(dateString, EPGEnum.EPF.value)
                    except FileNotFoundError:
                        self.logger.warning(
                            f"EPF rate file missing for {dateString}; "
                            "treating rate as 0 — fetcher likely broken"
                        )
                        rate = 0
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
