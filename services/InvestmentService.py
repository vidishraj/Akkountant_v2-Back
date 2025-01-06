import datetime
import os
from dotenv import load_dotenv
from flask import jsonify
from flask_sqlalchemy import SQLAlchemy
from marshmallow import ValidationError
from sqlalchemy.exc import SQLAlchemyError

from dtos.MSNSummaryDto import MSNSummary
from enums.DateFormatEnum import DateStatementEnum
from enums.EPGEnum import EPGEnum
from enums.MsnEnum import MSNENUM
from models import PurchasedSecurities, Jobs
from services.Base_Service import BaseService
from services.EPFService import EPFService
from services.GoldService import GoldService
from services.MfService import MfService
from services.NpsService import NPSService
from services.PPFService import PPFService
from services.StocksService import StocksService
from utils.DateTimeUtil import DateTimeUtil
from utils.GenericUtils import GenericUtil
from utils.logger import Logger
from decimal import Decimal, ROUND_DOWN
from logging import Logger as LG

load_dotenv()
if os.getenv('ENV') == "PROD":
    import nsepythonserver as nsepython
else:
    import nsepython


class InvestmentService(BaseService):
    db: SQLAlchemy
    logger: LG
    jobsObject = {
        "SetNPSRate": "Set NPS Rate",
        "SetNPSDetails": "Set NPS Details",
        "SetStocksOldDetails": "Set Stocks Old Codes",
        "SetStocksDetails": "Set Stocks Details",
        "SetMFRate": "Set Mutual Funds Rate",
        "SetMFDetails": "Set Mutual Funds Details",
        "SetGoldRate": "Set Gold Rates",
        "SetPPFRate": "Set PPF Rates",
        "CheckMail": "Check Mail",
        "CheckStatement": "Check Statements"
    }

    def __init__(self):
        super().__init__()
        self.logger = Logger(__name__).get_logger()
        self.StockService = StocksService()
        self.NPSService = NPSService()
        self.MFService = MfService()
        self.EPFService = EPFService()
        self.PPFService = PPFService()
        self.GoldService = GoldService()
        self.genericUtil = GenericUtil()

    def fetchAllSecurities(self, securityType: MSNENUM):
        if securityType == MSNENUM.Stocks:
            return self.StockService.fetchAllSecurities()
        elif securityType == MSNENUM.Mutual_Funds:
            return self.MFService.fetchAllSecurities()
        elif securityType == MSNENUM.NPS:
            return self.NPSService.fetchAllSecurities()
        else:
            self.logger.error("UNKNOWN MSN. API CALLS BEING MADE POSSIBLY")
            return {}

    def fetchSecuritySchemeRate(self, securityType: MSNENUM, schemeCode: str):
        if securityType == MSNENUM.Stocks.value:
            response = self.StockService.findSecurity(schemeCode)
            return self.genericUtil.fetchStockRates(response)
        elif securityType == MSNENUM.Mutual_Funds.value:
            return self.MFService.findSecurity(schemeCode)
        elif securityType == MSNENUM.NPS.value:
            item = self.NPSService.findSecurity(schemeCode)
            itemDetails = self.NPSService.JsonDownloadService.getNPSListDetailsForScheme(schemeCode)
            change = item['sixMonthsAgo'] - item['nav']
            pChange = (change / float(item['sixMonthsAgo'])) * 100
            return {
                'lastPrice': item['nav'],
                'date': item['date'],
                'id': item['scheme_id'],
                "yesterday": item['yesterday'],
                "lastWeek": item['lastWeek'],
                "sixMonthsAgo": item['sixMonthsAgo'],
                'pfmName': itemDetails['pfm_name'],
                'change': Decimal(change).quantize(Decimal('0.01'), rounding=ROUND_DOWN),
                'pChange': Decimal(pChange).quantize(Decimal('0.01'), rounding=ROUND_DOWN)
            }

    def processFiles(self, serviceType, file_path, userId):
        # Route the file processing based on the service type
        if serviceType == MSNENUM.Stocks:
            return self.StockService.readFromStatement(file_path, userId)
        elif serviceType == MSNENUM.NPS:
            return self.NPSService.readFromStatement(file_path, userId)
        elif serviceType == EPGEnum.EPF:
            return self.EPFService.readFromStatement(file_path, userId)

    def fetchActiveSecurities(self, securityType, userId):
        if securityType == MSNENUM.Stocks:
            return self.StockService.fetchActive(securityType, userId)
        elif securityType == MSNENUM.Mutual_Funds:
            return self.MFService.fetchActive(securityType, userId)
        elif securityType == MSNENUM.NPS:
            return self.NPSService.fetchActive(securityType, userId)
        elif securityType == EPGEnum.PF:
            return self.PPFService.fetchComplete(userId)
        elif securityType == EPGEnum.EPF:
            return self.EPFService.fetchComplete(userId)
        elif securityType == EPGEnum.Gold:
            return self.GoldService.fetchComplete(userId)

    def fetchHistory(self, securityType, userId):
        if securityType == MSNENUM.Stocks:
            return self.StockService.getInvestmentHistory(securityType, userId)
        elif securityType == MSNENUM.Mutual_Funds:
            return self.MFService.getInvestmentHistory(securityType, userId)
        elif securityType == MSNENUM.NPS:
            return self.NPSService.getInvestmentHistory(securityType, userId)

    def fetchSummary(self, securityType, userId):
        """
        TotalValue, currentValue, Change %, Change Amount, Count, Market Status
        :param securityType: Can be stocks, mf or nps
        :param userId: userId to query for
        :return: Summary dto
        """

        activeInvested = self.StockService.getActiveMoneyInvested(securityType, userId)
        activeProfitAll = self.StockService.calculateProfitAndCurrentValue(securityType, userId)
        totalProfit = 0
        try:
            marketStatus = nsepython.nse_marketStatus()
        except:
            marketStatus = None
        if marketStatus is not None:
            marketStatus = marketStatus['marketState'][0]['marketStatus']
            if marketStatus == 'Closed':
                marketStatus = False
            else:
                marketStatus = True
        else:
            marketStatus = False
        # Instantiate the schema
        msn_summary_schema = MSNSummary()
        if activeInvested != 0:
            for item in activeProfitAll:
                totalProfit += activeProfitAll[item]['profit']
            securityCount = self.StockService.getSecurityCount(userId, MSNENUM[securityType].value)

            changePercent = (((totalProfit + activeInvested) - activeInvested) / activeInvested) * 100
            data = {
                "totalValue": Decimal(activeInvested).quantize(Decimal('0.01'), rounding=ROUND_DOWN),
                "currentValue": Decimal(activeInvested + totalProfit).quantize(Decimal('0.01'), rounding=ROUND_DOWN),
                "changePercent": Decimal(changePercent).quantize(Decimal('0.01'), rounding=ROUND_DOWN),
                "changeAmount": Decimal(totalProfit).quantize(Decimal('0.01'), rounding=ROUND_DOWN),
                "count": securityCount,
                "marketStatus": marketStatus,
            }

        else:
            data = {
                "totalValue": 0,
                "currentValue": 0,
                "changePercent": 0,
                "changeAmount": 0,
                "count": 0,
                "marketStatus": marketStatus,
            }
        try:
            result = msn_summary_schema.load(data)
            return result
        except ValidationError as err:
            self.logger.error(f"validation error {err}")
            return {'error': 'Validation Error'}

    def fetchSecurityTransactions(self, securityType, userId):
        if securityType == MSNENUM.Stocks.value:
            return self.StockService.fetchTransactionsForUserAndService(securityType, userId)
        elif securityType == MSNENUM.Mutual_Funds.value:
            return self.MFService.fetchTransactionsForUserAndService(securityType, userId)
        elif securityType == MSNENUM.NPS.value:
            return self.NPSService.fetchTransactionsForUserAndService(securityType, userId)

    def fetchUserSecurities(self, securityType, userId):

        """
               TotalValue, currentValue, Change %, Change Amount, Count, Market Status
               :param securityType: Can be stocks, mf or nps
               :param userId: userId to query for
               :return: Summary dto
        """
        activeSecurities: [PurchasedSecurities] = self.StockService.fetchActive(securityType, userId)
        if activeSecurities is None:
            return {"error": "No active securities"}

        for security in activeSecurities:
            if securityType == MSNENUM.Stocks.value:
                rates = self.StockService.calculateStockRates(activeSecurities)
                security['info'] = rates[security['buyCode'] if security['buyCode'] != "SUZLON-BE" else "SUZLON"]
            elif securityType == MSNENUM.NPS.value:
                security['info'] = self.NPSService.findSecurity(security['buyCode'])
            elif securityType == MSNENUM.Mutual_Funds.value:
                infoDetails = self.MFService.findSecurity(security['buyCode'])
                change = float(infoDetails['nav']) - float(infoDetails['lastNav'])
                changeP = (change / float(infoDetails['lastNav'])) * 100
                security['info'] = {'lastPrice': self.genericUtil.convertToDecimal(infoDetails['nav']),
                                    'previousClose': self.genericUtil.convertToDecimal(infoDetails['lastNav']),
                                    'pChange': self.genericUtil.convertToDecimal(changeP),
                                    'change': self.genericUtil.convertToDecimal(change),
                                    'fundHouse': infoDetails['fundHouse'],
                                    'scheme_id': infoDetails['scheme_id'],
                                    'schemeType': infoDetails['schemeType']}
        return activeSecurities

    def insertSecurityPurchase(self, serviceType, userId, data):
        # Purchase from the UI is only possible for MF, EPF, PF or Gold
        if serviceType == MSNENUM.Mutual_Funds:
            insertionObject = {
                "securityCode": data['schemeCode'],
                "date": DateTimeUtil().convert_to_sql_datetime(data['date'], DateStatementEnum.EPF_STATEMENT.name),
                "buyQuant": data['quantity'],
                "buyPrice": data['amount'],
            }
            status = self.MFService.buySecurity(insertionObject, userId)
            if 'error' in status:
                return jsonify({"Error": "Error in MF entry"}), 406
            return jsonify({"Message": "MF Transaction inserted successfully"}), 200
        elif serviceType == EPGEnum.EPF:
            return self.EPFService.insertDeposit(data, userId)
        elif serviceType == EPGEnum.PF:
            return self.PPFService.insertDeposit(data, userId)
        elif serviceType == EPGEnum.Gold:
            return self.GoldService.insertDeposit(data, userId)

    def fetchRateForEPG(self, serviceType):
        if serviceType == EPGEnum.EPF:
            return self.EPFService.fetchRates()
        elif serviceType == EPGEnum.PF:
            return self.PPFService.fetchRates()
        elif serviceType == EPGEnum.Gold:
            return self.GoldService.fetchRates()

    def deleteAll(self, serviceType, userId):
        if serviceType == EPGEnum.EPF:
            return self.EPFService.delete_deposit_securities_by_user(userId)
        elif serviceType == EPGEnum.PF:
            return self.PPFService.delete_deposit_securities_by_user(userId)
        elif serviceType == EPGEnum.Gold:
            return self.GoldService.delete_deposit_securities_by_user(userId)
        elif serviceType == MSNENUM.Stocks:
            return self.StockService.delete_purchased_securities_by_user(userId)
        elif serviceType == MSNENUM.NPS:
            return self.NPSService.delete_purchased_securities_by_user(userId)
        elif serviceType == MSNENUM.Mutual_Funds:
            return self.MFService.delete_purchased_securities_by_user(userId)

    def deleteSingleRecord(self, serviceType, buyId):
        purchaseRecordDeletion = {'model': 'PurchasedSecurities', "filters": f'PurchasedSecurities.buyId=={buyId}'}
        sellRecordDeletion = {'model': 'SoldSecurities', "filters": f'SoldSecurities.buyID=={buyId}'}
        depositRecordDeletion = {'model': 'DepositSecurities', "filters": f'DepositSecurities.buyID=={buyId}'}
        # transactionTableDeletion = {'model': 'SecurityTransactions', "filters": f'PurchasedSecurities.userId=={
        # userId}'}
        # trade_associationDeletion = {'model': 'TradeAssociation', "filters": f'TradeAssociation.buyId=={buyId}'}
        # goldDetailsDeletion = {'model': 'PurchasedSecurities', "filters": f'PurchasedSecurities.userId=={userId}'}
        if serviceType == EPGEnum.EPF or serviceType == EPGEnum.PF or serviceType == EPGEnum.Gold:
            if self.delete_records([depositRecordDeletion]):
                return jsonify({"Message": "Successfully deleted"}), 200
        elif serviceType == MSNENUM.Mutual_Funds or serviceType == MSNENUM.Stocks or serviceType == MSNENUM.NPS:
            if self.delete_records([sellRecordDeletion, purchaseRecordDeletion]):
                return jsonify({"Message": "Successfully deleted"}), 200
            return jsonify({"Error": "Failed to delete record"}), 501

    def delete_records(self, delete_operations: list):
        """
        Deletes records from multiple models in a transactional manner.
        Args: delete_operations (list): A list of operations to execute. Each operation
                                       should be a dictionary with 'model' and 'filters'.
                                       Example: [
                                           {"model": Model1, "filters": Model1.id == 1},
                                           {"model": Model2, "filters": Model2.name == "example"}]
        Returns:
            bool: True if all deletions succeed, False if any deletion fails.
        """
        try:
            for operation in delete_operations:
                model = operation["model"]
                filters = operation["filters"]

                # Perform the deletion
                self.db.session.query(model).filter(filters).delete()

            # Commit all deletions
            self.db.session.commit()
            return True
        except SQLAlchemyError as ex:
            # Rollback transaction on any failure
            self.db.session.rollback()
            self.logger.error(f"Deletion failed: {ex}")
            return False

    def getJobsTable(self, page, page_size=10, limit=10):
        column_attr = getattr(Jobs.Job, 'due_date')
        paginationQuery = self.db.session.query(Jobs.Job).order_by(column_attr.desc()).\
            offset((int(page) - 1) * page_size).limit(limit)
        results = paginationQuery.all()
        res = [{
                "Title": result.title,
                "Result": result.result,
                "Status": result.status,
                "DueTime": result.due_date,
                "Failures": result.failures
            } for result in results]
        return {
            "results": res,
            "page": page,
            "jobs": self.jobsObject
        }

    def setJobsTable(self, jobId: str, user_id: str):
        if jobId not in list(self.jobsObject.keys()):
            return jsonify({"Error": "Invalid Job"}), 406
        newJob = Jobs.Job(
                title=jobId,
                status="Pending",
                priority="High",
                due_date=datetime.datetime.now(),
                user_id=user_id,
                result=None,
        )
        self.db.session.add(newJob)
        self.db.session.commit()
        return jsonify({"Success": "Job inserted"}), 200

    def getFileTimeStamps(self):
        return self.StockService.JsonDownloadService.getTimeStampsOfAllFiles()