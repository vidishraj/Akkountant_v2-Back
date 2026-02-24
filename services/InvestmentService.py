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
from models.investmentHistory import InvestmentHistory
from models.securities import SoldSecurities
from models.depositSecurities import DepositSecurities
from models.securityTransactions import SecurityTransactions
from models.stockTrade import TradeAssociation
from services.Base_Service import BaseService
from services.EPFService import EPFService
from services.GoldService import GoldService
from services.MfService import MfService
from services.NpsService import NPSService
from services.PPFService import PPFService
from services.FOService import FOService
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
        "SetStocksDetails": "Set Stocks Details (Kite)",
        "SetMFRate": "Set Mutual Funds Rate",
        "SetMFDetails": "Set Mutual Funds Details",
        "SetGoldRate": "Set Gold Rates",
        "SetPPFRate": "Set PPF Rates",
        "SetEPFRate": "Set EPF Rates",
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
        self.FOService = FOService()
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

            # Safely access optional historical fields with fallbacks
            nav = item.get('nav', 0)
            six_months_ago = item.get('sixMonthsAgo', item.get('nav', 0))

            # Calculate change only if we have valid historical data
            if six_months_ago > 0:
                change = six_months_ago - nav
                pChange = (change / float(six_months_ago)) * 100
            else:
                change = 0
                pChange = 0

            return {
                'lastPrice': nav,
                'date': item.get('date'),
                'id': item.get('scheme_id'),
                "yesterday": item.get('yesterday', nav),
                "lastWeek": item.get('lastWeek', nav),
                "sixMonthsAgo": six_months_ago,
                'pfmName': itemDetails.get('pfm_name', ''),
                'change': Decimal(change).quantize(Decimal('0.01'), rounding=ROUND_DOWN),
                'pChange': Decimal(pChange).quantize(Decimal('0.01'), rounding=ROUND_DOWN)
            }

    def processFiles(self, serviceType, file_path, userId):
        # Route the file processing based on the service type
        if serviceType == MSNENUM.Stocks:
            return self.StockService.readFromStatement(file_path, userId)
        elif serviceType == MSNENUM.NPS:
            return self.NPSService.readFromStatement(file_path, userId)
        elif serviceType == MSNENUM.FO:
            return self.FOService.readFromStatement(file_path, userId)
        elif serviceType == EPGEnum.EPF:
            return self.EPFService.readFromStatement(file_path, userId)
        else:
            self.logger.error(f"Unsupported service type for file processing: {serviceType}")
            return {"error": f"Unsupported service type: {serviceType}"}

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
        except Exception:
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
        if activeSecurities is None or len(activeSecurities) == 0:
            return {"error": "No active securities"}

        # Fetch all stock rates once outside the loop to avoid N*N API calls
        stockRates = None
        if securityType == MSNENUM.Stocks.value:
            stockRates = self.StockService.calculateStockRates(activeSecurities)

        for security in activeSecurities:
            if securityType == MSNENUM.Stocks.value:
                symbol = self.StockService.resolveSymbol(security['buyCode'])
                security['info'] = stockRates.get(symbol, {
                    'symbol': symbol, 'lastPrice': 0, 'change': 0,
                    'pChange': 0, 'previousClose': 0, 'error': 'NOT_FOUND'
                })
            elif securityType == MSNENUM.NPS.value:
                try:
                    npsInfo = self.NPSService.findSecurity(security['buyCode'])

                    # Check if we got valid data
                    if not npsInfo or 'nav' not in npsInfo:
                        self.logger.warning(f"No NPS rate data found for scheme {security['buyCode']}")
                        security['info'] = {
                            'nav': 0,
                            'scheme_id': security['buyCode'],
                            'date': None,
                            'yesterday': 0,
                            'lastWeek': 0,
                            'sixMonthsAgo': 0,
                            'pfm_name': 'Unknown',
                            'error': 'RATE_DATA_UNAVAILABLE'
                        }
                        continue

                    # Ensure optional fields have fallback values
                    nav = npsInfo.get('nav', 0)
                    security['info'] = {
                        'nav': nav,
                        'scheme_id': npsInfo.get('scheme_id', security['buyCode']),
                        'date': npsInfo.get('date'),
                        'yesterday': npsInfo.get('yesterday', nav),
                        'lastWeek': npsInfo.get('lastWeek', nav),
                        'sixMonthsAgo': npsInfo.get('sixMonthsAgo', nav),
                        'pfm_name': npsInfo.get('pfm_name', 'Unknown')
                    }
                except Exception as e:
                    self.logger.error(f"Error processing NPS data for scheme {security['buyCode']}: {str(e)}")
                    security['info'] = {
                        'nav': 0,
                        'scheme_id': security['buyCode'],
                        'date': None,
                        'yesterday': 0,
                        'lastWeek': 0,
                        'sixMonthsAgo': 0,
                        'pfm_name': 'Unknown',
                        'error': 'PROCESSING_ERROR'
                    }
            elif securityType == MSNENUM.Mutual_Funds.value:
                try:
                    infoDetails = self.MFService.findSecurity(security['buyCode'])
                    
                    # Check if we got valid data
                    if not infoDetails or 'nav' not in infoDetails:
                        self.logger.warning(f"No MF rate data found for scheme {security['buyCode']}")
                        security['info'] = {
                            'lastPrice': 0,
                            'previousClose': 0,
                            'pChange': 0,
                            'change': 0,
                            'fundHouse': 'Unknown',
                            'scheme_id': security['buyCode'],
                            'schemeType': 'Unknown',
                            'companyName': 'Unknown',
                            'error': 'RATE_DATA_UNAVAILABLE'
                        }
                        continue
                    
                    # Check for required fields
                    nav = infoDetails.get('nav', 0)
                    lastNav = infoDetails.get('lastNav', nav)  # Fallback to current nav if lastNav missing
                    
                    if lastNav == 0:
                        change = 0
                        changeP = 0
                    else:
                        change = float(nav) - float(lastNav)
                        changeP = (change / float(lastNav)) * 100
                    
                    security['info'] = {
                        'lastPrice': self.genericUtil.convertToDecimal(nav),
                        'previousClose': self.genericUtil.convertToDecimal(lastNav),
                        'pChange': self.genericUtil.convertToDecimal(changeP),
                        'change': self.genericUtil.convertToDecimal(change),
                        'fundHouse': infoDetails.get('fundHouse', 'Unknown'),
                        'scheme_id': infoDetails.get('scheme_id', security['buyCode']),
                        'schemeType': infoDetails.get('schemeType', 'Unknown'),
                        'companyName': infoDetails.get('companyName', 'Unknown')
                    }
                except Exception as e:
                    self.logger.error(f"Error processing MF data for scheme {security['buyCode']}: {str(e)}")
                    security['info'] = {
                        'lastPrice': 0,
                        'previousClose': 0,
                        'pChange': 0,
                        'change': 0,
                        'fundHouse': 'Unknown',
                        'scheme_id': security['buyCode'],
                        'schemeType': 'Unknown',
                        'companyName': 'Unknown',
                        'error': 'PROCESSING_ERROR'
                    }
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
        elif serviceType == MSNENUM.FO:
            return self.FOService.delete_all_fo_trades(userId)

    def deleteSingleRecord(self, serviceType, buyId):
        try:
            if serviceType == EPGEnum.EPF or serviceType == EPGEnum.PF or serviceType == EPGEnum.Gold:
                self.db.session.query(DepositSecurities).filter(
                    DepositSecurities.buyID == buyId
                ).delete()
            elif serviceType == MSNENUM.Mutual_Funds or serviceType == MSNENUM.Stocks or serviceType == MSNENUM.NPS:
                # Delete in correct order: trades, transactions, sold records, then purchase
                self.db.session.query(TradeAssociation).filter(
                    TradeAssociation.buyID == buyId
                ).delete()
                self.db.session.query(SecurityTransactions).filter(
                    SecurityTransactions.buyId == buyId
                ).delete()
                self.db.session.query(SoldSecurities).filter(
                    SoldSecurities.buyID == buyId
                ).delete()
                self.db.session.query(PurchasedSecurities).filter(
                    PurchasedSecurities.buyID == buyId
                ).delete()
            else:
                return jsonify({"Error": "Invalid service type"}), 400

            self.db.session.commit()
            return jsonify({"Message": "Successfully deleted"}), 200
        except SQLAlchemyError as ex:
            self.db.session.rollback()
            self.logger.error(f"Deletion failed for buyId {buyId}: {ex}")
            return jsonify({"Error": "Failed to delete record"}), 500

    def getJobsTable(self, page, filters=None, sort_by='due_date', sort_order='desc', page_size=10, limit=10):
        try:
            # Start with base query
            query = self.db.session.query(Jobs.Job)
            
            # Apply filters if provided
            if filters:
                if filters.get('title'):
                    query = query.filter(Jobs.Job.title.ilike(f"%{filters['title']}%"))
                
                if filters.get('status'):
                    query = query.filter(Jobs.Job.status == filters['status'])
                
                if filters.get('priority'):
                    query = query.filter(Jobs.Job.priority == filters['priority'])
                
                if filters.get('user_id'):
                    query = query.filter(Jobs.Job.user_id == filters['user_id'])
                
                if filters.get('min_failures') is not None:
                    query = query.filter(Jobs.Job.failures >= int(filters['min_failures']))
                
                if filters.get('max_failures') is not None:
                    query = query.filter(Jobs.Job.failures <= int(filters['max_failures']))
            
            # Apply sorting
            valid_sort_fields = ['id', 'title', 'status', 'priority', 'due_date', 'failures']
            if sort_by not in valid_sort_fields:
                sort_by = 'due_date'  # Default fallback
            
            column_attr = getattr(Jobs.Job, sort_by)
            if sort_order.lower() == 'asc':
                query = query.order_by(column_attr.asc())
            else:
                query = query.order_by(column_attr.desc())
            
            # Get total count before pagination
            total_count = query.count()
            
            # Apply pagination
            offset = (int(page) - 1) * page_size
            paginationQuery = query.offset(offset).limit(limit)
            results = paginationQuery.all()
            
            # Format results
            res = [{
                "ID": result.id,
                "Title": result.title,
                "Result": result.result,
                "Status": result.status,
                "Priority": result.priority,
                "DueTime": result.due_date,
                "Failures": result.failures,
                "UserID": result.user_id
            } for result in results]
            
            return {
                "results": res,
                "page": int(page),
                "page_size": page_size,
                "total_count": total_count,
                "total_pages": (total_count + page_size - 1) // page_size,
                "jobs": self.jobsObject,
                "applied_filters": {k: v for k, v in (filters or {}).items() if v is not None},
                "sort_by": sort_by,
                "sort_order": sort_order
            }
            
        except Exception as e:
            self.logger.error(f"Error fetching jobs table: {str(e)}")
            return {
                "error": "Failed to fetch jobs",
                "results": [],
                "page": int(page),
                "page_size": page_size,
                "total_count": 0,
                "total_pages": 0
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

    def setInvestmentHistory(self, data, user_id: str):
        newHistory = InvestmentHistory(
            date=datetime.datetime.now().strftime('%Y-%m-%d'),
            stocks=data['stocks'],
            mf=data['mf'],
            nps=data['nps'],
            epf=data['epf'],
            ppf=data['ppf'],
            gold=data['gold'],
            user=user_id,
        )
        self.db.session.add(newHistory)
        self.db.session.commit()
        return jsonify({"Success": "History inserted"}), 200

    def getFileTimeStamps(self):
        return self.StockService.JsonDownloadService.getTimeStampsOfAllFiles()

    def fetchKiteHoldings(self, userId):
        """Fetch holdings from Kite Connect API"""
        return self.StockService.fetch_kite_holdings(userId)

    def fetchKitePositions(self, userId):
        """Fetch positions from Kite Connect API"""
        return self.StockService.fetch_kite_positions(userId)

    def syncKiteHoldings(self, userId):
        """Sync Kite holdings to local database"""
        return self.StockService.sync_kite_holdings_to_db(userId)

    def fetchRealizedPnL(self, securityType, userId):
        """Fetch realized P&L from sold securities and historical closed trades"""
        return self.StockService.getRealizedPnL(securityType, userId)

    def getKiteLoginUrl(self):
        """Get Kite Connect login URL"""
        return self.StockService.kite_service.get_login_url()

    def generateKiteSession(self, userId, request_token):
        """Generate Kite session and store access token for user"""
        return self.StockService.generate_kite_session(userId, request_token)

    def fetchFOSummary(self, userId):
        """Fetch F&O P&L summary"""
        return self.FOService.get_fo_summary(userId)

    def fetchFOTrades(self, userId):
        """Fetch all individual F&O trades"""
        return self.FOService.get_fo_trades(userId)

