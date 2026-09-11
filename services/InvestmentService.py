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
# ak-ran v2 #5: leaf-module constant so the WealthDigest routing key
# is a single source of truth across scheduler + jobsObject + bootstrap.
# wealth_digest_constants has ZERO transitive deps, so the import
# doesn't create the InvestmentService <-> services.tasks.* cycle.
from services.tasks.wealth_digest_constants import WEALTH_DIGEST_JOB_TITLE
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
        "CheckStatement": "Check Statements",
        # ak-ran Phase 1 + v2 #5: manual-trigger + scheduler-recognized
        # title for the daily wealth-management digest task. Key is
        # the shared WEALTH_DIGEST_JOB_TITLE constant so a rename
        # only touches one file.
        WEALTH_DIGEST_JOB_TITLE: "Wealth Digest (daily briefing)",
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
            marketStatus = marketStatus['marketState'][0]['marketStatus']
            marketStatus = marketStatus != 'Closed'
        except Exception:
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
        kiteHoldings = {}
        if securityType == MSNENUM.Stocks.value:
            stockRates = self.StockService.calculateStockRates(activeSecurities)
            # Settlement / day-change detail from Kite. Best-effort: an expired
            # Kite session leaves the index empty and every row simply renders
            # without the optional fields.
            kiteHoldings, _kiteStatus = self.StockService.kite_holdings_index(userId)

        for security in activeSecurities:
            if securityType == MSNENUM.Stocks.value:
                symbol = self.StockService.resolveSymbol(security['buyCode'])
                security['info'] = stockRates.get(symbol, {
                    'symbol': symbol, 'lastPrice': 0, 'change': 0,
                    'pChange': 0, 'previousClose': 0, 'error': 'NOT_FOUND'
                })
                # Kite settlement/day-change fields, snake_case at row top
                # level.
                #
                # `buyQuant` (statement-DB settled qty) and `buyPrice`
                # (statement-derived cost) stay unchanged on the enriched
                # row -- the ak-yz9c fold-in adds `settled_qty` / `t1_qty` /
                # `total_qty` / `invested` / `current_value` /
                # `unrealized_pnl` / `day_change_amount` alongside, via
                # KITE_ROW_FIELDS overlay (Option (i) wire naming agreed
                # with akkountant_frontend 2026-09-11). FE reads the new
                # fields for the fold display; `buyQuant` / `buyPrice`
                # remain accessible for statement-authoritative views.
                #
                # The statement-DB write path (`sync_kite_holdings_to_db`)
                # remains statement-authoritative and unchanged — the fold
                # operates on the DISPLAY path only.
                kiteHolding = kiteHoldings.get(symbol)
                if kiteHolding:
                    for field in self.StockService.KITE_ROW_FIELDS:
                        if field in kiteHolding:
                            security[field] = kiteHolding[field]
            elif securityType == MSNENUM.NPS.value:
                try:
                    npsInfo = self.NPSService.findSecurity(security['buyCode'])

                    # Check if we got valid data
                    if not npsInfo or 'nav' not in npsInfo:
                        self.logger.warning(f"No NPS rate data found for scheme {security['buyCode']}")
                        security['info'] = {
                            'nav': 0,
                            'name': security['buyCode'],
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
                        'name': npsInfo.get('scheme_name', security['buyCode']),
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
                        'name': security['buyCode'],
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
        # Purchase from the UI is possible for MF, NPS, EPF, PF or Gold
        # NOTE: buyPrice in the DB = per-unit price (NAV), NOT total amount.
        # The agent sends {quantity (units), amount (total ₹)} so we compute: NAV = amount / quantity.
        #
        # ak-bgc fix: each branch validates its own required fields up
        # front and raises ValueError on missing / non-positive values.
        # Previously the MF/NPS branches did `data['quantity']` raw which
        # surfaced as a generic KeyError 500 (no clue what was missing),
        # and the NAV calc was `amount / quantity if quantity != 0 else 0`
        # which silently created NAV=0 rows on bad input. Both now loud-fail
        # so the agent gets a clear retry message instead of corrupted data.
        if serviceType == MSNENUM.Mutual_Funds:
            self._validate_msn_insert_data("Mutual_Funds", data)
            quantity = float(data['quantity'])
            amount = float(data['amount'])
            insertionObject = {
                "securityCode": data['schemeCode'],
                "date": DateTimeUtil().convert_to_sql_datetime(data['date'], DateStatementEnum.EPF_STATEMENT.name),
                "buyQuant": quantity,
                "buyPrice": amount / quantity,
            }
            status = self.MFService.buySecurity(insertionObject, userId)
            if 'error' in status:
                return jsonify({"Error": "Error in MF entry"}), 406
            return jsonify({"Message": "MF Transaction inserted successfully"}), 200
        elif serviceType == MSNENUM.NPS:
            self._validate_msn_insert_data("NPS", data)
            quantity = float(data['quantity'])
            amount = float(data['amount'])
            insertionObject = {
                "securityCode": data['schemeCode'],
                "date": DateTimeUtil().convert_to_sql_datetime(data['date'], DateStatementEnum.EPF_STATEMENT.name),
                "buyQuant": quantity,
                "buyPrice": amount / quantity,
            }
            status = self.NPSService.buySecurity(insertionObject, userId)
            if 'error' in status:
                return jsonify({"Error": "Error in NPS entry"}), 406
            return jsonify({"Message": "NPS Transaction inserted successfully"}), 200
        elif serviceType == EPGEnum.EPF:
            return self.EPFService.insertDeposit(data, userId)
        elif serviceType == EPGEnum.PF:
            return self.PPFService.insertDeposit(data, userId)
        elif serviceType == EPGEnum.Gold:
            return self.GoldService.insertDeposit(data, userId)

    @staticmethod
    def _validate_msn_insert_data(label, data):
        """ak-bgc fix: strict pre-flight on MF/NPS insert payload.

        Both share the same agent-side shape (schemeCode + date + quantity
        + amount). Raising ValueError surfaces a clear actionable message
        to the agent via the MCP tool error path; the previous bare
        `data['quantity']` style would 500 with a useless KeyError repr.
        """
        required = ('schemeCode', 'date', 'quantity', 'amount')
        missing = [k for k in required if not data.get(k)]
        if missing:
            raise ValueError(
                f"{label} insert requires {list(required)}; missing: {missing}. "
                f"Got fields: {sorted(data.keys())}."
            )
        try:
            quantity = float(data['quantity'])
            amount = float(data['amount'])
        except (TypeError, ValueError) as exc:
            raise ValueError(
                f"{label} insert: quantity and amount must be numeric; "
                f"got quantity={data['quantity']!r}, amount={data['amount']!r} ({exc})"
            ) from exc
        if quantity <= 0:
            raise ValueError(
                f"{label} insert: quantity must be > 0; got {quantity}. "
                "Zero / negative quantity would have produced NAV=0 rows "
                "silently (audit ak-bgc); re-prompt the user for the unit count."
            )
        if amount <= 0:
            raise ValueError(
                f"{label} insert: amount must be > 0; got {amount}."
            )

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
        # Check for existing Pending or Overdue job with the same title
        existing = self.db.session.query(Jobs.Job).filter(
            Jobs.Job.title == jobId,
            Jobs.Job.status.in_(["Pending", "Overdue"])
        ).first()
        if existing:
            return jsonify({"Error": f"Job '{jobId}' is already queued (status: {existing.status})"}), 409
        from utils.DateTimeUtil import clamp_to_allowed_window
        newJob = Jobs.Job(
                title=jobId,
                status="Pending",
                priority="High",
                due_date=clamp_to_allowed_window(datetime.datetime.now()),
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

    def fetchInvestmentEmails(self, user_id, category=None, page=1, page_size=50, service_type=None, categories=None):
        """Query processedEmails for specified categories (defaults to investment)."""
        from models.processedEmails import ProcessedEmails
        from sqlalchemy import cast, String

        if categories is None:
            categories = [
                'investment_confirmation', 'epf_passbook', 'gold_receipt', 'nps_statement'
            ]

        query = self.db.session.query(ProcessedEmails).filter(
            ProcessedEmails.user_id == user_id,
            ProcessedEmails.category.in_(categories)
        )

        if category:
            query = query.filter(ProcessedEmails.category == category)

        if service_type:
            query = query.filter(
                cast(ProcessedEmails.extraction_summary['service_type'], String) == f'"{service_type}"'
            )

        total = query.count()
        offset = (page - 1) * page_size
        rows = query.order_by(ProcessedEmails.email_date.desc()).offset(offset).limit(page_size).all()

        emails = []
        for row in rows:
            emails.append({
                "id": row.id,
                "gmail_id": row.gmail_id,
                "sender": row.sender,
                "subject": row.subject,
                "email_date": row.email_date.strftime('%Y-%m-%d') if row.email_date else None,
                "category": row.category,
                "items_extracted": row.items_extracted,
                "extraction_summary": row.extraction_summary,
                "status": row.status,
            })

        return {
            "emails": emails,
            "total": total,
            "page": page,
            "pageSize": page_size,
        }

    def getInvestmentSnapshots(self, user_id, date_from=None, date_to=None, investment_type=None):
        """Query investment snapshots for growth charts."""
        from models.investmentSnapshot import InvestmentSnapshot

        query = self.db.session.query(InvestmentSnapshot).filter(
            InvestmentSnapshot.user == user_id
        )
        if date_from:
            query = query.filter(InvestmentSnapshot.date >= date_from)
        if date_to:
            query = query.filter(InvestmentSnapshot.date <= date_to)
        if investment_type:
            query = query.filter(InvestmentSnapshot.investment_type == investment_type)

        rows = query.order_by(
            InvestmentSnapshot.date.asc(), InvestmentSnapshot.investment_type.asc()
        ).all()

        return [
            {
                "date": row.date.strftime('%Y-%m-%d'),
                "investmentType": row.investment_type,
                "totalInvested": float(row.total_invested),
                "currentValue": float(row.current_value),
                "profit": float(row.profit),
                "profitPercent": float(row.profit_percent),
            }
            for row in rows
        ]

    def fetchEmailBody(self, user_id, gmail_id):
        """Fetch full email body from Gmail API for a given gmail_id."""
        import base64
        from models.googleTokens import UserToken
        from enums.ServiceTypeEnum import ServiceTypeEnum

        user_token = self.db.session.query(UserToken).filter_by(
            user_id=user_id,
            service_type=ServiceTypeEnum.Gmail.value,
        ).first()
        if not user_token:
            return {"error": "Gmail not connected"}, 401

        token_info = {
            'token': user_token.access_token,
            'refresh_token': user_token.refresh_token,
            'client_id': user_token.client_id,
            'client_secret': user_token.client_secret,
        }

        gmail_api = self.gmailService.googleService.get_gmail_service(user_id, token_info)
        if not gmail_api:
            return {"error": "Failed to initialize Gmail service"}, 500

        msg = gmail_api.users().messages().get(userId='me', id=gmail_id, format='full').execute()
        payload = msg.get('payload', {})

        # Extract body — handle both single-part and multipart emails
        html_body = None
        text_body = None

        def _extract_parts(parts):
            nonlocal html_body, text_body
            for part in parts:
                mime = part.get('mimeType', '')
                data = part.get('body', {}).get('data')
                if data:
                    decoded = base64.urlsafe_b64decode(data).decode('utf-8', errors='replace')
                    if mime == 'text/html' and not html_body:
                        html_body = decoded
                    elif mime == 'text/plain' and not text_body:
                        text_body = decoded
                # Recurse into nested parts
                if 'parts' in part:
                    _extract_parts(part['parts'])

        if 'parts' in payload:
            _extract_parts(payload['parts'])
        else:
            data = payload.get('body', {}).get('data')
            if data:
                decoded = base64.urlsafe_b64decode(data).decode('utf-8', errors='replace')
                mime = payload.get('mimeType', '')
                if mime == 'text/html':
                    html_body = decoded
                else:
                    text_body = decoded

        return {
            "gmail_id": gmail_id,
            "body_html": html_body,
            "body_text": text_body,
        }

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

    def getKiteTokenStatus(self, userId):
        """Kite connection state plus the URL needed to re-authenticate.

        Kite tokens die at 6 AM IST daily and cannot be renewed programmatically,
        so the UI needs a cheap way to ask "do I need to show Reconnect?" without
        first failing a data call.
        """
        status = self.StockService.kite_service.get_token_status(userId)
        if status.get("reconnect_required"):
            try:
                status["login_url"] = self.StockService.kite_service.get_login_url()
            except Exception as e:
                self.logger.error(f"Could not build Kite login URL: {str(e)}")
                status["login_url"] = None
        return status

    def generateKiteSession(self, userId, request_token):
        """Generate Kite session and store access token for user"""
        return self.StockService.generate_kite_session(userId, request_token)

    def fetchFOSummary(self, userId):
        """Fetch F&O P&L summary"""
        return self.FOService.get_fo_summary(userId)

    def fetchFOTrades(self, userId):
        """Fetch all individual F&O trades"""
        return self.FOService.get_fo_trades(userId)

