import json
from decimal import Decimal, ROUND_DOWN

from services.tasks.baseTask import BaseTask
from utils.logger import Logger


class InvestmentHistoryTask(BaseTask):
    _instance = None

    def __new__(cls, *args, **kwargs):
        if not cls._instance:
            cls._instance = super(InvestmentHistoryTask, cls).__new__(cls)
        return cls._instance

    def __init__(self, title, priority):
        if not hasattr(self, 'initialized'):  # Prevent multiple initializations
            super().__init__(title, priority)
            self.logger = Logger(__name__).get_logger()
            # 24 hours
            self.interval = 60*24

    def run(self):
        try:
            data = {}
            if not self.user_id:
                self.logger.error("User ID not found. Stopping task")
                return "No userid", "Failed", self.interval

            serviceTypes = ['Stocks', 'Mutual_Funds', 'NPS']
            for securityType in serviceTypes:
                activeInvested = self.investmentService.StockService.getActiveMoneyInvested(securityType, self.user_id)
                activeProfitAll = self.investmentService.StockService.calculateProfitAndCurrentValue(securityType,
                                                                                                     self.user_id)
                totalProfit = 0
                changePercent = 0
                if activeInvested != 0:
                    for item in activeProfitAll:
                        totalProfit += activeProfitAll[item]['profit']
                    changePercent = (((totalProfit + activeInvested) - activeInvested) / activeInvested) * 100
                data[securityType] = json.dumps({
                    "totalValue": str(Decimal(activeInvested).quantize(Decimal('0.01'), rounding=ROUND_DOWN)),
                    "currentValue": str(Decimal(activeInvested + totalProfit).quantize(Decimal('0.01'),
                                                                                       rounding=ROUND_DOWN)),
                    "changePercent": str(Decimal(changePercent).quantize(Decimal('0.01'), rounding=ROUND_DOWN)),
                    "changeAmount": str(Decimal(totalProfit).quantize(Decimal('0.01'), rounding=ROUND_DOWN)),
                })
            ppfSummary = self.investmentService.PPFService.fetchComplete(self.user_id)
            data['ppf'] = json.dumps({
                "totalValue": str(ppfSummary['net'] - ppfSummary['netProfit']),
                "currentValue": str(ppfSummary['net']),
                "changePercent": str((ppfSummary['netProfit']/(ppfSummary['net'] - ppfSummary['netProfit']))*100),
                "changeAmount": str(ppfSummary['netProfit']),
            })

            epfSummary = self.investmentService.EPFService.fetchComplete(self.user_id)
            data['epf'] = json.dumps({
                "totalValue": str(epfSummary['net'] - ppfSummary['netProfit']),
                "currentValue": str(epfSummary['net']),
                "changePercent": str((ppfSummary['netProfit']/(ppfSummary['net'] - ppfSummary['netProfit']))*100),
                "changeAmount": str((ppfSummary['netProfit']/(ppfSummary['net'] - ppfSummary['netProfit']))*100),
            })

            goldSummary = self.investmentService.GoldService.fetchComplete(self.user_id)
            data['gold'] = json.dumps({
                "totalValue": str(goldSummary['net'] - ppfSummary['netProfit']),
                "currentValue": str(goldSummary['net']),
                "changePercent": str(Decimal(changePercent).quantize(Decimal('0.01'), rounding=ROUND_DOWN)),
                "changeAmount": str(goldSummary['netProfit']),
            })
            self.investmentService.setInvestmentHistory(data, self.user_id)
            return "Investment History Updated", "Completed", self.interval
        except Exception as ex:
            return ex.__str__(), "Failed", self.interval
