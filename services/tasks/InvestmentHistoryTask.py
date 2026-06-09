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
            # v5 (hq-wisp-a0byf, bead ak-agq): each block must reference its
            # OWN summary. Pre-v5 had three copy-paste bugs:
            #   - EPF totalValue + changePercent + changeAmount used ppfSummary
            #     (line 61 doubled the bug by storing a percent formula as
            #     "changeAmount").
            #   - Gold totalValue used ppfSummary['netProfit']; Gold
            #     changePercent used stale `changePercent` from the final
            #     MSN loop iteration above.
            # Each block now has its own cost-basis var (`net - netProfit`)
            # and an explicit ZeroDivisionError guard (a brand-new account
            # with `net == netProfit == 0` previously raised through the
            # outer try/except and silently failed the whole task).
            def _epg_dashboard_row(summary):
                # EPFService returns {} on calc failure; PPFService can
                # return 0-net for a brand-new account. Guard both.
                net = summary.get('net') if summary else 0
                netProfit = summary.get('netProfit') if summary else 0
                if net is None:
                    net = 0
                if netProfit is None:
                    netProfit = 0
                cost = net - netProfit
                if cost:
                    pct = (netProfit / cost) * 100
                else:
                    pct = 0
                return {
                    "totalValue": str(cost),
                    "currentValue": str(net),
                    "changePercent": str(pct),
                    "changeAmount": str(netProfit),
                }

            ppfSummary = self.investmentService.PPFService.fetchComplete(self.user_id)
            data['ppf'] = json.dumps(_epg_dashboard_row(ppfSummary))

            epfSummary = self.investmentService.EPFService.fetchComplete(self.user_id)
            data['epf'] = json.dumps(_epg_dashboard_row(epfSummary))

            goldSummary = self.investmentService.GoldService.fetchComplete(self.user_id)
            data['gold'] = json.dumps(_epg_dashboard_row(goldSummary))
            self.investmentService.setInvestmentHistory(data, self.user_id)
            return "Investment History Updated", "Completed", self.interval
        except Exception as ex:
            return ex.__str__(), "Failed", self.interval
