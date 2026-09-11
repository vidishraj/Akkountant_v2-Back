from abc import ABC
import time
from datetime import datetime

from sqlalchemy.exc import NoResultFound, SQLAlchemyError

from enums.MsnEnum import MSNENUM
from models import PurchasedSecurities
from models import SoldSecurities
from models.stockTrade import TradeAssociation
from services.Base_MSN import Base_MSN
import pandas as pd
import os
from dotenv import load_dotenv

# v8 (hq-wisp-3gs06, ak-m4r): monkey-patch nse_eq onto the NextApi
# endpoint BEFORE the conditional nsepython/nsepythonserver import below.
# nse_patch patches whichever (or both) library is importable, so the
# subsequent `nsepython.nse_eq(...)` call site goes through the patched
# function transparently. See utils/nse_patch.py for the audit + shape
# adapter rationale. Revert by deleting nse_patch.py + this import line.
import utils.nse_patch  # noqa: F401

load_dotenv()
if os.getenv('ENV') == "PROD":
    import nsepythonserver as nsepython
else:
    import nsepython
from decimal import Decimal, ROUND_DOWN
from utils.logger import Logger
from services.KiteService import KiteService, KiteAuthError
from utils.AIHelper import fetch_via_ai


class StocksService(Base_MSN, ABC):

    def __init__(self):
        super().__init__()
        self.baseAPIURL = "https://api.mfapi.in/"
        self.logger = Logger(__name__).get_logger()
        self.kite_service = KiteService()
        # Add in-memory cache with 5-minute expiry
        self._stock_cache = {}
        self._cache_expiry = 300  # 5 minutes in seconds

    def buySecurity(self, security_data, userId):
        try:
            # Validate the securityCode using the separate function
            if not self.checkIfSecurityExists(security_data['securityCode']):
                differentName = self.JsonDownloadService.checkSymbolChange(security_data['securityCode'])
                if differentName is None:
                    return {"error": "Invalid code"}
                else:
                    security_data['securityCode'] = differentName

            # Check if the specific trade has been inserted before
            if self.tradeExists(security_data['tradeID']):
                return {"error": "Trade exists"}

            # Check if the user has the same security bought already. If yes add
            existingRow: PurchasedSecurities = self.findIfSameSecurityTransactionExists(userId, security_data['buyID'])
            # Manage Date
            date = security_data.get('date')
            if date is None:
                date = self.dateTimeUtil.getCurrentDatetimeSqlFormat()
            transactionObject = dict(date=date, quant=security_data['buyQuant'], price=security_data['buyPrice'],
                                     transactionType="buy", userID=userId, securityType=MSNENUM.Stocks.value)
            if existingRow is None:
                # Proceed with insertion if validation passes and not existing
                buyID = security_data.get('buyID')
                # Insert Trade
                transactionObject['buyId'] = buyID
                newTrade = TradeAssociation(
                    tradeID=security_data['tradeID'],
                    buyID=buyID,
                )
                new_purchase = PurchasedSecurities(
                    buyID=buyID,
                    securityCode=security_data['securityCode'],
                    date=date,
                    buyQuant=security_data['buyQuant'],
                    buyPrice=security_data['buyPrice'],
                    userID=userId,
                    securityType=MSNENUM.Stocks.value
                )
                self.db.session.add(new_purchase)
            else:
                # Adding the new trade for existing security
                newTrade = TradeAssociation(
                    tradeID=security_data['tradeID'],
                    buyID=existingRow.buyID,
                )
                transactionObject['buyId'] = existingRow.buyID
                # We update the old purchase by finding average of price
                newQuant = existingRow.buyQuant + security_data['buyQuant']
                newPrice = (((existingRow.buyPrice * existingRow.buyQuant) +
                             (Decimal(security_data['buyQuant']) * Decimal(security_data['buyPrice'])))
                            / newQuant).quantize(Decimal('0.00001'), rounding=ROUND_DOWN)
                self.updatePriceAndQuant(newPrice, newQuant, existingRow.buyID)
            # Finally adding the trade
            self.insert_security_transaction(transactionObject)
            self.db.session.add(newTrade)
            return {"message": "Security purchased successfully"}

        except Exception as e:
            return {"error": str(e)}

    def sellSecurity(self, sell_data, userId):
        try:

            # Check if the specific trade has been inserted before
            if self.tradeExists(sell_data['tradeID']):
                return {"error": "Trade exists"}

            # Fetch the corresponding purchase record — prefer ISIN (buyID) for exact match
            purchase = None
            if sell_data.get('buyID'):
                purchase = self.findByBuyID(userId, sell_data['buyID'])
            if purchase is None:
                purchase = self.findIdIfSecurityBought(userId, sell_data['securityCode'])
            if purchase is None:
                return {'error': "Chronology error"}
            if sell_data['sellQuant'] > purchase.buyQuant:
                return {"error": "Sell quantity exceeds available quantity"}

            # Insert Trade (after validation to avoid orphaned records in session)
            newTrade = TradeAssociation(
                buyID=purchase.buyID,
                tradeID=sell_data['tradeID'],
            )
            self.db.session.add(newTrade)

            # Calculate profit
            profit = (Decimal(sell_data['sellQuant'] * sell_data['sellPrice']) - (
                    sell_data['sellQuant'] * Decimal(purchase.buyPrice))).quantize(Decimal('0.00001'),
                                                                                   rounding=ROUND_DOWN)

            # Reduce quantity purchased
            purchase.buyQuant = (Decimal(purchase.buyQuant) - sell_data['sellQuant'])

            # Deleting if quantity has become 0
            if purchase.buyQuant == 0:
                self.deleteSecurity(purchase.buyID)

            # Manage date
            date = sell_data.get('date')
            if date is None:
                date = self.dateTimeUtil.getCurrentDatetimeSqlFormat()

            # Insert transaction into separate table
            transactionObject = dict(date=date, quant=sell_data['sellQuant'], price=sell_data['sellPrice'],
                                     transactionType="sell", userID=userId, securityType=MSNENUM.Stocks.value,
                                     buyId=purchase.buyID)
            self.insert_security_transaction(transactionObject)

            # Insert into SoldSecurities
            new_sale = SoldSecurities(
                buyID=purchase.buyID,
                date=date,
                sellQuant=sell_data['sellQuant'],
                sellPrice=sell_data['sellPrice'],
                profit=profit,
                source_type='purchased'
            )

            self.db.session.add(new_sale)
            return {"message": "Security sold successfully", "sellID": new_sale.sellID, "profit": profit}
        except NoResultFound:
            return {"error": "Purchase record not found for the given buyID"}

    def deleteSecurity(self, buyId):
        security = self.db.session.query(PurchasedSecurities).filter(
            PurchasedSecurities.buyID == buyId).first()
        if security is None:
            self.logger.warning(f"deleteSecurity: No record found for buyID {buyId}")
            return
        # Setting quant and price to 0 is equivalent
        security.buyQuant = 0
        security.buyPrice = 0

    def readFromStatement(self, file_path, userId):
        """
        We will be reading the Zerodha trade book here
        :return:
        """
        df = pd.read_excel(file_path, header=14)

        # Columns to extract
        columns_to_extract = ["Symbol", "ISIN", "Trade Date", "Exchange", "Trade Type", "Quantity", "Price", "Trade ID"]

        # Extract the specified columns and convert to a list of dictionaries
        trade_data = df[columns_to_extract].to_dict(orient='records')

        # Build ISIN lookup from valid rows to fill NaN ISINs
        isin_lookup = {}
        for trade in trade_data:
            sym = trade.get('Symbol')
            isin = trade.get('ISIN')
            if sym and not pd.isna(isin) and not pd.isna(sym):
                isin_lookup[sym] = isin

        buyList = []
        sellList = []
        skipped = 0
        for trade in trade_data:
            # Skip rows with missing symbol or trade ID
            if pd.isna(trade.get('Symbol')) or pd.isna(trade.get('Trade ID')):
                skipped += 1
                continue
            # Fill missing ISIN from other rows of the same symbol
            if pd.isna(trade.get('ISIN')):
                resolved_isin = isin_lookup.get(trade['Symbol'])
                if resolved_isin:
                    trade['ISIN'] = resolved_isin
                    self.logger.info(f"Filled missing ISIN for {trade['Symbol']} -> {resolved_isin}")
                else:
                    skipped += 1
                    self.logger.warning(f"Skipping {trade['Symbol']} - no ISIN available")
                    continue
            # Resolve old symbol names to current names for both buys and sells
            symbol = trade['Symbol']
            if not self.checkIfSecurityExists(symbol):
                resolved = self.JsonDownloadService.checkSymbolChange(symbol)
                if resolved:
                    self.logger.info(f"Resolved old symbol {symbol} -> {resolved}")
                    symbol = resolved
                # If still unresolved, buySecurity will handle with "Invalid code"
            if trade['Trade Type'] == 'buy':
                buyList.append({
                    'buyID': trade['ISIN'],
                    'securityCode': symbol,
                    'date': trade['Trade Date'],
                    'buyQuant': trade['Quantity'],
                    'buyPrice': trade['Price'],
                    'tradeID': trade['Trade ID']
                })
            else:
                sellList.append({
                    'securityCode': symbol,
                    'buyID': trade['ISIN'],
                    'date': trade['Trade Date'],
                    'sellQuant': trade['Quantity'],
                    'sellPrice': trade['Price'],
                    'tradeID': trade['Trade ID']
                })
        if skipped:
            self.logger.warning(f"Skipped {skipped} rows with missing ISIN/Symbol/TradeID")
        self.logger.info(f"Read {len(buyList)} purchases and {len(sellList)} sells in the tradebook")
        boughtInserted = 0
        soldInserted = 0
        session = self.db.session
        try:
            with session.begin():  # Start an outer transaction
                # Process buying items
                for item in buyList:
                    insertedResult = self.buySecurity(item, userId)
                    if insertedResult.get('error') is None:
                        boughtInserted += 1
                    self.logger.info(f"Buying: {item.get('securityCode')} - {insertedResult}")
                session.flush()  # Ensure buying changes are sent to the database

                # Identify orphan sells (IPO allotments / pre-tradebook buys)
                orphan_sells = []
                for item in sellList:
                    purchase = None
                    if item.get('buyID'):
                        purchase = self.findByBuyID(userId, item['buyID'])
                    if purchase is None:
                        purchase = self.findIdIfSecurityBought(userId, item['securityCode'])
                    if purchase is None:
                        orphan_sells.append(item)

                if orphan_sells:
                    self.logger.info(f"Found {len(orphan_sells)} orphan sells — resolving IPO allotment prices via AI")
                    synthetic_buys = self._resolve_ipo_allotment_prices(orphan_sells)
                    for buy in synthetic_buys:
                        insertedResult = self.buySecurity(buy, userId)
                        if insertedResult.get('error') is None:
                            boughtInserted += 1
                            self.logger.info(f"Created IPO allotment buy: {buy['securityCode']} @ {buy['buyPrice']}")
                        else:
                            self.logger.warning(f"Failed to create IPO buy for {buy['securityCode']}: {insertedResult}")
                    session.flush()

                # Process selling items
                for item in sellList:
                    insertedResult = self.sellSecurity(item, userId)
                    if insertedResult.get('error') is None:
                        soldInserted += 1
                    elif insertedResult['error'] == 'Chronology error':
                        self.logger.warning(f"Skipping sell for {item.get('securityCode')} - no matching buy found")
                    self.logger.info(f"Selling: {item.get('securityCode')} - {insertedResult}")

            # The commit happens automatically at the end of 'with session.begin()' if no errors occur
            self.logger.info("Transaction committed successfully.")

        except SQLAlchemyError as e:
            session.rollback()
            self.logger.error(f"Transactions failed. Rolled back. Error: {e}")
            raise

        self.logger.info("Finished processing file and inserting statements")
        return {"readFromStatement": {'buy': len(buyList), 'sold': len(sellList)},
                "inserted": {'buy': boughtInserted, 'sold': soldInserted}}

    def _resolve_ipo_allotment_prices(self, orphan_sells):
        """
        Given sell records that have no matching buy (IPO allotments, corporate actions, etc.),
        use AI + web search to find the original allotment/issue price and create synthetic buy dicts.
        """
        # Group orphan sells by ISIN to avoid duplicate lookups
        by_isin = {}
        for sell in orphan_sells:
            isin = sell.get('buyID')
            if isin and isin not in by_isin:
                by_isin[isin] = sell

        if not by_isin:
            return []

        # Build a single prompt for all orphan securities
        lines = []
        for isin, sell in by_isin.items():
            lines.append(f"- {sell['securityCode']} (ISIN: {isin}), first sell date: {sell['date']}")

        prompt = (
            "I need the IPO allotment price (issue price to retail investors) for the following "
            "Indian stocks. These were likely acquired via IPO allotment or corporate action "
            "before being sold on NSE.\n\n"
            + "\n".join(lines) +
            "\n\nFor each stock, search the web and return a JSON object with this exact format:\n"
            "{\n"
            '  "results": [\n'
            '    {"symbol": "SYMBOL", "isin": "ISIN", "allotment_price": 123.45, "allotment_date": "YYYY-MM-DD", "source": "brief description"},\n'
            "    ...\n"
            "  ]\n"
            "}\n\n"
            "The allotment_price should be the price at which retail investors were allotted shares in the IPO. "
            "If the stock was received via a corporate action (demerger, bonus, etc.) rather than an IPO, "
            "use the listing price on the first day of trading. "
            "allotment_date should be the IPO allotment date or listing date."
        )

        self.logger.info(f"Querying AI for IPO allotment prices of {len(by_isin)} securities")
        result, ai_err = fetch_via_ai(prompt)

        if not result or 'results' not in result:
            self.logger.warning(
                f"AI returned no results for IPO price lookup (err={ai_err!r}): {result}"
            )
            return []

        synthetic_buys = []
        for entry in result['results']:
            isin = entry.get('isin')
            price = entry.get('allotment_price')
            if not isin or not price or isin not in by_isin:
                continue

            sell = by_isin[isin]
            # Total quantity for this ISIN across all orphan sells
            total_qty = sum(s['sellQuant'] for s in orphan_sells if s.get('buyID') == isin)
            allotment_date = entry.get('allotment_date', str(sell['date']))

            synthetic_buys.append({
                'buyID': isin,
                'securityCode': sell['securityCode'],
                'date': allotment_date,
                'buyQuant': total_qty,
                'buyPrice': price,
                'tradeID': f"IPO_{isin}_{allotment_date}",
            })
            self.logger.info(
                f"AI resolved: {sell['securityCode']} IPO allotment @ {price} "
                f"(qty={total_qty}, date={allotment_date}, source={entry.get('source', 'unknown')})"
            )

        return synthetic_buys

    def getSecurityList(self):
        self.JsonDownloadService.getStockList()

    def _cleanup_cache(self):
        """Remove expired entries from cache to prevent memory bloat"""
        current_time = time.time()
        expired_keys = [
            key for key, value in self._stock_cache.items()
            if current_time - value['timestamp'] >= self._cache_expiry
        ]
        for key in expired_keys:
            del self._stock_cache[key]
        if expired_keys:
            self.logger.debug(f"Cleaned up {len(expired_keys)} expired cache entries")

    def findSecurity(self, securityCode):
        # Check cache first
        current_time = time.time()
        if securityCode in self._stock_cache:
            cache_entry = self._stock_cache[securityCode]
            if current_time - cache_entry['timestamp'] < self._cache_expiry:
                self.logger.debug(f"Cache hit for {securityCode}")
                return cache_entry['data']
        
        # Clean up cache periodically (every 50 requests)
        if len(self._stock_cache) > 50:
            self._cleanup_cache()
        
        try:
            quote = nsepython.nse_eq(securityCode)
            if quote:  # Only cache successful responses
                self._stock_cache[securityCode] = {
                    'data': quote,
                    'timestamp': current_time
                }
                self.logger.info(f"Successfully fetched live data for {securityCode}")
                return quote
            else:
                self.logger.warning(f"No data returned for {securityCode}")
                return None
        except Exception as ex:
            self.logger.error(f"Error while fetching symbol from NSEPYTHON {ex}")
            return None

    def tradeExists(self, tradeId: str):
        tradeRow = self.db.session.query(TradeAssociation).filter(TradeAssociation.tradeID == tradeId).first()
        if tradeRow:
            return True
        return False

    def checkIfSecurityExists(self, symbol):
        stockList = self.JsonDownloadService.getStockList()
        stockList = stockList['data']
        # @TODO CORNER CASE? How to manage changed codes?
        for stock in stockList:
            if stock.get('stockCode') == symbol:
                return True
        return False

    def fetch_kite_holdings(self, userId):
        """Fetch holdings from Kite and format for the Kite-view display.

        Kite's holdings payload already carries settlement, day-change and
        collateral detail; we used to drop all of it on the floor here. Keeping
        it costs no extra API call and no extra auth surface.

        ## Quantity semantics (ak-yz9c fold-in, 2026-09-11)

        Under Overseer's 2026-09-11 directive T+1 shares are treated as
        **owned** for accounting: Invested / TotalAssetValue / Change /
        %Change / day P&L all fold T+1 into the totals. The chip stays as a
        visual "not yet in demat" indicator; the summary "Pending (T+1)"
        column is REMOVED.

        Wire naming (Option (i), agreed with akkountant_frontend
        2026-09-11 -- see contract discussion on the ak-yz9c bead):
          * ``quantity``      -- KITE raw settled qty. Semantic UNCHANGED from
                                 pre-fold to avoid a silent double-count on
                                 any FE consumer that still does
                                 ``quantity + t1_quantity``. Kept for
                                 backwards compat.
          * ``t1_quantity``   -- KITE raw T+1 qty. Semantic UNCHANGED for the
                                 same backwards-compat reason. Present
                                 alongside the new ``t1_qty`` alias.
          * ``settled_qty``   -- NEW. Kite's settled qty view, coerced to a
                                 non-None int/float. Explicit fold-in field.
          * ``t1_qty``        -- NEW. Renamed alias of ``t1_quantity`` for
                                 symmetry with ``settled_qty``. FE cuts over
                                 to the new name in its ak-yz9c commit.
          * ``total_qty``     -- NEW. ``settled_qty + t1_qty`` (BLENDED total).
                                 The fold discriminator field.

        Money-math fields (ak-yz9c fold, Path A -- awaiting Overseer stamp):
          * ``invested``            = total_qty × average_price
          * ``current_value``       = total_qty × last_price
          * ``unrealized_pnl``      = current_value - invested
          * ``day_change_amount``   = total_qty × (last_price - close_price)
                                      (folds T+1 per Overseer Q2 answer)

        Kite's ``average_price`` is the broker-canonical weighted avg for
        the NET position (settled + T+1 combined). Verified via Kite Connect
        docs + real T+2 settlement behavior (avg unchanged, just reclassifies
        buckets). No separate ``t1_average_price`` field is exposed by Kite;
        no re-derivation is needed under Path A -- we use ``average_price``
        directly as the blended broker-canonical avg.

        ## Removed field

        ``pending_t1_value`` -- REMOVED per Overseer Q1 answer: redundant
        once T+1 folds into Invested / TotalAssetValue / per-holding cost.
        FE-side removes the render at MSNSummary.tsx L198-208. Leaving it
        emitted would tempt a future FE consumer to re-introduce the
        double-count path.

        ## Prior-policy history (why the pre-fold contract was inverted)

        The ak-w4p contract (Aug 2026) treated Kite's ``average_price`` as a
        cross-check hint only, kept ``quantity`` settled-only, and surfaced
        ``pending_t1_value`` as a separate rupee amount excluded from P&L.
        Rationale at the time: "valuing pending shares at market inside P&L
        would book their entire market value as phantom profit." That was
        correct under a statement-authoritative cost basis with no T+1 cost.

        Superseded by:
          * 2026-09-04 flatten-on-sync policy -- Kite ``average_price`` is
            broker-canonical for the Kite-enriched display path
          * 2026-09-11 Overseer directive (ak-yz9c) -- fold T+1 fully
          * Overseer Q1 confirm (remove Pending T+1 col) / Q2 confirm
            (T+1 in day P&L)

        Statement-DB write path (``sync_kite_holdings_to_db``) is unchanged
        -- statement ledger remains statement-authoritative, distinct from
        the Kite display path. See guarding tests in
        test_kite_holdings_fields.py section 6.
        """
        try:
            holdings = self.kite_service.get_holdings(userId)
            formatted_holdings = []

            for holding in holdings:
                # Coerce every numeric to a defined non-None value first --
                # Kite is known to emit `null` for optional fields on
                # stripped-down responses, and downstream arithmetic must
                # never see None.
                settled_qty = holding.get('quantity', 0) or 0
                t1_qty = holding.get('t1_quantity', 0) or 0
                total_qty = settled_qty + t1_qty

                average_price = holding.get('average_price', 0) or 0
                last_price = holding.get('last_price', 0) or 0
                close_price = holding.get('close_price', 0) or 0

                # Folded money math (Path A: Kite average_price as
                # broker-canonical blended avg). If Overseer stamps Path B or
                # C, `invested` and `unrealized_pnl` shift to
                # `InvestmentService.fetchUserSecurities` where DB `buyPrice`
                # is visible; `current_value` and `day_change_amount` stay
                # here (they don't touch DB `buyPrice`).
                invested = float(total_qty) * float(average_price)
                current_value = float(total_qty) * float(last_price)
                unrealized_pnl = current_value - invested
                day_change_amount = float(total_qty) * (float(last_price) - float(close_price))

                formatted_holding = {
                    'symbol': holding.get('tradingsymbol'),
                    # --- raw Kite fields (semantic preserved, see docstring) ---
                    'quantity': settled_qty,
                    't1_quantity': t1_qty,

                    # --- ak-yz9c fold-in additive fields ---
                    'settled_qty': settled_qty,
                    't1_qty': t1_qty,
                    'total_qty': total_qty,

                    # --- prices (Kite broker-canonical) ---
                    'average_price': average_price,
                    'last_price': last_price,
                    'close_price': close_price,

                    # --- ak-yz9c folded money math ---
                    'invested': invested,
                    'current_value': current_value,
                    'unrealized_pnl': unrealized_pnl,
                    'day_change_amount': day_change_amount,

                    # --- Kite's own settled-only P&L / day change (preserved
                    #     for observability; NOT the folded values) ---
                    'pnl': holding.get('pnl', 0),
                    'day_change': holding.get('day_change', 0) or 0,
                    'day_change_percentage': holding.get('day_change_percentage', 0) or 0,

                    # --- identifiers ---
                    'product': holding.get('product'),
                    'exchange': holding.get('exchange'),
                    'isin': holding.get('isin'),

                    # --- settlement / quantity breakdown (unchanged pre-fold) ---
                    'realised_quantity': holding.get('realised_quantity', 0) or 0,
                    'authorised_quantity': holding.get('authorised_quantity', 0) or 0,
                    'collateral_quantity': holding.get('collateral_quantity', 0) or 0,
                    'collateral_type': holding.get('collateral_type'),

                    # --- margin trading facility (zeros when MTF is unused) ---
                    'mtf_quantity': (holding.get('mtf') or {}).get('quantity', 0) or 0,
                    'mtf_average_price': (holding.get('mtf') or {}).get('average_price', 0) or 0,
                }
                formatted_holdings.append(formatted_holding)

            self.logger.info(f"Fetched {len(formatted_holdings)} holdings from Kite for user {userId}")
            return formatted_holdings

        except Exception as e:
            self.logger.error(f"Error fetching Kite holdings for user {userId}: {str(e)}")
            raise

    #: Fields copied from a Kite holding onto a securities-list row. Kept
    #: snake_case and at the row's top level.
    #:
    #: ak-yz9c (2026-09-11): added `settled_qty`, `t1_qty`, `total_qty`, and
    #: the folded money-math fields (`invested`, `current_value`,
    #: `unrealized_pnl`, `day_change_amount`). Removed `pending_t1_value`
    #: (Overseer Q1 answer; redundant once T+1 folds in). Kept `t1_quantity`
    #: for one-cycle backwards compat while FE renames to `t1_qty`.
    KITE_ROW_FIELDS = (
        # Fold-in additive quantities (ak-yz9c)
        'settled_qty',
        't1_qty',
        'total_qty',
        # Legacy raw Kite quantities (kept for FE-side transition window)
        't1_quantity',
        # Preserved from pre-fold
        'realised_quantity',
        'authorised_quantity',
        'collateral_quantity',
        # Prices (Kite broker-canonical)
        'close_price',
        'day_change',
        'day_change_percentage',
        'average_price',
        'last_price',
        # Folded money math (ak-yz9c)
        'invested',
        'current_value',
        'unrealized_pnl',
        'day_change_amount',
        # MTF fields
        'mtf_quantity',
    )

    def kite_holdings_index(self, userId):
        """Kite holdings keyed by trading symbol, for enriching securities rows.

        Returns ``(index, status)``. Enrichment is strictly best-effort: an
        expired Kite session must NOT take down the securities list, which is
        otherwise built entirely from our own DB plus the NSE price feed. On
        any Kite failure we return an empty index and let every row render
        without the optional fields.
        """
        try:
            holdings = self.fetch_kite_holdings(userId)
        except KiteAuthError as e:
            self.logger.warning(
                f"Skipping Kite enrichment of securities list for {userId}: {str(e)}"
            )
            return {}, {"connected": False, "reconnect_required": True}
        except Exception as e:
            self.logger.error(
                f"Skipping Kite enrichment of securities list for {userId}: {str(e)}"
            )
            return {}, {"connected": False, "reconnect_required": False}

        index = {}
        for holding in holdings:
            symbol = holding.get('symbol')
            if symbol:
                index[symbol] = holding
        return index, {"connected": True, "reconnect_required": False}

    def fetch_kite_positions(self, userId):
        """Fetch positions from Kite"""
        try:
            positions = self.kite_service.get_positions(userId)
            day_positions = positions.get('day', [])
            net_positions = positions.get('net', [])
            
            formatted_positions = {
                'day': [],
                'net': []
            }
            
            for position in day_positions:
                formatted_positions['day'].append({
                    'symbol': position.get('tradingsymbol'),
                    'quantity': position.get('quantity', 0),
                    'average_price': position.get('average_price', 0),
                    'last_price': position.get('last_price', 0),
                    'pnl': position.get('pnl', 0),
                    'product': position.get('product'),
                    'exchange': position.get('exchange')
                })
            
            for position in net_positions:
                formatted_positions['net'].append({
                    'symbol': position.get('tradingsymbol'),
                    'quantity': position.get('quantity', 0),
                    'average_price': position.get('average_price', 0),
                    'last_price': position.get('last_price', 0),
                    'pnl': position.get('pnl', 0),
                    'product': position.get('product'),
                    'exchange': position.get('exchange')
                })
            
            self.logger.info(f"Fetched positions from Kite for user {userId}")
            return formatted_positions
            
        except Exception as e:
            self.logger.error(f"Error fetching Kite positions for user {userId}: {str(e)}")
            raise

    def generate_kite_session(self, userId, request_token):
        """Generate Kite session using request token"""
        try:
            return self.kite_service.generate_session(userId, request_token)
        except Exception as e:
            self.logger.error(f"Error generating Kite session for user {userId}: {str(e)}")
            raise

    def sync_kite_holdings_to_db(self, userId):
        """Sync Kite holdings to local database"""
        try:
            holdings = self.fetch_kite_holdings(userId)
            synced_count = 0
            
            for holding in holdings:
                if holding['quantity'] > 0:  # Only sync holdings with positive quantity
                    # Check if this holding already exists in our database
                    existing = self.db.session.query(PurchasedSecurities).filter(
                        PurchasedSecurities.userID == userId,
                        PurchasedSecurities.securityCode == holding['symbol']
                    ).first()
                    
                    if not existing:
                        # Create new entry
                        randomBuyId = self.genericUtil.generate_custom_buyID()
                        new_holding = PurchasedSecurities(
                            buyID=randomBuyId,
                            userID=userId,
                            securityCode=holding['symbol'],
                            buyQuant=holding['quantity'],
                            buyPrice=holding['average_price'],
                            date=self.dateTimeUtil.getCurrentDatetimeSqlFormat(),
                            securityType=MSNENUM.Stocks.value
                        )
                        self.db.session.add(new_holding)
                        synced_count += 1
                    else:
                        # Update existing entry if quantities differ
                        if existing.buyQuant != holding['quantity']:
                            existing.buyQuant = holding['quantity']
                            existing.buyPrice = holding['average_price']
                            synced_count += 1
            
            self.db.session.commit()
            self.logger.info(f"Synced {synced_count} holdings from Kite to database for user {userId}")
            return {"synced": synced_count, "total_holdings": len(holdings)}
            
        except Exception as e:
            self.db.session.rollback()
            self.logger.error(f"Error syncing Kite holdings to database for user {userId}: {str(e)}")
            raise
