import os
import re
from decimal import Decimal

import pandas as pd
from flask import g, jsonify
from flask_sqlalchemy import SQLAlchemy
from sqlalchemy import create_engine
from sqlalchemy.exc import IntegrityError, SQLAlchemyError
from sqlalchemy.orm import sessionmaker, scoped_session
from logging import Logger as LG

from models.foTrades import FOTrade, OptionType, TradeType
from utils.DotDict import DotDict
from utils.logger import Logger


class FOService:
    db: SQLAlchemy
    logger: LG

    @property
    def db(self):
        if g.get('db') is None:
            DATABASE_URL = os.getenv('DATABASE_URL')
            engine = create_engine(DATABASE_URL)
            db_session = scoped_session(sessionmaker(autocommit=False, autoflush=False, bind=engine))
            return DotDict({'session': db_session})
        return g.db

    def __init__(self):
        self.logger = Logger(__name__).get_logger()

    @staticmethod
    def parse_fo_symbol(symbol):
        """
        Parse an FO contract symbol into underlying, strike_price, and option_type.

        Examples:
            RELIANCE21MAR2200CE -> {underlying: RELIANCE, strike_price: 2200, option_type: CE}
            NIFTY20D2413750CE  -> {underlying: NIFTY, strike_price: 13750, option_type: CE}
            M&M21MAR700PE      -> {underlying: M&M, strike_price: 700, option_type: PE}
        """
        if len(symbol) < 4:
            return None

        # 1. option_type = last 2 chars
        option_type = symbol[-2:]
        if option_type not in ('CE', 'PE'):
            return None

        # 2. remainder after removing option type
        remainder = symbol[:-2]

        # Try monthly expiry: UNDERLYING + YYMM + STRIKE
        # Pattern: (non-digit chars, possibly with &) + (2 digits + 3 letters) + (digits for strike)
        monthly = re.match(r'^(.+?)(\d{2}[A-Z]{3})(\d+)$', remainder)
        if monthly:
            underlying = monthly.group(1)
            strike_price = monthly.group(3)
            return {
                'underlying': underlying,
                'strike_price': Decimal(strike_price),
                'option_type': option_type,
            }

        # Try weekly expiry: UNDERLYING + (2 digits + 1 alphanum + 2 digits) + STRIKE
        # e.g., NIFTY + 20D24 + 13750
        weekly = re.match(r'^(.+?)(\d{2}[0-9A-Z]\d{2})(\d+)$', remainder)
        if weekly:
            underlying = weekly.group(1)
            strike_price = weekly.group(3)
            return {
                'underlying': underlying,
                'strike_price': Decimal(strike_price),
                'option_type': option_type,
            }

        # Fallback: split at first digit run, take everything after expiry-like pattern as strike
        strike_match = re.search(r'(\d+)$', remainder)
        if not strike_match:
            return None
        strike_price = strike_match.group(1)
        prefix = remainder[:strike_match.start()]
        # underlying is everything before first digit in prefix
        first_digit = re.search(r'\d', prefix)
        underlying = prefix[:first_digit.start()] if first_digit else prefix

        if not underlying:
            return None

        return {
            'underlying': underlying,
            'strike_price': Decimal(strike_price),
            'option_type': option_type,
        }

    def readFromStatement(self, file_path, user_id):
        """
        Parse a Zerodha FO tradebook Excel file and insert trades into fo_trades table.
        Uses trade_id as PK for automatic dedup on re-upload.
        """
        df = pd.read_excel(file_path, header=14)

        total_rows = 0
        inserted = 0
        duplicates = 0
        skipped = 0
        session = self.db.session

        for _, row in df.iterrows():
            total_rows += 1

            # Skip rows with missing essential data
            if pd.isna(row.get('Symbol')) or pd.isna(row.get('Trade ID')):
                skipped += 1
                continue

            symbol = str(row['Symbol']).strip()
            parsed = self.parse_fo_symbol(symbol)
            if parsed is None:
                self.logger.warning(f"Could not parse FO symbol: {symbol}")
                skipped += 1
                continue

            # Read expiry date from Unnamed: 14 column (reliable source)
            expiry_raw = row.get('Unnamed: 14')
            if pd.isna(expiry_raw):
                # Fallback: try Expiry Date column
                expiry_raw = row.get('Expiry Date')
            if pd.isna(expiry_raw):
                self.logger.warning(f"No expiry date for trade {row['Trade ID']}")
                skipped += 1
                continue

            try:
                expiry_date = pd.to_datetime(expiry_raw).date()
            except (ValueError, TypeError):
                self.logger.warning(f"Could not parse expiry date: {expiry_raw}")
                skipped += 1
                continue

            trade_date_raw = row.get('Trade Date')
            try:
                trade_date = pd.to_datetime(trade_date_raw).date()
            except (ValueError, TypeError):
                self.logger.warning(f"Could not parse trade date: {trade_date_raw}")
                skipped += 1
                continue

            trade_type_str = str(row.get('Trade Type', '')).strip().lower()
            if trade_type_str not in ('buy', 'sell'):
                skipped += 1
                continue

            # Parse order execution time if available
            order_exec_time = None
            order_exec_raw = row.get('Order Execution Time')
            if not pd.isna(order_exec_raw):
                try:
                    order_exec_time = pd.to_datetime(order_exec_raw)
                except (ValueError, TypeError):
                    self.logger.debug(f"Could not parse order execution time: {order_exec_raw}")

            order_id = None
            order_id_raw = row.get('Order ID')
            if not pd.isna(order_id_raw):
                order_id = str(int(order_id_raw)) if isinstance(order_id_raw, float) else str(order_id_raw)

            exchange = str(row.get('Exchange', 'NSE')).strip() if not pd.isna(row.get('Exchange')) else 'NSE'

            fo_trade = FOTrade(
                trade_id=str(int(row['Trade ID'])) if isinstance(row['Trade ID'], float) else str(row['Trade ID']),
                user_id=user_id,
                symbol=symbol,
                underlying=parsed['underlying'],
                expiry_date=expiry_date,
                strike_price=parsed['strike_price'],
                option_type=OptionType[parsed['option_type']],
                trade_date=trade_date,
                trade_type=TradeType[trade_type_str],
                quantity=int(row['Quantity']),
                price=Decimal(str(row['Price'])),
                order_id=order_id,
                order_execution_time=order_exec_time,
                exchange=exchange,
            )

            try:
                with session.begin_nested():
                    session.add(fo_trade)
                    session.flush()
                inserted += 1
            except IntegrityError:
                duplicates += 1

        try:
            session.commit()
        except SQLAlchemyError as e:
            session.rollback()
            self.logger.error(f"Error committing FO trades: {e}")
            return {'total_rows': total_rows, 'inserted': 0, 'duplicates': duplicates, 'skipped': skipped, 'error': str(e)}

        result = {
            'total_rows': total_rows,
            'inserted': inserted,
            'duplicates': duplicates,
            'skipped': skipped,
        }
        self.logger.info(f"FO readFromStatement result: {result}")
        return result

    def get_fo_summary(self, user_id):
        """
        Compute FO P&L summary by grouping trades per contract symbol.
        Returns per-contract and aggregate P&L.
        """
        session = self.db.session
        trades = session.query(FOTrade).filter(
            FOTrade.user_id == user_id
        ).order_by(FOTrade.symbol, FOTrade.trade_date).all()

        if not trades:
            return {
                'totalContracts': 0,
                'netPnL': 0,
                'totalPremiumPaid': 0,
                'totalPremiumReceived': 0,
                'profitableContracts': 0,
                'losingContracts': 0,
                'tradeCount': 0,
                'contracts': [],
            }

        contracts = {}
        for trade in trades:
            sym = trade.symbol
            if sym not in contracts:
                contracts[sym] = {
                    'symbol': sym,
                    'underlying': trade.underlying,
                    'expiry_date': str(trade.expiry_date),
                    'strike_price': float(trade.strike_price),
                    'option_type': trade.option_type.value,
                    'buy_qty': 0,
                    'sell_qty': 0,
                    'buy_value': Decimal('0'),
                    'sell_value': Decimal('0'),
                }

            qty = trade.quantity
            value = trade.price * qty

            if trade.trade_type == TradeType.buy:
                contracts[sym]['buy_qty'] += qty
                contracts[sym]['buy_value'] += value
            else:
                contracts[sym]['sell_qty'] += qty
                contracts[sym]['sell_value'] += value

        contract_list = []
        total_premium_paid = Decimal('0')
        total_premium_received = Decimal('0')
        net_pnl = Decimal('0')
        profitable = 0
        losing = 0

        for sym, c in contracts.items():
            pnl = c['sell_value'] - c['buy_value']
            total_premium_paid += c['buy_value']
            total_premium_received += c['sell_value']
            net_pnl += pnl

            if pnl >= 0:
                profitable += 1
            else:
                losing += 1

            contract_list.append({
                'symbol': c['symbol'],
                'underlying': c['underlying'],
                'expiry_date': c['expiry_date'],
                'strike_price': c['strike_price'],
                'option_type': c['option_type'],
                'buy_qty': c['buy_qty'],
                'sell_qty': c['sell_qty'],
                'buy_value': float(c['buy_value']),
                'sell_value': float(c['sell_value']),
                'pnl': float(pnl),
            })

        # Sort by absolute P&L descending
        contract_list.sort(key=lambda x: abs(x['pnl']), reverse=True)

        return {
            'totalContracts': len(contracts),
            'netPnL': float(net_pnl),
            'totalPremiumPaid': float(total_premium_paid),
            'totalPremiumReceived': float(total_premium_received),
            'profitableContracts': profitable,
            'losingContracts': losing,
            'tradeCount': len(trades),
            'contracts': contract_list,
        }

    def get_fo_trades(self, user_id):
        """Return all individual FO trades ordered by date desc."""
        session = self.db.session
        trades = session.query(FOTrade).filter(
            FOTrade.user_id == user_id
        ).order_by(FOTrade.trade_date.desc()).all()

        return [{
            'tradeId': trade.trade_id,
            'symbol': trade.symbol,
            'underlying': trade.underlying,
            'expiryDate': str(trade.expiry_date),
            'strikePrice': float(trade.strike_price),
            'optionType': trade.option_type.value,
            'tradeDate': str(trade.trade_date),
            'tradeType': trade.trade_type.value,
            'quantity': trade.quantity,
            'price': float(trade.price),
            'value': float(trade.price * trade.quantity),
            'orderId': trade.order_id,
        } for trade in trades]

    def delete_all_fo_trades(self, user_id):
        """Delete all FO trades for a given user."""
        session = self.db.session
        try:
            count = session.query(FOTrade).filter(
                FOTrade.user_id == user_id
            ).delete()
            session.commit()
            return jsonify({"message": f"Successfully deleted all {count} FO trades"}), 200
        except SQLAlchemyError as e:
            session.rollback()
            self.logger.error(f"Error deleting FO trades: {e}")
            return jsonify({"Error": "Failed to delete FO trades"}), 500
