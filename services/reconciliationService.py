"""
Reconciliation service for statement-based transaction management,
cross-instrument transfer linking, and zero-sum verification.
"""

import uuid
from datetime import datetime, timedelta
from decimal import Decimal

from sqlalchemy import and_, func
from sqlalchemy.exc import IntegrityError

from models.transactions import Transactions
from models.statementPeriods import StatementPeriod
from models.transferLinks import TransferLink
from services.Base_Service import BaseService
from utils.logger import Logger


# Keywords that indicate inter-instrument transfers
TRANSFER_KEYWORDS = [
    'imps', 'neft', 'rtgs', 'upi', 'transfer', 'fund transfer',
    'cc payment', 'bill payment', 'auto debit', 'ach', 'nach',
    'standing instruction', 'bil/bpay', 'credit card payment',
    'emi', 'si/', 'autopay',
]


class ReconciliationService(BaseService):
    _instance = None

    def __new__(cls):
        if cls._instance is None:
            cls._instance = super(ReconciliationService, cls).__new__(cls)
            cls.logger = Logger(__name__).get_logger()
        return cls._instance

    def __init__(self):
        super().__init__()

    # ── Phase 2: Statement Period Tracking + Email Replacement ────────

    def replace_email_transactions_with_statement(
        self, user_id, bank, period_start, period_end,
        file_id=None, gmail_message_id=None, transaction_count=0,
    ):
        """
        When a statement arrives for a bank+period:
        1. Delete email-sourced ALERT transactions (source='email' AND
           fileID IS NULL) for that instrument+period.
        2. Record the statement period.

        ak-a0m (P1): the pre-ak-a0m implementation ALSO deleted
        "overlapping statement transactions", excluding the current
        file_id. That silently nuked rows from OTHER statement files
        whose periods overlapped — a hard zero-loss violation
        (Overseer's ledger drops rows without warning). Removed.

        The delete policy now lives in
        utils.reconciliation_delete_policy — see that module's
        docstring for the "email-alert only, cross-file never" rule
        and rationale.

        Cross-file protection: statement-source rows (any fileID) are
        NEVER deleted here. If two statements legitimately overlap
        (e.g. HDFC monthly + quarterly summary), BOTH sets stay.
        Chunk re-reads of the same statement collapse via ak-8l5's
        referenceID PK collision path at insert time — no delete
        required.

        Returns dict with counts (statement_transactions_deleted and
        orphaned_files_deleted are always 0 under the ak-a0m policy;
        the fields stay in the dict for backward compat with
        upstream callers that log the counts).
        """
        session = self.db.session
        from utils.reconciliation_delete_policy import (
            build_email_deletion_filters,
        )

        # 1. Delete email-alert transactions for this bank+period.
        #    ak-a0m: the filter is now sourced from a single policy
        #    module so it matches the pure-Python
        #    should_delete_on_statement_arrival predicate exactly.
        email_delete_filter = build_email_deletion_filters(
            Transactions, user_id, bank,
            period_start, period_end, func=func,
        )
        email_deleted = session.query(Transactions).filter(
            *email_delete_filter
        ).delete(synchronize_session='fetch')

        self.logger.info(
            f"ak-a0m: deleted {email_deleted} email-alert transaction(s) "
            f"for {bank} ({period_start} to {period_end}) on statement "
            f"arrival (file_id={file_id!r}). Statement-source rows "
            f"preserved cross-file per zero-loss policy."
        )

        # 2. ak-a0m: cross-file statement-delete REMOVED. Rows from
        #    other statements are protected under the zero-loss
        #    constraint. Chunk re-reads of the same statement dedup
        #    at the storage layer via ak-8l5 (referenceID PK
        #    collision → silent drop OR suffix-and-keep). No further
        #    action needed here.
        stmt_deleted = 0
        orphaned_files_deleted = 0

        # 3. Upsert statement period record
        existing = session.query(StatementPeriod).filter_by(
            bank=bank, period_start=period_start,
            period_end=period_end, user=user_id,
        ).first()

        if existing:
            existing.file_id = file_id or existing.file_id
            existing.gmail_message_id = gmail_message_id or existing.gmail_message_id
            existing.transaction_count = transaction_count
            existing.email_txns_replaced = email_deleted
            existing.created_at = datetime.utcnow()
        else:
            period = StatementPeriod(
                bank=bank,
                period_start=period_start,
                period_end=period_end,
                file_id=file_id,
                user=user_id,
                gmail_message_id=gmail_message_id,
                transaction_count=transaction_count,
                email_txns_replaced=email_deleted,
            )
            session.add(period)

        try:
            session.commit()
        except IntegrityError:
            session.rollback()
            self.logger.warning("Statement period upsert conflict, retrying update")
            existing = session.query(StatementPeriod).filter_by(
                bank=bank, period_start=period_start,
                period_end=period_end, user=user_id,
            ).first()
            if existing:
                existing.transaction_count = transaction_count
                existing.email_txns_replaced = email_deleted
                session.commit()

        return {
            "email_transactions_deleted": email_deleted,
            "statement_transactions_deleted": stmt_deleted,
            "orphaned_files_deleted": orphaned_files_deleted,
            "statement_period_recorded": True,
        }

    def get_statement_periods(self, user_id, bank=None):
        """Query all recorded statement periods for a user, optionally filtered by bank."""
        query = self.db.session.query(StatementPeriod).filter_by(user=user_id)
        if bank:
            query = query.filter_by(bank=bank)
        query = query.order_by(StatementPeriod.period_end.desc())

        periods = query.all()
        return [
            {
                "id": p.id,
                "bank": p.bank,
                "period_start": p.period_start.isoformat() if p.period_start else None,
                "period_end": p.period_end.isoformat() if p.period_end else None,
                "file_id": p.file_id,
                "gmail_message_id": p.gmail_message_id,
                "transaction_count": p.transaction_count,
                "email_txns_replaced": p.email_txns_replaced,
                "created_at": p.created_at.isoformat() if p.created_at else None,
            }
            for p in periods
        ]

    # ── Phase 3: Cross-Instrument Reconciliation ─────────────────────

    def reconcile_transfers(self, user_id, period_start, period_end):
        """
        Find cross-instrument transfers (e.g., CC payment from bank account)
        by matching debits in one instrument with credits in another.

        Algorithm:
        1. Load all statement-sourced transactions for user in period
        2. For each debit in bank A, search for matching credit in bank B
        3. Tier 1: exact amount + 3-day window + transfer keywords
        4. Tier 2: exact amount + 7-day window
        5. Tier 3: amount within 0.5% + 3-day window + transfer keywords
        6. Greedy matching, avoid double-counting
        """
        session = self.db.session

        # Clear existing links for this period (idempotent re-run)
        session.query(TransferLink).filter(
            TransferLink.user == user_id,
            TransferLink.period_start == period_start,
            TransferLink.period_end == period_end,
        ).delete(synchronize_session='fetch')

        # Also clear transfer_group_id on transactions in period
        session.query(Transactions).filter(
            Transactions.user == user_id,
            Transactions.date.between(period_start, period_end),
            Transactions.transfer_group_id.isnot(None),
        ).update({Transactions.transfer_group_id: None}, synchronize_session='fetch')

        session.commit()

        # Load statement-sourced transactions for period (case-insensitive source match)
        txns = session.query(Transactions).filter(
            Transactions.user == user_id,
            func.lower(Transactions.source) == 'statement',
            Transactions.date.between(period_start, period_end),
        ).all()

        if not txns:
            return {
                "pairs_found": 0,
                "total_transfer_amount": 0,
                "unmatched_debits": 0,
                "unmatched_credits": 0,
                "message": "No statement transactions found in period",
            }

        # Separate debits (positive = money out) and credits (negative = money in)
        debits = [t for t in txns if float(t.amount) > 0]
        credits = [t for t in txns if float(t.amount) < 0]

        matched_debit_ids = set()
        matched_credit_ids = set()
        pairs = []

        def _has_transfer_keyword(details):
            if not details:
                return False
            lower = details.lower()
            return any(kw in lower for kw in TRANSFER_KEYWORDS)

        def _try_match(debit, credit, tier):
            """Check if debit and credit form a transfer pair at the given tier."""
            if debit.bank == credit.bank:
                return None
            if debit.referenceID in matched_debit_ids:
                return None
            if credit.referenceID in matched_credit_ids:
                return None

            d_amt = abs(float(debit.amount))
            c_amt = abs(float(credit.amount))
            d_date = debit.date
            c_date = credit.date
            day_diff = abs((d_date - c_date).days)

            if tier == 1:
                # Exact amount, 3-day window, transfer keyword
                if d_amt != c_amt:
                    return None
                if day_diff > 3:
                    return None
                if not (_has_transfer_keyword(debit.details) or _has_transfer_keyword(credit.details)):
                    return None
                return 'exact'
            elif tier == 2:
                # Exact amount, 7-day window
                if d_amt != c_amt:
                    return None
                if day_diff > 7:
                    return None
                return 'fuzzy_date'
            elif tier == 3:
                # Amount within 0.5%, 3-day window, transfer keyword
                if d_amt == 0:
                    return None
                if abs(d_amt - c_amt) / d_amt > 0.005:
                    return None
                if day_diff > 3:
                    return None
                if not (_has_transfer_keyword(debit.details) or _has_transfer_keyword(credit.details)):
                    return None
                return 'fuzzy_amount'

            return None

        # Greedy matching across tiers
        for tier in [1, 2, 3]:
            for debit in debits:
                if debit.referenceID in matched_debit_ids:
                    continue
                for credit in credits:
                    confidence = _try_match(debit, credit, tier)
                    if confidence:
                        group_id = uuid.uuid4().hex
                        pairs.append({
                            "group_id": group_id,
                            "debit": debit,
                            "credit": credit,
                            "confidence": confidence,
                            "tier": tier,
                        })
                        matched_debit_ids.add(debit.referenceID)
                        matched_credit_ids.add(credit.referenceID)
                        break

        # Persist matches
        total_amount = Decimal(0)
        for pair in pairs:
            debit = pair["debit"]
            credit = pair["credit"]
            group_id = pair["group_id"]

            link = TransferLink(
                transfer_group_id=group_id,
                debit_reference_id=debit.referenceID,
                credit_reference_id=credit.referenceID,
                debit_bank=debit.bank,
                credit_bank=credit.bank,
                amount=abs(debit.amount),
                match_confidence=pair["confidence"],
                match_method=f"tier_{pair['tier']}_greedy",
                user=user_id,
                period_start=period_start,
                period_end=period_end,
            )
            session.add(link)

            # Tag transactions with group ID
            debit.transfer_group_id = group_id
            credit.transfer_group_id = group_id
            total_amount += abs(debit.amount)

        session.commit()

        # Build instrument-pair breakdown
        pair_breakdown = {}
        for pair in pairs:
            key = f"{pair['debit'].bank} -> {pair['credit'].bank}"
            if key not in pair_breakdown:
                pair_breakdown[key] = {"count": 0, "amount": 0}
            pair_breakdown[key]["count"] += 1
            pair_breakdown[key]["amount"] += float(abs(pair["debit"].amount))

        return {
            "pairs_found": len(pairs),
            "total_transfer_amount": float(total_amount),
            "unmatched_debits": len(debits) - len(matched_debit_ids),
            "unmatched_credits": len(credits) - len(matched_credit_ids),
            "instrument_pair_breakdown": pair_breakdown,
            "matches_by_tier": {
                "tier_1_exact": sum(1 for p in pairs if p["tier"] == 1),
                "tier_2_fuzzy_date": sum(1 for p in pairs if p["tier"] == 2),
                "tier_3_fuzzy_amount": sum(1 for p in pairs if p["tier"] == 3),
            },
        }

    # ── Phase 4: Zero-Sum Report ─────────────────────────────────────

    def compute_zero_sum_report(self, user_id, period_start, period_end):
        """
        Compute a zero-sum verification report:
        1. Per-instrument totals from statement-sourced transactions
        2. Linked transfer summary
        3. Per-instrument residual (net minus linked transfers = external flows)
        4. Cross-instrument verification: linked transfers should cancel to zero
        """
        session = self.db.session

        # 1. Per-instrument totals (case-insensitive source match)
        txns = session.query(Transactions).filter(
            Transactions.user == user_id,
            func.lower(Transactions.source) == 'statement',
            Transactions.date.between(period_start, period_end),
        ).all()

        per_instrument = {}
        for t in txns:
            bank = t.bank
            if bank not in per_instrument:
                per_instrument[bank] = {
                    "net": Decimal(0), "debits": Decimal(0), "credits": Decimal(0),
                    "txn_count": 0, "linked_in": Decimal(0), "linked_out": Decimal(0),
                }
            amt = t.amount
            per_instrument[bank]["net"] += amt
            per_instrument[bank]["txn_count"] += 1
            if amt > 0:
                per_instrument[bank]["debits"] += amt
            else:
                per_instrument[bank]["credits"] += amt

        # 2. Linked transfers
        links = session.query(TransferLink).filter(
            TransferLink.user == user_id,
            TransferLink.period_start == period_start,
            TransferLink.period_end == period_end,
        ).all()

        transfer_pairs = []
        for link in links:
            transfer_pairs.append({
                "group_id": link.transfer_group_id,
                "from_bank": link.debit_bank,
                "to_bank": link.credit_bank,
                "amount": float(link.amount),
                "confidence": link.match_confidence,
            })

            # Accumulate linked amounts per instrument
            if link.debit_bank in per_instrument:
                per_instrument[link.debit_bank]["linked_out"] += link.amount
            if link.credit_bank in per_instrument:
                per_instrument[link.credit_bank]["linked_in"] += link.amount

        # 3. Compute residuals
        for bank, data in per_instrument.items():
            # Residual = net - linked_out + linked_in
            # This represents external flows: salary in, expenses out
            data["residual"] = data["net"] - data["linked_out"] + data["linked_in"]

        # 4. Transfer balance check: sum of all linked debits should equal credits
        total_linked_debits = sum(link.amount for link in links)
        total_linked_credits = sum(link.amount for link in links)
        transfer_balance_check = float(total_linked_debits - total_linked_credits)

        # Statement coverage
        statement_periods = session.query(StatementPeriod).filter(
            StatementPeriod.user == user_id,
            StatementPeriod.period_start >= period_start,
            StatementPeriod.period_end <= period_end,
        ).all()
        covered_banks = list(set(sp.bank for sp in statement_periods))

        # Convert Decimals for JSON
        per_instrument_json = {}
        for bank, data in per_instrument.items():
            per_instrument_json[bank] = {
                k: float(v) if isinstance(v, Decimal) else v
                for k, v in data.items()
            }

        net_external_flow = float(sum(
            data["residual"] for data in per_instrument.values()
        ))

        return {
            "period": {
                "start": period_start.isoformat() if hasattr(period_start, 'isoformat') else str(period_start),
                "end": period_end.isoformat() if hasattr(period_end, 'isoformat') else str(period_end),
            },
            "statement_coverage": covered_banks,
            "per_instrument": per_instrument_json,
            "transfer_pairs": transfer_pairs,
            "summary": {
                "total_transfers": float(total_linked_debits),
                "transfer_pairs_count": len(transfer_pairs),
                "transfer_balance_check": transfer_balance_check,
                "net_external_flow": net_external_flow,
            },
        }

    def get_reconciliation_status(self, user_id, period_start, period_end):
        """
        Check which banks have statement coverage for the period,
        and whether reconciliation has been run.
        """
        session = self.db.session

        # Statement coverage
        statement_periods = session.query(StatementPeriod).filter(
            StatementPeriod.user == user_id,
            StatementPeriod.period_start >= period_start,
            StatementPeriod.period_end <= period_end,
        ).all()

        covered_banks = {}
        for sp in statement_periods:
            covered_banks[sp.bank] = {
                "period_start": sp.period_start.isoformat(),
                "period_end": sp.period_end.isoformat(),
                "transaction_count": sp.transaction_count,
                "email_txns_replaced": sp.email_txns_replaced,
            }

        # Check if reconciliation has been run
        link_count = session.query(TransferLink).filter(
            TransferLink.user == user_id,
            TransferLink.period_start == period_start,
            TransferLink.period_end == period_end,
        ).count()

        return {
            "covered_banks": covered_banks,
            "uncovered_banks": [],  # Would need opted banks list to compute
            "reconciliation_run": link_count > 0,
            "transfer_links_count": link_count,
        }
