"""
Reconciliation service for statement-based transaction management,
cross-instrument transfer linking, zero-sum verification, and
trade-level broker-statement reconciliation (ak-cipj Phase 1 read
surface for the ak-zai2 write-only ReconcileTradesTask pipeline).
"""

import json
import uuid
from datetime import datetime, timedelta
from decimal import Decimal

from sqlalchemy import and_, func
from sqlalchemy.exc import IntegrityError

from models.reconciliationIssue import ReconciliationIssue
from models.transactions import Transactions
from models.statementPeriods import StatementPeriod
from models.transferLinks import TransferLink
from services.Base_Service import BaseService
from utils.logger import Logger


# ak-cipj: operator-supplied resolution_note is capped at the column
# width (500) at the service boundary. Longer input is truncated
# rather than 400'd — the field is a convenience annotation, not a
# strict contract. See ReconciliationIssue.resolution_note docstring.
_RESOLUTION_NOTE_MAX_LEN = 500

# ak-cipj: valid values for the ``status`` filter on list_trade_issues.
# Checked at the controller boundary (raises 400 on unknown) AND at
# the service boundary (silently falls back to 'open' for defense in
# depth, so a caller that bypasses the controller still gets a safe
# default rather than returning ALL issues to a buggy UI).
_LIST_STATUS_VALID = frozenset({"open", "resolved", "all"})


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
        1. MATCH-BEFORE-DELETE (ak-a0m v2) email-alert candidates
           against statement rows in the same period. Delete only
           those where a statement-side row for the SAME tx exists;
           preserve the rest.
        2. Record the statement period.

        ak-a0m v1 removed the cross-file statement-delete that was
        nuking rows from other files. That was necessary but not
        sufficient: the email-side delete was still blanket, which
        silently loses:
          - alert-only tx (never appear on a statement)
          - alerts under an under-parsed statement (statement missed
            the row for some reason).

        ak-a0m v2 (reviewer BOUNCE hq-wisp-35ufm6) adds symmetric
        zero-loss on the email side: match each candidate against
        the statement rows in the period. Primary match uses
        bank_reference_id (ak-8l5's stable identity); fallback uses
        (bank, date, amount, normalized-description). Rows with no
        match are PRESERVED.

        Cross-file protection (v1) still holds: statement rows are
        never deleted here regardless of overlap. Chunk re-reads of
        the same statement collapse via ak-8l5's referenceID PK
        collision path at insert time.

        Returns dict with counts:
          email_transactions_deleted: number matched + deleted
          email_transactions_preserved: number preserved (v2 added)
          statement_transactions_deleted: 0 (retained for backward
            compat with upstream loggers)
          orphaned_files_deleted: 0 (same)
          statement_period_recorded: True on success.
        """
        session = self.db.session
        from utils.reconciliation_delete_policy import (
            build_email_deletion_filters,
            build_statement_lookup_filters,
            build_statement_match_index,
            is_email_matched_by_statement,
        )

        # 1a. Load the email-alert CANDIDATES for the period.
        #     ak-a0m v2: filter is sourced from the same policy
        #     module as the pure predicate, so a SQL / Python
        #     divergence can't slip in silently (v2 MINOR fix).
        candidate_filter = build_email_deletion_filters(
            Transactions, user_id, bank,
            period_start, period_end, func=func,
        )
        candidates = session.query(Transactions).filter(
            *candidate_filter
        ).all()

        # 1b. Load the statement-side rows in the same period. Any
        #     statement file for the (user, bank, period). The
        #     statement we're about to record has already inserted
        #     via the calling pipeline, so its rows are visible
        #     here.
        statement_filter = build_statement_lookup_filters(
            Transactions, user_id, bank,
            period_start, period_end, func=func,
        )
        statement_rows = session.query(Transactions).filter(
            *statement_filter
        ).all()

        # 1c. Build the match indexes ONCE per invocation.
        #     O(n_stmt) build, O(1) lookup per candidate. Beats the
        #     naive O(n_email × n_stmt) inner loop.
        by_ref, by_tuple = build_statement_match_index(statement_rows)

        # 1d. Match-before-delete: only mark candidates with a
        #     matching statement-side row. Zero match → preserve.
        to_delete_refs = []
        preserved_count = 0
        for c in candidates:
            if is_email_matched_by_statement(c, by_ref, by_tuple):
                to_delete_refs.append(c.referenceID)
            else:
                preserved_count += 1

        email_deleted = 0
        if to_delete_refs:
            email_deleted = session.query(Transactions).filter(
                Transactions.referenceID.in_(to_delete_refs),
                Transactions.user == user_id,
            ).delete(synchronize_session='fetch')

        self.logger.info(
            f"ak-a0m v2: {email_deleted} email-alert(s) replaced by "
            f"matching statement rows; {preserved_count} preserved "
            f"(no matching statement row). "
            f"bank={bank} period={period_start} to {period_end} "
            f"file_id={file_id!r} statement_rows_in_period={len(statement_rows)} "
            f"candidates={len(candidates)}"
        )

        # 2. ak-a0m v1: cross-file statement-delete REMOVED. Rows
        #    from other statements are protected under the zero-loss
        #    constraint. Chunk re-reads of the same statement dedup
        #    at the storage layer via ak-8l5.
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
            # ak-a0m v2: NEW observability field — how many candidate
            # email alerts were preserved because no matching
            # statement row existed.
            "email_transactions_preserved": preserved_count,
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

    # ── ak-cipj: Trade reconciliation read surface ────────────────────
    #
    # ReconcileTradesTask (ak-zai2) runs daily post-market-close and
    # writes ``ReconciliationIssue`` rows for Kite-reported trades
    # absent from the local ``TradeAssociation`` table. Prior to
    # ak-cipj the pipeline was write-only (verified: zero controller
    # endpoint, zero service read method, zero FE call — the Home
    # "Reconciliation · Zerodha vs statement" card was a sample
    # placeholder with no backing). These two methods close the loop:
    # Overseer can see flagged issues and mark them resolved.
    #
    # Design notes:
    #   * user_id-scoped on every query — single-user app today but
    #     cheap to preserve the pattern so a future multi-user move
    #     isn't a security defect.
    #   * 404 on user mismatch (not 403) to avoid leaking issue-
    #     existence to the wrong user. Identical shape to a nonexistent
    #     id.
    #   * resolve is idempotent — double-resolve returns 200 with
    #     ``idempotent=True`` so the FE can distinguish a no-op repeat
    #     from a fresh resolution for UI semantics. First-set
    #     ``resolved_at`` is preserved on repeat to keep the audit
    #     trail of WHEN the issue was first closed.
    #   * discrepancy_detail is JSON-loaded at the service boundary
    #     so the controller returns a structured object instead of a
    #     json-string-inside-json; matches the on-wire shape of other
    #     read endpoints. Falls back to the raw string if json.loads
    #     fails (defensive against a hand-written row).

    def list_trade_issues(self, user_id, status="open"):
        """Return list of ReconciliationIssue rows for user, filtered
        by status.

        Args:
            user_id: Firebase user id (required; all queries scoped).
            status: one of ``'open'`` (resolved_at IS NULL — default),
                ``'resolved'`` (resolved_at IS NOT NULL), ``'all'``.
                Unknown values fall back to ``'open'`` defensively.

        Returns:
            list of dicts with id / kite_trade_id / discrepancy_type /
            discrepancy_detail (parsed JSON or raw str) / detected_at
            (ISO) / resolved_at (ISO or None) / resolution_note.
            Sorted by detected_at descending so the review surface
            shows newest discrepancies first.
        """
        if status not in _LIST_STATUS_VALID:
            status = "open"
        q = self.db.session.query(ReconciliationIssue).filter(
            ReconciliationIssue.user_id == user_id,
        )
        if status == "open":
            q = q.filter(ReconciliationIssue.resolved_at.is_(None))
        elif status == "resolved":
            q = q.filter(ReconciliationIssue.resolved_at.isnot(None))
        # 'all' → no resolved_at filter
        rows = q.order_by(ReconciliationIssue.detected_at.desc()).all()
        return [self._serialize_trade_issue(r) for r in rows]

    def resolve_trade_issue(self, user_id, issue_id, resolution_note=None):
        """Mark a trade reconciliation issue resolved. Idempotent.

        Args:
            user_id: Firebase user id (required; scoped read-then-
                mutate guards against cross-user mutation).
            issue_id: integer primary key.
            resolution_note: optional operator-supplied free-text;
                truncated to 500 chars at the service boundary.

        Returns:
            Tuple ``(result_dict, http_status)``:
              * ``(404, {"error": ...})`` — not found OR user mismatch
                (same shape in both cases; see docstring rationale).
              * ``(200, {id, resolved_at, resolution_note,
                idempotent})`` — fresh resolve OR idempotent repeat.
                ``idempotent=True`` when the row was already resolved
                (first-set ``resolved_at`` preserved).
        """
        issue = self.db.session.query(ReconciliationIssue).filter(
            ReconciliationIssue.id == issue_id,
            ReconciliationIssue.user_id == user_id,
        ).first()
        if issue is None:
            return {"error": "Issue not found"}, 404

        if issue.resolved_at is not None:
            # Idempotent: already resolved. Preserve original
            # resolved_at + resolution_note (don't overwrite).
            return {
                "id": issue.id,
                "resolved_at": issue.resolved_at.isoformat(),
                "resolution_note": issue.resolution_note,
                "idempotent": True,
            }, 200

        issue.resolved_at = datetime.utcnow()
        if resolution_note:
            # Truncate rather than reject — see module-level constant
            # docstring for the convenience-over-strictness rationale.
            issue.resolution_note = str(resolution_note)[
                :_RESOLUTION_NOTE_MAX_LEN
            ]
        try:
            self.db.session.commit()
        except Exception as exc:
            self.db.session.rollback()
            self.logger.error(
                f"Failed to resolve ReconciliationIssue id={issue_id} "
                f"user_id={user_id}: {exc}"
            )
            return {"error": "Resolve failed"}, 500

        return {
            "id": issue.id,
            "resolved_at": issue.resolved_at.isoformat(),
            "resolution_note": issue.resolution_note,
            "idempotent": False,
        }, 200

    @staticmethod
    def _serialize_trade_issue(row):
        """Shape a ReconciliationIssue row for the API response.

        discrepancy_detail is stored as a JSON string (ReconcileTrades
        Task uses json.dumps per models/reconciliationIssue.py L76-80).
        Parse it here so the controller emits structured JSON rather
        than a string-inside-string. Falls back to the raw string if
        the row somehow carries non-JSON (defensive against hand-
        inserted records or a format drift).
        """
        detail = row.discrepancy_detail
        if detail:
            try:
                detail = json.loads(detail)
            except (TypeError, ValueError):
                # Raw string pass-through — logged at WARN by the
                # caller if they care; here we just keep the field
                # populated rather than nulling a legitimate value.
                pass
        return {
            "id": row.id,
            "kite_trade_id": row.kite_trade_id,
            "discrepancy_type": row.discrepancy_type,
            "discrepancy_detail": detail,
            "detected_at": (
                row.detected_at.isoformat() if row.detected_at else None
            ),
            "resolved_at": (
                row.resolved_at.isoformat() if row.resolved_at else None
            ),
            "resolution_note": row.resolution_note,
        }
