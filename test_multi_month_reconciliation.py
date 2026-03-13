#!/usr/bin/env python3
"""
Multi-month reconciliation test:
1. For each bank+month with BOTH email and statement txns, run email replacement
2. Run cross-instrument reconciliation across the full date range
3. Report matching statistics — what we match, what we miss
"""

import json
import os
import sys
from datetime import date, timedelta
from collections import defaultdict

sys.path.insert(0, os.path.dirname(os.path.abspath(__file__)))
os.environ["ENV"] = "LOCAL"

from dotenv import load_dotenv
load_dotenv()

from flask import g
from sqlalchemy import func, text

USER_ID = "AwKnpKEPmEhQnpc2kJAnsEikOBK2"


def main():
    from run_mail_pipeline import build_app
    app = build_app()

    with app.app_context():
        g.db = app.db
        g.firebase_id = USER_ID
        session = app.db.session

        recon_svc = app.reconciliationService
        from models import Transactions
        from models.statementPeriods import StatementPeriod

        # ── Snapshot BEFORE ──────────────────────────────────────
        print("=" * 70)
        print("BEFORE MULTI-MONTH RECONCILIATION")
        print("=" * 70)

        # Count email vs statement per bank
        results = session.execute(text('''
            SELECT bank, LOWER(source) as src, COUNT(*) as cnt
            FROM transactions WHERE user = :uid
            GROUP BY bank, LOWER(source)
            ORDER BY bank, src
        '''), {'uid': USER_ID}).fetchall()

        before_counts = {}
        for bank, src, cnt in results:
            before_counts[(bank, src)] = cnt
            print(f"  {bank:25s} {src:12s} {cnt:5d}")

        # ── Find overlap months ──────────────────────────────────
        print("\n" + "=" * 70)
        print("IDENTIFYING OVERLAP MONTHS (email + statement both exist)")
        print("=" * 70)

        # Get monthly email txn date ranges per bank
        email_months = session.execute(text('''
            SELECT t.bank, YEAR(t.date) as y, MONTH(t.date) as m,
                   MIN(t.date) as min_date, MAX(t.date) as max_date, COUNT(*) as cnt
            FROM transactions t
            WHERE t.user = :uid AND LOWER(t.source) = 'email'
            GROUP BY t.bank, YEAR(t.date), MONTH(t.date)
            ORDER BY t.bank, y, m
        '''), {'uid': USER_ID}).fetchall()

        stmt_months = session.execute(text('''
            SELECT t.bank, YEAR(t.date) as y, MONTH(t.date) as m,
                   MIN(t.date) as min_date, MAX(t.date) as max_date, COUNT(*) as cnt
            FROM transactions t
            WHERE t.user = :uid AND LOWER(t.source) = 'statement'
            GROUP BY t.bank, YEAR(t.date), MONTH(t.date)
            ORDER BY t.bank, y, m
        '''), {'uid': USER_ID}).fetchall()

        email_set = {(r[0], r[1], r[2]): r[5] for r in email_months}
        stmt_set = {(r[0], r[1], r[2]): r[5] for r in stmt_months}

        overlaps = []
        for key in sorted(email_set.keys()):
            if key in stmt_set:
                bank, y, m = key
                # Use calendar month as period
                period_start = date(y, m, 1)
                if m == 12:
                    period_end = date(y + 1, 1, 1) - timedelta(days=1)
                else:
                    period_end = date(y, m + 1, 1) - timedelta(days=1)
                overlaps.append({
                    'bank': bank,
                    'year': y,
                    'month': m,
                    'email_count': email_set[key],
                    'stmt_count': stmt_set[key],
                    'period_start': period_start,
                    'period_end': period_end,
                })
                print(f"  {bank:25s} {y}-{m:02d}  Email={email_set[key]:4d}  Stmt={stmt_set[key]:4d}")

        if not overlaps:
            print("  No overlaps found — nothing to reconcile")
            return

        # ── Phase 1: Replace email txns with statement data ──────
        print("\n" + "=" * 70)
        print("PHASE 1: REPLACING EMAIL TRANSACTIONS (statement is source of truth)")
        print("=" * 70)

        total_emails_deleted = 0
        periods_recorded = 0

        for ov in overlaps:
            result = recon_svc.replace_email_transactions_with_statement(
                user_id=USER_ID,
                bank=ov['bank'],
                period_start=ov['period_start'],
                period_end=ov['period_end'],
                transaction_count=ov['stmt_count'],
            )
            deleted = result.get('email_transactions_deleted', 0)
            total_emails_deleted += deleted
            if result.get('statement_period_recorded'):
                periods_recorded += 1

            print(f"  {ov['bank']:25s} {ov['year']}-{ov['month']:02d}: "
                  f"deleted {deleted:3d} email txns, "
                  f"keeping {ov['stmt_count']:3d} stmt txns")

        print(f"\n  TOTAL: {total_emails_deleted} email txns deleted, "
              f"{periods_recorded} statement periods recorded")

        # ── Phase 2: Cross-instrument reconciliation ─────────────
        # Find the full date range of our data
        session.expire_all()

        date_range = session.execute(text('''
            SELECT MIN(date), MAX(date)
            FROM transactions
            WHERE user = :uid AND LOWER(source) = 'statement'
        '''), {'uid': USER_ID}).first()

        range_start = date_range[0]
        range_end = date_range[1]

        print("\n" + "=" * 70)
        print(f"PHASE 2: CROSS-INSTRUMENT RECONCILIATION ({range_start} to {range_end})")
        print("=" * 70)

        recon_result = recon_svc.reconcile_transfers(USER_ID, range_start, range_end)
        print(f"\n  Transfer pairs found: {recon_result['pairs_found']}")
        print(f"  Total transfer amount: Rs {recon_result['total_transfer_amount']:,.2f}")
        print(f"  Unmatched debits:  {recon_result['unmatched_debits']}")
        print(f"  Unmatched credits: {recon_result['unmatched_credits']}")

        print(f"\n  Matches by tier:")
        tiers = recon_result.get('matches_by_tier', {})
        print(f"    Tier 1 (exact amt + 3d + keywords):  {tiers.get('tier_1_exact', 0)}")
        print(f"    Tier 2 (exact amt + 7d):             {tiers.get('tier_2_fuzzy_date', 0)}")
        print(f"    Tier 3 (fuzzy amt + 3d + keywords):  {tiers.get('tier_3_fuzzy_amount', 0)}")

        print(f"\n  Instrument pair breakdown:")
        for pair, data in recon_result.get('instrument_pair_breakdown', {}).items():
            print(f"    {pair:50s} {data['count']:3d} pairs  Rs {data['amount']:>12,.2f}")

        # ── Phase 3: Show matched transfer details ───────────────
        from models.transferLinks import TransferLink

        links = session.query(TransferLink).filter(
            TransferLink.user == USER_ID,
            TransferLink.period_start == range_start,
            TransferLink.period_end == range_end,
        ).all()

        if links:
            print(f"\n  Matched transfer details (showing first 20):")
            print(f"  {'From Bank':25s} {'To Bank':25s} {'Amount':>12s} {'Conf':10s} {'Tier':6s}")
            print(f"  {'-'*25} {'-'*25} {'-'*12} {'-'*10} {'-'*6}")
            for link in links[:20]:
                # Get transaction details
                debit = session.query(Transactions).get(link.debit_reference_id)
                credit = session.query(Transactions).get(link.credit_reference_id)
                d_desc = (debit.details[:30] if debit else "?")
                c_desc = (credit.details[:30] if credit else "?")
                print(f"  {link.debit_bank:25s} {link.credit_bank:25s} "
                      f"Rs {float(link.amount):>10,.2f} {link.match_confidence:10s} "
                      f"{link.match_method}")
                print(f"    Debit:  {debit.date if debit else '?'} {d_desc}")
                print(f"    Credit: {credit.date if credit else '?'} {c_desc}")
                print()

        # ── Phase 4: Zero-sum report ─────────────────────────────
        print("=" * 70)
        print(f"PHASE 3: ZERO-SUM VERIFICATION ({range_start} to {range_end})")
        print("=" * 70)

        zs = recon_svc.compute_zero_sum_report(USER_ID, range_start, range_end)

        print(f"\n  Statement coverage: {', '.join(zs['statement_coverage'])}")
        print(f"\n  Per-instrument summary:")
        print(f"  {'Bank':25s} {'Net':>12s} {'Debits':>12s} {'Credits':>12s} "
              f"{'Link Out':>12s} {'Link In':>12s} {'Residual':>12s} {'Txns':>5s}")
        print(f"  {'-'*25} {'-'*12} {'-'*12} {'-'*12} {'-'*12} {'-'*12} {'-'*12} {'-'*5}")

        for bank, data in sorted(zs['per_instrument'].items()):
            print(f"  {bank:25s} {data['net']:>12,.2f} {data['debits']:>12,.2f} "
                  f"{data['credits']:>12,.2f} {data['linked_out']:>12,.2f} "
                  f"{data['linked_in']:>12,.2f} {data['residual']:>12,.2f} "
                  f"{data['txn_count']:>5d}")

        print(f"\n  Transfer balance check: {zs['summary']['transfer_balance_check']:.2f} "
              f"(should be 0.00)")
        print(f"  Net external flow: Rs {zs['summary']['net_external_flow']:,.2f}")
        print(f"  Total linked transfers: Rs {zs['summary']['total_transfers']:,.2f} "
              f"({zs['summary']['transfer_pairs_count']} pairs)")

        # ── Phase 5: Snapshot AFTER ──────────────────────────────
        session.expire_all()

        print("\n" + "=" * 70)
        print("AFTER MULTI-MONTH RECONCILIATION")
        print("=" * 70)

        results = session.execute(text('''
            SELECT bank, LOWER(source) as src, COUNT(*) as cnt
            FROM transactions WHERE user = :uid
            GROUP BY bank, LOWER(source)
            ORDER BY bank, src
        '''), {'uid': USER_ID}).fetchall()

        for bank, src, cnt in results:
            before = before_counts.get((bank, src), 0)
            delta = cnt - before
            delta_str = f"({delta:+d})" if delta != 0 else ""
            print(f"  {bank:25s} {src:12s} {cnt:5d} {delta_str}")

        # Statement periods
        periods = session.query(StatementPeriod).filter_by(
            user=USER_ID
        ).order_by(StatementPeriod.bank, StatementPeriod.period_start).all()

        print(f"\n  Statement Periods ({len(periods)} total):")
        for p in periods:
            print(f"    {p.bank:25s} {p.period_start} to {p.period_end} "
                  f"(txns={p.transaction_count}, emails_replaced={p.email_txns_replaced})")


if __name__ == "__main__":
    main()
