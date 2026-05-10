#!/usr/bin/env python3
"""
Cleanup phantom EPF rows in investmentSnapshots (and conditionally
investmentHistory) — companion to the hq-w71a EPF auto-enumeration code fix
in services/EPFService.py.

Background: EPFService.calculateTransactionTable used to project the last
real EPF deposit forward to the current calendar month, generating
"GENERATED ROW" entries in the in-memory transactions list. The phantoms
themselves were never persisted to depositSecurities, but the inflated
`net` / `netProfit` they produced flowed into:

  1. investmentSnapshots — daily 11PM IST writes by InvestmentSnapshotTask
  2. investmentHistory   — 24h cadence writes by InvestmentHistoryTask
                           (caveat: model declares Integer columns but the
                           writer puts JSON strings, so writes may always
                           have been failing or the model definition is
                           stale; this script probes at runtime)

This script runs ONCE post-deploy of the code fix. Idempotent — running
twice has no effect after the first run.

Heuristic: phantom iff `date > MAX(date) FROM depositSecurities WHERE
securityType='EPF' AND userID=user`. Multi-user safe (Akkountant is
single-user today; per-user iteration costs nothing).

Usage:
    python3 cleanup_phantom_epf_snapshots.py            # ACT — does the cleanup
    python3 cleanup_phantom_epf_snapshots.py --dry-run  # Print what would change

Refs: bead hq-w71a.
"""

import argparse
import sys
import os

sys.path.insert(0, os.path.dirname(os.path.abspath(__file__)))
from dotenv import load_dotenv
load_dotenv()

import mysql.connector


# Same shape as cleanup_duplicates.py / migrate_processed_emails.py
DB_CONFIG = {
    "host": "localhost",
    "user": "root",
    "password": "iamvidish",
    "database": "akkountantv2",
}


# investmentHistory.epf neutral value depending on column type. The writer
# (InvestmentHistoryTask) puts a JSON dict like
# {"totalValue":"...","currentValue":"...","changePercent":"...","changeAmount":"..."}
# so for VARCHAR/TEXT/JSON columns we replace with a zeroed version of that
# shape; for INT we just set 0.
_NEUTRAL_JSON = (
    '{"totalValue":"0","currentValue":"0",'
    '"changePercent":"0","changeAmount":"0"}'
)


def probe_investmentHistory_column(cursor) -> str:
    """Return the column type of investmentHistory.epf, or '' if missing.

    Resolves Lead's Q3 from the proposal mail at runtime — we don't know
    whether the model definition (Integer) matches the live DB (might be
    VARCHAR/TEXT to actually accommodate the JSON-string writes).
    """
    cursor.execute("SHOW COLUMNS FROM investmentHistory LIKE 'epf'")
    row = cursor.fetchone()
    if not row:
        return ""
    # row[0]=field, row[1]=type, row[2]=null, ...
    return str(row[1]).lower()


def neutral_epf_value(col_type: str):
    """Pick the neutral value to write into investmentHistory.epf based on
    the live column type."""
    if col_type.startswith("int") or col_type.startswith("bigint") \
       or col_type.startswith("smallint") or col_type.startswith("mediumint") \
       or col_type.startswith("tinyint"):
        return 0
    if col_type.startswith(("varchar", "text", "char", "json", "longtext", "mediumtext")):
        return _NEUTRAL_JSON
    # Decimal / numeric / unknown — fall back to 0; MySQL will coerce.
    return 0


def get_user_last_epf_date(cursor):
    """Return [(userID, last_epf_date), ...] for every user with at least
    one EPF row in depositSecurities."""
    cursor.execute("""
        SELECT userID, MAX(date)
        FROM depositSecurities
        WHERE securityType = 'EPF'
        GROUP BY userID
    """)
    return cursor.fetchall()


def count_phantom_snapshots(cursor, user_id, last_real_date):
    cursor.execute("""
        SELECT COUNT(*)
        FROM investmentSnapshots
        WHERE user = %s
          AND investment_type = 'EPF'
          AND date > %s
    """, (user_id, last_real_date))
    return cursor.fetchone()[0]


def delete_phantom_snapshots(cursor, user_id, last_real_date):
    cursor.execute("""
        DELETE FROM investmentSnapshots
        WHERE user = %s
          AND investment_type = 'EPF'
          AND date > %s
    """, (user_id, last_real_date))
    return cursor.rowcount


def count_phantom_history(cursor, user_id, last_real_date):
    cursor.execute("""
        SELECT COUNT(*)
        FROM investmentHistory
        WHERE user = %s
          AND date > %s
    """, (user_id, last_real_date))
    return cursor.fetchone()[0]


def neutralize_phantom_history(cursor, user_id, last_real_date, neutral_value):
    """Set the epf column to a neutral value for stale rows. Preserves
    other-asset values in the row."""
    cursor.execute("""
        UPDATE investmentHistory
        SET epf = %s
        WHERE user = %s
          AND date > %s
    """, (neutral_value, user_id, last_real_date))
    return cursor.rowcount


def main():
    parser = argparse.ArgumentParser(description=__doc__)
    parser.add_argument(
        "--dry-run", action="store_true",
        help="Print what would change without modifying the DB",
    )
    args = parser.parse_args()

    print(f"[cleanup_phantom_epf] dry_run={args.dry_run}")

    conn = mysql.connector.connect(**DB_CONFIG)
    cursor = conn.cursor()

    try:
        users = get_user_last_epf_date(cursor)
        if not users:
            print("[cleanup_phantom_epf] No EPF DepositSecurities rows found — nothing to do.")
            return 0

        # ── investmentHistory column-type probe (runtime branching) ──
        col_type = probe_investmentHistory_column(cursor)
        if not col_type:
            print("[cleanup_phantom_epf] WARN: investmentHistory.epf column not found — "
                  "skipping investmentHistory cleanup entirely.")
            history_action = None
        else:
            print(f"[cleanup_phantom_epf] investmentHistory.epf column type: {col_type}")
            history_action = neutral_epf_value(col_type)
            print(f"[cleanup_phantom_epf]   → will set stale rows' epf to: {history_action!r}")

        total_snapshots_deleted = 0
        total_history_neutralized = 0

        for user_id, last_real_date in users:
            print(f"\n[user={user_id}] last real EPF date: {last_real_date}")

            # investmentSnapshots cleanup
            snap_count = count_phantom_snapshots(cursor, user_id, last_real_date)
            print(f"  investmentSnapshots phantoms (date > {last_real_date}): {snap_count}")
            if snap_count > 0:
                if args.dry_run:
                    print(f"  [DRY-RUN] would DELETE {snap_count} investmentSnapshots EPF rows")
                else:
                    deleted = delete_phantom_snapshots(cursor, user_id, last_real_date)
                    total_snapshots_deleted += deleted
                    print(f"  DELETED {deleted} investmentSnapshots EPF rows")

            # investmentHistory cleanup (gated on the runtime probe)
            if history_action is not None:
                hist_count = count_phantom_history(cursor, user_id, last_real_date)
                print(f"  investmentHistory rows with stale epf (date > {last_real_date}): {hist_count}")
                if hist_count > 0:
                    if args.dry_run:
                        print(f"  [DRY-RUN] would UPDATE {hist_count} investmentHistory rows: SET epf = {history_action!r}")
                    else:
                        updated = neutralize_phantom_history(
                            cursor, user_id, last_real_date, history_action,
                        )
                        total_history_neutralized += updated
                        print(f"  UPDATED {updated} investmentHistory rows (neutralized epf)")

        if not args.dry_run:
            conn.commit()
            print(f"\n[cleanup_phantom_epf] Committed.")
            print(f"  investmentSnapshots EPF rows deleted: {total_snapshots_deleted}")
            print(f"  investmentHistory rows neutralized:   {total_history_neutralized}")
        else:
            print(f"\n[cleanup_phantom_epf] Dry-run complete. Nothing committed.")

        return 0
    finally:
        cursor.close()
        conn.close()


if __name__ == "__main__":
    sys.exit(main())
