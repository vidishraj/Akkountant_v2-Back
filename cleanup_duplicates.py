#!/usr/bin/env python3
"""
Cleanup duplicate statement transactions.

Root cause: HDFC (and other banks) send the same monthly statement multiple times
as separate emails. Each email has a unique gmail_id, so it bypasses the dedup.
Each processing of the same PDF produces slightly different OCR output, creating
duplicate transactions with different referenceIDs.

Strategy:
1. Identify fileIDs with excess transactions (actual >> expected)
2. For each, group transactions by (date, amount, normalized_payee)
3. Keep one row per group (longest details = best OCR)
4. Delete the rest
5. Clean up duplicate processedEmails entries
"""

import re
import sys
import os

sys.path.insert(0, os.path.dirname(os.path.abspath(__file__)))
from dotenv import load_dotenv
load_dotenv()

import mysql.connector

DB_CONFIG = {
    "host": "localhost",
    "user": "root",
    "password": "iamvidish",
    "database": "akkountantv2",
}


def normalize_payee(details: str) -> str:
    """Extract and normalize payee name from transaction details.

    Handles UPI, NEFT, IMPS formats. Returns first meaningful identifier
    that's stable across OCR variations.
    """
    if not details:
        return ""

    details = details.strip()

    # UPI: "UPI-PAYEE NAME-upiid@bank-IFSC-REF-..."
    # Extract just the payee name (between first and second hyphen)
    m = re.match(r'^UPI[/-]([A-Z][A-Z\s]+)', details, re.IGNORECASE)
    if m:
        payee = m.group(1).strip()
        # Normalize: remove trailing spaces, lowercase
        # Use only alphabetic chars to handle OCR errors like VEERESH/VEENESH
        alpha_only = re.sub(r'[^A-Za-z]', '', payee).upper()
        # Use first 8 alpha chars — enough to distinguish payees
        # but tolerant of OCR errors in later chars
        return f"UPI_{alpha_only[:8]}"

    # NEFT/IMPS: "NEFT-REF-PAYEE" or "IMPS/P2A/REF/ACCT/desc"
    m = re.match(r'^(NEFT|IMPS)[/-]', details, re.IGNORECASE)
    if m:
        # For NEFT/IMPS, use first 20 chars normalized (remove spaces/special)
        alpha = re.sub(r'[^A-Za-z0-9]', '', details[:30]).upper()
        return f"BANK_{alpha[:15]}"

    # GST, Interest, etc. — use first 25 chars normalized
    alpha = re.sub(r'[^A-Za-z0-9]', '', details[:30]).upper()
    return f"OTHER_{alpha[:15]}"


def get_bloated_fileids(cursor):
    """Find fileIDs where transaction count significantly exceeds statement count."""
    cursor.execute("""
        SELECT f.fileID, f.statementCount, COUNT(t.referenceID) as actual
        FROM filedetails f
        JOIN transactions t ON t.fileID = f.fileID
        WHERE t.source = 'Statement'
        GROUP BY f.fileID, f.statementCount
        HAVING actual > statementCount * 1.5 AND actual > statementCount + 5
    """)
    return cursor.fetchall()


def dedup_file_transactions(cursor, file_id, dry_run=False):
    """Dedup transactions for a single fileID. Returns (kept, deleted) counts."""
    cursor.execute("""
        SELECT referenceID, date, amount, details, LENGTH(details) as detail_len
        FROM transactions
        WHERE fileID = %s AND source = 'Statement'
        ORDER BY date, amount, LENGTH(details) DESC
    """, (file_id,))

    rows = cursor.fetchall()
    if not rows:
        return 0, 0

    # Group by (date, amount, normalized_payee)
    groups = {}
    for ref_id, txn_date, amount, details, detail_len in rows:
        payee = normalize_payee(details)
        key = (str(txn_date), str(amount), payee)

        if key not in groups:
            # First (longest details due to ORDER BY) — keep this one
            groups[key] = ref_id

    keep_ids = set(groups.values())
    all_ids = {row[0] for row in rows}
    delete_ids = all_ids - keep_ids

    if not dry_run and delete_ids:
        # Delete in batches
        delete_list = list(delete_ids)
        batch_size = 500
        for i in range(0, len(delete_list), batch_size):
            batch = delete_list[i:i + batch_size]
            placeholders = ','.join(['%s'] * len(batch))
            cursor.execute(
                f"DELETE FROM transactions WHERE referenceID IN ({placeholders})",
                batch
            )

    return len(keep_ids), len(delete_ids)


def cleanup_duplicate_emails(cursor, dry_run=False):
    """Remove duplicate processedEmails entries (same statement sent multiple times)."""

    # Find groups of bank_statement emails with identical subjects
    cursor.execute("""
        SELECT subject, GROUP_CONCAT(id ORDER BY processed_at ASC) as ids,
               GROUP_CONCAT(gmail_id ORDER BY processed_at ASC) as gmail_ids,
               COUNT(*) as cnt
        FROM processedEmails
        WHERE category = 'bank_statement'
        GROUP BY subject
        HAVING cnt > 1
        ORDER BY cnt DESC
    """)

    groups = cursor.fetchall()
    total_deleted = 0

    for subject, id_str, gmail_id_str, cnt in groups:
        ids = id_str.split(',')
        gmail_ids = gmail_id_str.split(',')

        # Check if these are truly duplicates (same statement period)
        # or different months with same subject prefix
        # For "Email Account Statement" — these ARE different months, skip
        if subject.startswith("Email Account Statement"):
            continue
        # For generic subjects like "BOI: Account Statement", check dates
        if subject in ("BOI: Account Statement", "INWARD REMITTANCE"):
            # These could be different months — skip automatic cleanup
            continue

        # Keep the first one (earliest processed_at), delete the rest
        keep_id = ids[0]
        delete_ids = ids[1:]

        if not dry_run:
            # First delete from customer_emails (FK reference)
            for del_id in delete_ids:
                cursor.execute(
                    "DELETE FROM customer_emails WHERE email_id = %s",
                    (int(del_id),)
                )

            placeholders = ','.join(['%s'] * len(delete_ids))
            cursor.execute(
                f"DELETE FROM processedEmails WHERE id IN ({placeholders})",
                [int(x) for x in delete_ids]
            )

        total_deleted += len(delete_ids)
        print(f"  {subject[:65]}: kept 1, deleted {len(delete_ids)}")

    return total_deleted


def main():
    dry_run = "--dry-run" in sys.argv

    conn = mysql.connector.connect(**DB_CONFIG)
    cursor = conn.cursor()

    print("=" * 70)
    print(f"DUPLICATE TRANSACTION CLEANUP {'(DRY RUN)' if dry_run else ''}")
    print("=" * 70)

    # Step 1: Get current state
    cursor.execute("SELECT COUNT(*) FROM transactions WHERE source = 'Statement'")
    total_before = cursor.fetchone()[0]
    cursor.execute("SELECT COUNT(*) FROM transactions")
    grand_total_before = cursor.fetchone()[0]
    print(f"\nBefore: {grand_total_before} total transactions ({total_before} Statement)")

    # Step 2: Find bloated fileIDs
    bloated = get_bloated_fileids(cursor)
    if not bloated:
        print("No bloated fileIDs found. Nothing to clean.")
        return

    print(f"\nFound {len(bloated)} fileIDs with excess transactions:")

    total_kept = 0
    total_deleted = 0

    for file_id, expected, actual in bloated:
        kept, deleted = dedup_file_transactions(cursor, file_id, dry_run)
        total_kept += kept
        total_deleted += deleted

        short_id = file_id.split('_', 4)[-1] if '_' in file_id else file_id
        print(f"  {short_id}: {actual} → {kept} (deleted {deleted})")

    print(f"\nTransaction cleanup: kept {total_kept}, deleted {total_deleted}")

    # Step 3: Clean up duplicate processedEmails
    print(f"\nCleaning up duplicate processedEmails:")
    emails_deleted = cleanup_duplicate_emails(cursor, dry_run)
    print(f"Deleted {emails_deleted} duplicate processedEmails entries")

    # Step 4: Final state
    if not dry_run:
        conn.commit()
        cursor.execute("SELECT COUNT(*) FROM transactions WHERE source = 'Statement'")
        total_after = cursor.fetchone()[0]
        cursor.execute("SELECT COUNT(*) FROM transactions")
        grand_total_after = cursor.fetchone()[0]
        print(f"\nAfter:  {grand_total_after} total transactions ({total_after} Statement)")
        print(f"Net change: {grand_total_before} → {grand_total_after} "
              f"(removed {grand_total_before - grand_total_after})")
    else:
        print(f"\n[DRY RUN] Would delete {total_deleted} transactions "
              f"and {emails_deleted} processedEmails")

    cursor.close()
    conn.close()
    print("\nDone.")


if __name__ == "__main__":
    main()
