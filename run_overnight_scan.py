#!/usr/bin/env python3
"""
Overnight email scan script.

Scans emails from March 2022 to March 2026 in 2-week windows with adaptive
backoff. Clears processedEmails and customer_emails tables before starting.

Usage:
    python run_overnight_scan.py
    python run_overnight_scan.py --user <firebase_id>
    python run_overnight_scan.py --resume-from 2024/06/01
    python run_overnight_scan.py --dry-run
"""

import argparse
import json
import signal
import sys
import os
import time
from datetime import date, datetime, timedelta

sys.path.insert(0, os.path.dirname(os.path.abspath(__file__)))

from dotenv import load_dotenv
load_dotenv()

from flask import g
from sqlalchemy import text
from utils.logger import Logger

logger = Logger("overnight_scan").get_logger()

# Timing constants
BASE_DELAY = 5            # seconds between successful scans
PROBE_INTERVAL = 900      # 15 minutes — fixed retry interval on errors (limit resets in ~4 hours)
MAX_PROBES = 20           # give up on a window after ~5 hours of probing (20 * 15min)

# Rate-limit detection: if we fetch >= this many emails but classify 0 as financial,
# it almost certainly means classification was rate-limited (not genuinely 0 financial).
RATE_LIMIT_FETCH_THRESHOLD = 20

# Scan range
SCAN_START = date(2022, 3, 1)
SCAN_END = date(2026, 3, 7)
WINDOW_DAYS = 14


def build_app():
    """Build Flask app without HTTP server or schedulers."""
    original_env = os.environ.get("ENV")
    os.environ["ENV"] = "LOCAL"

    from app import Akkountant
    app = Akkountant(__name__)

    if original_env is not None:
        os.environ["ENV"] = original_env
    else:
        os.environ.pop("ENV", None)

    return app


def get_users_with_gmail(app):
    """Return list of user_ids that have Gmail tokens stored."""
    from models import UserToken
    from enums.ServiceTypeEnum import ServiceTypeEnum

    users = (
        app.db.session.query(UserToken.user_id)
        .filter_by(service_type=ServiceTypeEnum.Gmail.value)
        .distinct()
        .all()
    )
    return [uid for (uid,) in users]


def generate_scan_windows(start, end, interval_days=WINDOW_DAYS):
    """Generate (start_date, end_date) tuples covering the full range."""
    windows = []
    current = start
    while current < end:
        window_end = min(current + timedelta(days=interval_days), end)
        windows.append((current, window_end))
        current = window_end
    return windows


def clear_tables(app):
    """Clear processedEmails and customer_emails tables."""
    with app.app_context():
        g.db = app.db
        # customer_emails has FK to processedEmails, delete it first
        ce_count = app.db.session.execute(text("SELECT COUNT(*) FROM customer_emails")).scalar()
        pe_count = app.db.session.execute(text("SELECT COUNT(*) FROM processedEmails")).scalar()

        app.db.session.execute(text("DELETE FROM customer_emails"))
        app.db.session.execute(text("DELETE FROM processedEmails"))
        app.db.session.commit()

        logger.info(f"Cleared {ce_count} customer_emails + {pe_count} processedEmails rows")
        print(f"  Cleared: {ce_count} customer_email links, {pe_count} processed emails")


def format_date(d):
    """Format date as YYYY/MM/DD for the pipeline."""
    return d.strftime("%Y/%m/%d")


def run_scan_window(app, user_id, date_from, date_to):
    """Run the pipeline for a single scan window. Returns result dict."""
    with app.app_context():
        g.db = app.db
        g.firebase_id = user_id
        result = app.mailProcessor.process_emails(user_id, date_from, date_to)
    return result


def main():
    parser = argparse.ArgumentParser(description="Overnight email scan: March 2022 → March 2026")
    parser.add_argument("--user", dest="user_id", default=None, help="Firebase user ID")
    parser.add_argument("--resume-from", dest="resume_from", default=None,
                        help="Skip windows before this date (YYYY/MM/DD)")
    parser.add_argument("--dry-run", dest="dry_run", action="store_true",
                        help="Show scan plan without executing")
    parser.add_argument("--stop-at", dest="stop_at", default=None,
                        help="Stop scanning at this date (YYYY/MM/DD)")
    parser.add_argument("--no-clear", dest="no_clear", action="store_true",
                        help="Skip clearing tables (useful with --resume-from)")
    args = parser.parse_args()

    # State for Ctrl+C summary
    completed_windows = []
    skipped_windows = []
    failed_windows = []
    total_emails = 0
    start_time = time.time()
    interrupted = False

    def signal_handler(sig, frame):
        nonlocal interrupted
        interrupted = True
        print("\n\nInterrupted! Printing summary...\n")
        print_summary()
        sys.exit(0)

    def print_summary():
        elapsed = time.time() - start_time
        hours = elapsed / 3600
        print("\n" + "=" * 70)
        print("OVERNIGHT SCAN SUMMARY")
        print("=" * 70)
        print(f"  Runtime:           {hours:.1f} hours ({elapsed:.0f}s)")
        print(f"  Windows completed: {len(completed_windows)} / {len(windows)}")
        print(f"  Windows skipped:   {len(skipped_windows)}")
        print(f"  Windows failed:    {len(failed_windows)}")
        print(f"  Total emails:      {total_emails}")
        if completed_windows:
            print(f"  First window:      {completed_windows[0]}")
            print(f"  Last window:       {completed_windows[-1]}")
        if failed_windows:
            print(f"\n  Failed windows:")
            for w in failed_windows:
                print(f"    - {w}")
        print("=" * 70)

    signal.signal(signal.SIGINT, signal_handler)
    signal.signal(signal.SIGTERM, signal_handler)

    # Build app
    print("Building Flask app...")
    app = build_app()

    # Resolve user
    with app.app_context():
        g.db = app.db
        if args.user_id:
            user_id = args.user_id
        else:
            user_ids = get_users_with_gmail(app)
            if not user_ids:
                print("ERROR: No users with Gmail tokens found.")
                sys.exit(1)
            user_id = user_ids[0]
            print(f"Using first Gmail user: {user_id}")

    # Generate windows
    windows = generate_scan_windows(SCAN_START, SCAN_END)
    print(f"\nScan plan: {len(windows)} windows of {WINDOW_DAYS} days each")
    print(f"  Range: {SCAN_START} → {SCAN_END}")

    # Apply resume filter
    if args.resume_from:
        try:
            resume_date = datetime.strptime(args.resume_from, "%Y/%m/%d").date()
        except ValueError:
            resume_date = datetime.strptime(args.resume_from, "%Y-%m-%d").date()

        original_count = len(windows)
        windows = [(s, e) for s, e in windows if e > resume_date]
        skipped = original_count - len(windows)
        print(f"  Resuming from {resume_date}: skipping {skipped} windows, {len(windows)} remaining")

    if args.stop_at:
        try:
            stop_date = datetime.strptime(args.stop_at, "%Y/%m/%d").date()
        except ValueError:
            stop_date = datetime.strptime(args.stop_at, "%Y-%m-%d").date()

        before = len(windows)
        windows = [(s, e) for s, e in windows if s < stop_date]
        print(f"  Stopping at {stop_date}: {len(windows)} windows (trimmed {before - len(windows)})")

    if args.dry_run:
        print("\n[DRY RUN] Scan windows:")
        for i, (s, e) in enumerate(windows, 1):
            print(f"  {i:3d}. {format_date(s)} → {format_date(e)}")
        print(f"\nTotal: {len(windows)} windows. Estimated time: {len(windows) * 2:.0f} - {len(windows) * 5:.0f} minutes")
        return

    # Clear tables
    if not args.no_clear:
        print("\nClearing previous scan data...")
        clear_tables(app)
    else:
        print("\nSkipping table clear (--no-clear)")

    # Run scan loop
    print(f"\nStarting overnight scan at {datetime.now().strftime('%H:%M:%S')}...")
    print(f"{'=' * 70}\n")

    for idx, (window_start, window_end) in enumerate(windows):
        if interrupted:
            break

        date_from = format_date(window_start)
        date_to = format_date(window_end)
        window_label = f"[{idx + 1}/{len(windows)}] {date_from} → {date_to}"

        probes = 0
        while probes < MAX_PROBES:
            if interrupted:
                break

            try:
                print(f"{datetime.now().strftime('%H:%M:%S')} {window_label} — scanning...", end="", flush=True)
                scan_start = time.time()

                result = run_scan_window(app, user_id, date_from, date_to)

                scan_duration = time.time() - scan_start
                fetched = result.get("total_emails_fetched", 0)
                financial = result.get("financial_emails", 0)
                text_proc = result.get("text_emails_processed", 0)
                pdf_proc = result.get("pdf_emails_processed", 0)
                errors = result.get("errors", [])

                # Detect rate-limited classification: many emails fetched but
                # 0 classified as financial — classification silently failed.
                if fetched >= RATE_LIMIT_FETCH_THRESHOLD and financial == 0:
                    probes += 1
                    if probes >= MAX_PROBES:
                        print(f" RATE-LIMITED — gave up after {MAX_PROBES} probes: "
                              f"{fetched} fetched, 0 financial")
                        failed_windows.append(f"{date_from} → {date_to}: rate-limited (0/{fetched})")
                        break
                    print(f" RATE-LIMITED ({fetched} fetched, 0 financial) — "
                          f"probe {probes}/{MAX_PROBES}, retrying in {PROBE_INTERVAL // 60}min")
                    sleep_end = time.time() + PROBE_INTERVAL
                    while time.time() < sleep_end and not interrupted:
                        time.sleep(min(5, sleep_end - time.time()))
                    continue  # Retry same window

                total_emails += fetched
                completed_windows.append(f"{date_from} → {date_to}")

                print(f" done ({scan_duration:.0f}s) — {fetched} fetched, {financial} financial, "
                      f"{text_proc} text, {pdf_proc} pdf"
                      + (f", {len(errors)} errors" if errors else ""))

                if errors:
                    for err in errors[:3]:
                        print(f"    WARNING: {str(err)[:120]}")

                # Progress update every 10 windows
                if (idx + 1) % 10 == 0:
                    elapsed = time.time() - start_time
                    avg_per_window = elapsed / (idx + 1)
                    remaining = avg_per_window * (len(windows) - idx - 1)
                    print(f"    >>> Progress: {idx + 1}/{len(windows)} windows, "
                          f"~{remaining / 60:.0f}min remaining, {total_emails} total emails")

                # Sleep before next window
                if idx + 1 < len(windows):
                    time.sleep(BASE_DELAY)

                break  # Success — exit retry loop

            except Exception as e:
                probes += 1

                if probes >= MAX_PROBES:
                    print(f" FAILED after {MAX_PROBES} probes (~{MAX_PROBES * PROBE_INTERVAL // 3600}h): {str(e)[:100]}")
                    failed_windows.append(f"{date_from} → {date_to}: {str(e)[:80]}")
                    break  # Give up on this window

                print(f" error: {str(e)[:80]} (probe {probes}/{MAX_PROBES}, "
                      f"retrying in {PROBE_INTERVAL // 60}min)")

                # Fixed 15-min wait with interrupt check
                sleep_end = time.time() + PROBE_INTERVAL
                while time.time() < sleep_end and not interrupted:
                    time.sleep(min(5, sleep_end - time.time()))

    # Final summary
    print_summary()


if __name__ == "__main__":
    main()
