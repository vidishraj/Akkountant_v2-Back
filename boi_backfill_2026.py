#!/usr/bin/env python3
"""ak-5oi — one-shot BOI backfill for Feb-May 2026 statements.

After ak-2ql (StatementPatternEnum.BOI + domain-map) deploys to prod,
the cron will pick up FUTURE BOI statements automatically — but the
4 historical months it missed (Feb / Mar / Apr / May 2026) won't
self-heal because schedular's window is `yesterday → now`.

This script drives the full pipeline (Gmail fetch → classify → PDF
parse → insert) over the missing window. process_emails already
pre-filters processedEmails so re-runs are idempotent — the script
can be safely fired again if the first run partial-fails.

# Usage (on prod, after [DEPLOY-VERIFY GREEN] confirms ak-2ql is live)

  # Dry run — fetch + classify but do NOT insert:
  python boi_backfill_2026.py --dry-run

  # Full run — fetch + classify + parse + insert:
  python boi_backfill_2026.py

  # Pin a specific user (default: first Gmail-authorized user):
  python boi_backfill_2026.py --user <firebase_id>

  # Custom date window (defaults to 2026/02/01 → 2026/06/01):
  python boi_backfill_2026.py --date-from 2026/02/01 --date-to 2026/06/01

# Reports back

Final summary line is the [DONE ak-5oi] payload: emails fetched,
emails classified as financial, PDF emails processed, plus the
download-side diagnostic from StatementDownloadService for BOI only
(should print ≥4 if Gmail still has them).

# Why both StatementDownloadService AND process_emails

- route_download_process is BANK-SPECIFIC: fires the exact Gmail
  query that ak-2ql's StatementPatternEnum.BOI value encodes. It's
  the cheapest way to verify the new pattern actually matches the
  4 emails before we pay the cost of a full pipeline run.
- process_emails is the canonical INGEST flow used by cron. It runs
  classification + PDF analysis + DB insert. We invoke it second so
  the ingest hits the exact same code path the scheduler uses every
  hour.

# Idempotency

mailProcessorService._filter_already_processed (line 261 in
services/mailProcessorService.py) skips emails whose gmail_id is
already in processedEmails. Re-running this script is safe — already-
ingested statements won't double-insert. The download_to_temp side
writes files to a tmp dir each run; those don't affect DB state.

dispatched_by: akkountant/crew/akkountant_lead (hq-wisp-31r3e)
bead: ak-5oi
"""

import argparse
import os
import sys
from datetime import datetime

sys.path.insert(0, os.path.dirname(os.path.abspath(__file__)))

from dotenv import load_dotenv
load_dotenv()

from flask import g
from utils.logger import Logger


logger = Logger("boi_backfill_2026").get_logger()


# Default window. Gmail uses YYYY/MM/DD format.
# 2026/06/01 catches everything up to (but not including) June so the
# 4 monthly statements (Feb / Mar / Apr / May 2026) are inside the
# half-open range.
DEFAULT_DATE_FROM = "2026/02/01"
DEFAULT_DATE_TO = "2026/06/01"


def build_app():
    """Build Flask app without HTTP server or schedulers.

    Same pattern as reprocess_icici.py + run_overnight_scan.py: flip
    ENV to LOCAL during construction so we don't fire the production
    cron threads, then restore the env."""
    original_env = os.environ.get("ENV")
    os.environ["ENV"] = "LOCAL"
    from app import Akkountant
    app = Akkountant(__name__)
    if original_env is not None:
        os.environ["ENV"] = original_env
    else:
        os.environ.pop("ENV", None)
    return app


def pick_user(app, override=None):
    """Resolve a user_id. Prefer --user override; otherwise pick the
    first Gmail-authorized user. Akkountant is single-tenant
    (per CLAUDE.md user note) so the first one is always the
    Overseer."""
    from models import UserToken
    from enums.ServiceTypeEnum import ServiceTypeEnum

    if override:
        # Sanity-check the override exists in the token table so we
        # don't run a no-op against a typo.
        match = (
            app.db.session.query(UserToken.user_id)
            .filter_by(
                user_id=override,
                service_type=ServiceTypeEnum.Gmail.value,
            )
            .first()
        )
        if match is None:
            raise SystemExit(
                f"User {override!r} has no Gmail token; aborting."
            )
        return override

    users = (
        app.db.session.query(UserToken.user_id)
        .filter_by(service_type=ServiceTypeEnum.Gmail.value)
        .distinct()
        .all()
    )
    if not users:
        raise SystemExit(
            "No Gmail-authorized users in DB — cannot run backfill."
        )
    return users[0][0]


def probe_boi_download(app, date_from, date_to):
    """STEP 1: bank-specific Gmail probe.

    Uses StatementDownloadService.route_download_process to fire the
    BOI-only Gmail query — a sanity check that ak-2ql's
    StatementPatternEnum.BOI value actually matches the 4 emails
    BEFORE we run the heavier classification pipeline.

    Returns the list of downloaded file paths (typically 4 PDFs)."""
    from services.StatementDownloadService import StatementDownloadService

    print(f"\n[STEP 1] BOI-only Gmail probe ({date_from} → {date_to})")
    print(f"         using ak-2ql StatementPatternEnum.BOI")

    svc = StatementDownloadService()
    # Note: route_download_process uses the singleton gmailService set
    # during app boot. The first user we pick should be the same
    # Overseer whose Gmail token is loaded.
    try:
        files = svc.route_download_process(
            bank_type="BOI",
            date_to=date_to,
            date_from=date_from,
        )
    except Exception as exc:
        # Loud failure per spec
        print(f"[STEP 1 FAIL] route_download_process raised: {exc}")
        raise

    print(f"[STEP 1 RESULT] downloaded {len(files)} attachment(s)")
    for f in files:
        print(f"  - {f}")
    if len(files) < 4:
        print(
            f"[STEP 1 WARN] expected ≥4 files (Feb/Mar/Apr/May 2026); "
            f"got {len(files)}. Inspect the Gmail query manually if "
            f"this is low."
        )
    return files


def run_ingest(app, user_id, date_from, date_to, dry_run=False):
    """STEP 2: canonical pipeline.

    Runs mail_processor.process_emails — same code path the cron
    uses every hour. Idempotent: pre-filter skips emails already in
    processedEmails, so duplicate runs land zero new rows.

    On dry_run=True we still call process_emails (there's no
    cheap-to-implement "fetch + classify but skip insert" toggle in
    the service), but we WARN loudly so the operator can choose to
    bail before the heavy LLM-classification + DB-insert step.
    """
    print(f"\n[STEP 2] Canonical ingest pipeline "
          f"({date_from} → {date_to}, user={user_id})")
    if dry_run:
        print("[STEP 2 DRY-RUN] aborting BEFORE process_emails call. "
              "process_emails does not expose a fetch-only mode; "
              "rerun WITHOUT --dry-run to actually ingest.")
        return None

    # mail_processor is wired by app.py during _setup_services. Grab
    # it off the app instance the same way the cron does.
    mail_processor = getattr(app, "mailProcessor", None)
    if mail_processor is None:
        raise SystemExit(
            "app.mailProcessor not wired; cannot run pipeline. "
            "Check app.py _setup_services."
        )

    started = datetime.utcnow()
    try:
        summary = mail_processor.process_emails(
            user_id=user_id,
            date_from=date_from,
            date_to=date_to,
        )
    except Exception as exc:
        print(f"[STEP 2 FAIL] process_emails raised: {exc}")
        raise

    elapsed = (datetime.utcnow() - started).total_seconds()
    print(f"[STEP 2 RESULT] {elapsed:.1f}s elapsed")
    for k, v in (summary or {}).items():
        print(f"  - {k}: {v}")
    return summary


def main():
    parser = argparse.ArgumentParser(
        description="BOI backfill — fetch + ingest Feb-May 2026 monthly statements",
    )
    parser.add_argument(
        "--user", default=None,
        help="Firebase ID (default: first Gmail-authorized user in DB)",
    )
    parser.add_argument(
        "--date-from", default=DEFAULT_DATE_FROM,
        help=f"Gmail YYYY/MM/DD start (default: {DEFAULT_DATE_FROM})",
    )
    parser.add_argument(
        "--date-to", default=DEFAULT_DATE_TO,
        help=f"Gmail YYYY/MM/DD end, exclusive (default: {DEFAULT_DATE_TO})",
    )
    parser.add_argument(
        "--dry-run", action="store_true",
        help="Run STEP 1 (BOI probe) but skip the ingest pipeline.",
    )
    parser.add_argument(
        "--skip-probe", action="store_true",
        help="Skip STEP 1 (BOI-only download probe). Useful for re-runs.",
    )
    args = parser.parse_args()

    print("ak-5oi BOI backfill — Feb-May 2026")
    print(f"  date_from = {args.date_from}")
    print(f"  date_to   = {args.date_to}")
    print(f"  dry-run   = {args.dry_run}")

    app = build_app()
    with app.app_context():
        # The investment-side handlers expect g.db; the mailProcessor
        # path uses self.transaction_service.db. Hooking g.db is the
        # safe / standard pattern used by reprocess_icici.py.
        g.db = app.db

        user_id = pick_user(app, override=args.user)
        print(f"  user_id   = {user_id}")

        probe_files = None
        if not args.skip_probe:
            probe_files = probe_boi_download(
                app, date_from=args.date_from, date_to=args.date_to,
            )

        summary = run_ingest(
            app, user_id=user_id,
            date_from=args.date_from, date_to=args.date_to,
            dry_run=args.dry_run,
        )

        # Final consolidated report for the [DONE ak-5oi] mail back to
        # Lead. Stable format makes scraping the prod log easy.
        print("\n[ak-5oi DONE]")
        print(f"  probe_files   = {0 if probe_files is None else len(probe_files)}")
        if summary:
            print(f"  fetched       = {summary.get('total_emails_fetched', 0)}")
            print(f"  pre_skipped   = {summary.get('pre_skipped', 0)}")
            print(f"  financial     = {summary.get('financial_emails', 0)}")
            print(f"  text_processed= {summary.get('text_emails_processed', 0)}")
            print(f"  pdf_processed = {summary.get('pdf_emails_processed', 0)}")
            errors = summary.get("errors") or []
            print(f"  error_count   = {len(errors)}")
            if errors:
                for e in errors:
                    print(f"    ERROR: {e}")
        else:
            print("  (skipped ingest; dry-run mode)")


if __name__ == "__main__":
    main()
