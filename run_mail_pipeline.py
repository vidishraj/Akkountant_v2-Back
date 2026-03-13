#!/usr/bin/env python3
"""
Standalone script to run the unified mail processing pipeline.

Usage:
    python run_mail_pipeline.py --from 2025/01/01 --to 2025/02/26
    python run_mail_pipeline.py --from 2025/01/01 --to 2025/02/26 --user <firebase_id>
    python run_mail_pipeline.py --from 2025/01/01 --to 2025/02/26 --all-users
    python run_mail_pipeline.py --from 2025/01/01 --to 2025/02/26 --skip-dedup
    python run_mail_pipeline.py --from 2025/01/01 --to 2025/02/26 --dry-run

Boots the Flask app (without starting the HTTP server or schedulers),
sets up the DB + services, then calls MailProcessorService.process_emails()
exactly as the live pipeline or cron task would.
"""

import argparse
import json
import sys
import os

# Ensure the project root is on sys.path so imports resolve
sys.path.insert(0, os.path.dirname(os.path.abspath(__file__)))

from dotenv import load_dotenv

load_dotenv()

from flask import g

from utils.logger import Logger

logger = Logger("run_mail_pipeline").get_logger()


def build_app():
    """
    Build the Flask app with DB + services but skip schedulers/cron.
    We force ENV=LOCAL so _setup_schedulers() is a no-op.
    """
    original_env = os.environ.get("ENV")
    os.environ["ENV"] = "LOCAL"

    from app import Akkountant

    app = Akkountant(__name__)

    # Restore original ENV if it was set
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


def run_pipeline(app, user_id, date_from, date_to, dry_run=False, skip_dedup=False,
                 compare=False, reconcile=False, reconcile_only=False):
    """
    Run the mail processing pipeline for a single user within the Flask app context.
    """
    with app.app_context():
        g.db = app.db
        g.firebase_id = user_id

        mail_processor = app.mailProcessor
        reconciliation_service = app.reconciliationService

        result = {}

        if not reconcile_only:
            if skip_dedup:
                # Monkey-patch the dedup filter to pass everything through
                original_filter = mail_processor._filter_already_processed
                mail_processor._filter_already_processed = lambda emails, uid: emails
                logger.info("Dedup check disabled (--skip-dedup)")

            if compare:
                result = _compare_run(mail_processor, user_id, date_from, date_to)
            elif dry_run:
                result = _dry_run(mail_processor, user_id, date_from, date_to)
            else:
                result = mail_processor.process_emails(user_id, date_from, date_to)

            if skip_dedup:
                mail_processor._filter_already_processed = original_filter

        # Run cross-instrument reconciliation if requested
        if reconcile or reconcile_only:
            from datetime import datetime
            try:
                ps = datetime.strptime(date_from.replace("/", "-"), "%Y-%m-%d").date()
                pe = datetime.strptime(date_to.replace("/", "-"), "%Y-%m-%d").date()
            except ValueError:
                # Try alternate format
                ps = datetime.strptime(date_from, "%Y/%m/%d").date()
                pe = datetime.strptime(date_to, "%Y/%m/%d").date()

            logger.info(f"Running cross-instrument reconciliation for {ps} to {pe}")
            reconcile_result = reconciliation_service.reconcile_transfers(user_id, ps, pe)
            logger.info(f"Reconciliation result: {json.dumps(reconcile_result, default=str)}")

            if reconcile_only:
                result = reconcile_result
            else:
                result["reconciliation"] = reconcile_result

    return result


def _dry_run(mail_processor, user_id, date_from, date_to):
    """
    Execute the pipeline up to email separation but skip Claude processing.
    Useful to see what WOULD be processed without burning API calls.
    """
    summary = {
        "total_emails_fetched": 0,
        "financial_emails": 0,
        "already_processed": 0,
        "new_emails": 0,
        "text_emails": 0,
        "pdf_emails": 0,
        "dry_run": True,
        "emails": [],
    }

    token = mail_processor.transaction_service.fetchGmailTokenForUser(user_id)
    gmail_svc = mail_processor.transaction_service.gmailService
    all_emails = list(gmail_svc.findAllEmailsInInterval(user_id, token, date_from, date_to))
    summary["total_emails_fetched"] = len(all_emails)
    logger.info(f"[DRY RUN] Fetched {len(all_emails)} emails")

    # Agent-1: Classify ALL emails via Claude
    logger.info(f"[DRY RUN] Running Agent-1 classification on {len(all_emails)} emails...")
    financial_emails = mail_processor._classify_emails(all_emails)
    summary["financial_emails"] = len(financial_emails)
    logger.info(f"[DRY RUN] Agent-1 classified {len(financial_emails)} financial emails")

    # Save classified emails to disk
    mail_processor._save_emails_to_disk(financial_emails, user_id, date_from, date_to)

    new_emails = mail_processor._filter_already_processed(financial_emails, user_id)
    skipped = len(financial_emails) - len(new_emails)
    summary["already_processed"] = skipped
    summary["new_emails"] = len(new_emails)
    logger.info(f"[DRY RUN] New: {len(new_emails)}, Already processed: {skipped}")

    # Separate text/pdf without downloading attachments — just check structure
    text_emails, pdf_emails = mail_processor._separate_emails(new_emails, user_id, token)
    summary["text_emails"] = len(text_emails)
    summary["pdf_emails"] = len(pdf_emails)
    logger.info(f"[DRY RUN] Text: {len(text_emails)}, PDF: {len(pdf_emails)}")

    # Include email details with classification category
    for e in new_emails:
        summary["emails"].append({
            "gmail_id": e.get("gmail_id") or e.get("message_id"),
            "sender": e.get("sender", ""),
            "subject": e.get("subject", ""),
            "date": e.get("time", ""),
            "category": e.get("_category", ""),
        })

    return summary


def _compare_run(mail_processor, user_id, date_from, date_to):
    """
    Run BOTH the old domain pre-filter and Agent-1 on the same emails,
    then show the diff to detect false negatives and new catches.
    """
    token = mail_processor.transaction_service.fetchGmailTokenForUser(user_id)
    gmail_svc = mail_processor.transaction_service.gmailService
    all_emails = list(gmail_svc.findAllEmailsInInterval(user_id, token, date_from, date_to))
    logger.info(f"[COMPARE] Fetched {len(all_emails)} emails")

    # Run old domain pre-filter
    domain_filtered = mail_processor._pre_filter_emails(all_emails)
    domain_ids = {
        e.get("gmail_id") or e.get("message_id")
        for e in domain_filtered
    }
    logger.info(f"[COMPARE] Domain pre-filter: {len(domain_filtered)} emails")

    # Run Agent-1 classification
    logger.info(f"[COMPARE] Running Agent-1 classification on {len(all_emails)} emails...")
    agent_filtered = mail_processor._classify_emails(all_emails)
    agent_ids = {
        e.get("gmail_id") or e.get("message_id")
        for e in agent_filtered
    }
    logger.info(f"[COMPARE] Agent-1 classified: {len(agent_filtered)} emails")

    # Build lookup for display
    email_lookup = {}
    for e in all_emails:
        gid = e.get("gmail_id") or e.get("message_id")
        if gid:
            email_lookup[gid] = e

    def _email_summary(gid):
        e = email_lookup.get(gid, {})
        return {
            "gmail_id": gid,
            "sender": e.get("sender", ""),
            "subject": e.get("subject", ""),
            "date": e.get("time", ""),
            "category": e.get("_category", ""),
            "domain": e.get("_sender_domain", ""),
        }

    # FALSE NEGATIVES: domain filter caught them, Agent-1 missed them
    false_negatives = sorted(domain_ids - agent_ids)
    # NEW CATCHES: Agent-1 found them, domain filter missed them
    new_catches = sorted(agent_ids - domain_ids)
    # AGREEMENT: both caught them
    agreement = sorted(domain_ids & agent_ids)

    result = {
        "total_emails_fetched": len(all_emails),
        "domain_filter_count": len(domain_filtered),
        "agent1_count": len(agent_filtered),
        "agreement_count": len(agreement),
        "false_negatives_count": len(false_negatives),
        "new_catches_count": len(new_catches),
        "false_negatives": [_email_summary(gid) for gid in false_negatives],
        "new_catches": [_email_summary(gid) for gid in new_catches],
    }

    # Print readable comparison
    print("\n" + "=" * 60)
    print("COMPARISON: Domain Filter vs Agent-1")
    print("=" * 60)
    print(f"Total emails:        {len(all_emails)}")
    print(f"Domain filter found: {len(domain_filtered)}")
    print(f"Agent-1 found:       {len(agent_filtered)}")
    print(f"Both agree on:       {len(agreement)}")
    print()

    if false_negatives:
        print(f"FALSE NEGATIVES ({len(false_negatives)}) — Domain filter caught, Agent-1 MISSED:")
        print("-" * 60)
        for gid in false_negatives:
            e = email_lookup.get(gid, {})
            print(f"  {e.get('sender', '?')}")
            print(f"    Subject: {e.get('subject', '?')}")
            print(f"    Date: {e.get('time', '?')}")
            print()
    else:
        print("FALSE NEGATIVES: None — Agent-1 caught everything the domain filter did")
        print()

    if new_catches:
        print(f"NEW CATCHES ({len(new_catches)}) — Agent-1 found, domain filter missed:")
        print("-" * 60)
        for gid in new_catches:
            e = email_lookup.get(gid, {})
            print(f"  {e.get('sender', '?')}")
            print(f"    Subject: {e.get('subject', '?')}")
            print(f"    Category: {e.get('_category', '?')}")
            print(f"    Date: {e.get('time', '?')}")
            print()

    return result


def main():
    parser = argparse.ArgumentParser(
        description="Run the Akkountant unified mail processing pipeline."
    )
    parser.add_argument(
        "--from", dest="date_from", required=True,
        help="Start date in YYYY/MM/DD format (e.g. 2025/01/01)"
    )
    parser.add_argument(
        "--to", dest="date_to", required=True,
        help="End date in YYYY/MM/DD format (e.g. 2025/02/26)"
    )
    parser.add_argument(
        "--user", dest="user_id", default=None,
        help="Firebase user ID. If omitted, uses the first user with a Gmail token."
    )
    parser.add_argument(
        "--all-users", dest="all_users", action="store_true",
        help="Process all users with Gmail tokens."
    )
    parser.add_argument(
        "--dry-run", dest="dry_run", action="store_true",
        help="Fetch and filter emails but don't send to Claude. Shows what would be processed."
    )
    parser.add_argument(
        "--skip-dedup", dest="skip_dedup", action="store_true",
        help="Skip the processedEmails dedup check (reprocess everything)."
    )
    parser.add_argument(
        "--compare", dest="compare", action="store_true",
        help="Run both domain pre-filter and Agent-1, then show diff (false negatives + new catches)."
    )
    parser.add_argument(
        "--reconcile", dest="reconcile", action="store_true",
        help="After processing, run cross-instrument reconciliation for the date range."
    )
    parser.add_argument(
        "--reconcile-only", dest="reconcile_only", action="store_true",
        help="Skip email processing, only run cross-instrument reconciliation."
    )

    args = parser.parse_args()

    logger.info("Building Flask app (no HTTP server, no schedulers)...")
    app = build_app()

    with app.app_context():
        g.db = app.db

        # Resolve target user(s)
        if args.all_users:
            user_ids = get_users_with_gmail(app)
            if not user_ids:
                logger.error("No users with Gmail tokens found in the database.")
                sys.exit(1)
            logger.info(f"Processing {len(user_ids)} users: {user_ids}")
        elif args.user_id:
            user_ids = [args.user_id]
        else:
            # Default: first user with a Gmail token
            user_ids = get_users_with_gmail(app)
            if not user_ids:
                logger.error("No users with Gmail tokens found. Use --user to specify one.")
                sys.exit(1)
            user_ids = [user_ids[0]]
            logger.info(f"No --user specified, using first Gmail user: {user_ids[0]}")

    # Run pipeline for each user
    all_results = {}
    for uid in user_ids:
        logger.info(f"{'=' * 60}")
        logger.info(f"Processing user: {uid}")
        logger.info(f"Date range: {args.date_from} to {args.date_to}")
        logger.info(f"{'=' * 60}")

        try:
            result = run_pipeline(
                app, uid, args.date_from, args.date_to,
                dry_run=args.dry_run,
                skip_dedup=args.skip_dedup,
                compare=args.compare,
                reconcile=args.reconcile,
                reconcile_only=args.reconcile_only,
            )
            all_results[uid] = result

            logger.info(f"Result for {uid}:")
            logger.info(json.dumps(result, indent=2, default=str))
        except Exception as e:
            logger.error(f"Pipeline failed for user {uid}: {e}", exc_info=True)
            all_results[uid] = {"error": str(e)}

    # Final summary
    print("\n" + "=" * 60)
    print("PIPELINE RESULTS")
    print("=" * 60)
    for uid, result in all_results.items():
        print(f"\nUser: {uid}")
        print(json.dumps(result, indent=2, default=str))


if __name__ == "__main__":
    main()
