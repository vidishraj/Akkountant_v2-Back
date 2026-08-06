#!/usr/bin/env python3
"""ak-ran Phase 1 bootstrap: seed the first Pending Job for
WealthDigestTask so the TaskScheduler picks it up on the next tick.

Ships with the initial ak-ran deploy. Infra runs this ONCE after the
schedular.service restart that activates the new WealthDigestTask
class. After that, WealthDigestTask.run() self-reschedules at 24h
cadence and no further bootstrap is needed.

Idempotent: if a Pending or Overdue WealthDigest Job already exists,
the script logs + exits 0 (no duplicate). Safe to re-run after
partial deploys or if the operator isn't sure whether it was seeded.

Usage (from repo root, with Flask app context available):
    python3 bootstrap_wealth_digest.py                # dry-run
    python3 bootstrap_wealth_digest.py --commit       # actually seed
    python3 bootstrap_wealth_digest.py --user-id UID  # override user

Environment:
    WEALTH_DIGEST_USER_ID       — user_id for the seeded Job.row.user_id
                                  column. Falls back to CLI --user-id
                                  or "SYSTEM_BOOTSTRAP" placeholder
                                  (WealthDigestTask.run reads its own
                                  user_id from the same env var; the
                                  Job.row.user_id is only used for
                                  ownership metadata, not for actual
                                  digest generation).

Exit codes:
    0 — Pending/Overdue WealthDigest job exists (either pre-existing
        or just seeded).
    1 — Bootstrap failed (DB error, missing app context, etc.).
"""

import argparse
import os
import sys
from datetime import datetime


_DEFAULT_USER_ID_ENV = "WEALTH_DIGEST_USER_ID"
_PLACEHOLDER_USER = "SYSTEM_BOOTSTRAP"


def main():
    parser = argparse.ArgumentParser(description=__doc__)
    parser.add_argument(
        "--commit", action="store_true",
        help="Actually write the Job row (default: dry-run + report).",
    )
    parser.add_argument(
        "--user-id", default=None,
        help=(
            f"Explicit user_id for the seeded Job.row.user_id. "
            f"Defaults to ${_DEFAULT_USER_ID_ENV} or "
            f"'{_PLACEHOLDER_USER}' placeholder."
        ),
    )
    args = parser.parse_args()

    user_id = (
        args.user_id
        or os.getenv(_DEFAULT_USER_ID_ENV)
        or _PLACEHOLDER_USER
    )
    if user_id == _PLACEHOLDER_USER:
        print(
            f"WARNING: no --user-id or ${_DEFAULT_USER_ID_ENV} set; "
            f"seeding with placeholder '{_PLACEHOLDER_USER}'. "
            f"WealthDigestTask.run() reads its own user_id from the "
            f"same env var at runtime — set it on schedular.service.",
            file=sys.stderr,
        )

    # Late imports so --help works without the app stack. Also lets
    # ak-ran v2 #5's WEALTH_DIGEST_JOB_TITLE constant live in the
    # same import block — bootstrap uses it as the Job.title so a
    # rename only touches services/tasks/wealth_digest_constants.py.
    try:
        from app import app  # Flask app object with DB wiring
        from models.Jobs import Job
        from enums.TaskStatusEnum import JobStatus
        from utils.DateTimeUtil import clamp_to_allowed_window
        from services.tasks.wealth_digest_constants import (
            WEALTH_DIGEST_JOB_TITLE,
        )
    except ImportError as exc:
        print(f"ERROR: import chain missing ({exc}) — run from repo root "
              f"with the app venv activated.", file=sys.stderr)
        return 1
    job_title = WEALTH_DIGEST_JOB_TITLE

    with app.app_context():
        from services.Base_Service import BaseService
        db = BaseService().db

        # ak-3eo H3: RUNNING included in the non-terminal set so
        # bootstrap can't seed a duplicate during an active daily run.
        # Pre-H3 the check was (PENDING, OVERDUE) only — if bootstrap
        # was re-run WHILE the scheduler had a Running WealthDigest
        # job in-flight (e.g. operator re-running post-deploy to be
        # sure), a second Pending would be seeded → next tick fires
        # two digests → double-write to the conversation. RUNNING in
        # the set makes bootstrap wait for the current run to
        # complete/fail before adding another.
        existing = db.session.query(Job).filter(
            Job.title == job_title,
            Job.status.in_([
                JobStatus.PENDING.value,
                JobStatus.OVERDUE.value,
                JobStatus.RUNNING.value,
            ]),
        ).first()
        if existing is not None:
            print(
                f"IDEMPOTENT: {job_title} already has a "
                f"{existing.status} job (id={existing.id}, "
                f"due={existing.due_date}). Nothing to do."
            )
            return 0

        due = clamp_to_allowed_window(datetime.now())
        print(
            f"{'SEED' if args.commit else 'DRY-RUN'}: would insert "
            f"Job(title={job_title!r}, user_id={user_id!r}, "
            f"due_date={due}, status=Pending, priority=High)"
        )
        if not args.commit:
            print("  (add --commit to actually write)")
            return 0

        job = Job(
            title=job_title,
            status=JobStatus.PENDING.value,
            priority="High",
            due_date=due,
            user_id=user_id,
            result=None,
        )
        db.session.add(job)
        db.session.commit()
        print(f"OK: seeded Job id={job.id}")
        return 0


if __name__ == "__main__":
    sys.exit(main())
