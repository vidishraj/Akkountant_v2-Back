"""ak-kxdr: daily scheduler task that hard-DROPs user-vault files
whose soft-delete window has expired.

## Scope

Finds every ``UserFile`` row where ``deleted_at < NOW - 30 days``
and hard-deletes it (disk file + DB row). Runs once every 24h.

## Why a scheduler task (not a before_request sweep)

Without a background task, expired trash would accumulate
indefinitely — the soft-delete endpoint only sets ``deleted_at``
and never DROPs. Alternative shapes rejected:
  * ``before_request`` cleanup sweep per API call — latency cost
    + doesn't fire if the vault isn't accessed.
  * Trigger-on-restore-attempt — same problem; doesn't fire for
    "never restored" files.

A daily task is the right lifecycle: small, bounded, idempotent,
easy to monitor via the standard Jobs row.

## Observability

Logs the count hard-dropped + per-file ``(user_id, file_id)``
pairs. **Does NOT log filenames or file contents** per bead §23
#4 — vault often holds identity docs, and filenames can be PII
(e.g. `passport_vidish_2026.pdf`). The id-pair is sufficient to
cross-reference with vault audit queries if something goes wrong.

## Blast radius

Zero. The task operates on a new column + a new endpoint added by
ak-kxdr; pre-ak-kxdr rows have ``deleted_at IS NULL`` so the
cleanup query selects zero rows on a freshly-ALTER'd prod DB. The
first non-trivial run can only land AFTER the ak-kxdr delete
endpoint has been live for at least 30 days.

## Dependencies

* ``FileStorageService.cleanup_expired_trash`` — the one-line
  entrypoint the task calls. All DB / disk work lives in the
  service to keep the task thin + testable.
* Schema ``user_files.deleted_at`` column — requires the infra
  ALTER to have landed. If the column is missing the task will
  raise an OperationalError; the task returns "Failed" so the
  scheduler surfaces the problem in the next Jobs cycle rather
  than silently skipping.
"""
from __future__ import annotations

from services.fileStorageService import (
    FileStorageService,
    VAULT_RETENTION_DAYS,
)
from services.tasks.baseTask import BaseTask
from utils.logger import Logger


# 24 hours in minutes — standard daily cadence (matches
# BackfillAMFIHistoricalNAVsTask, InstrumentsCacheTask).
_RUN_INTERVAL_MINUTES = 24 * 60


class VaultTrashCleanupTask(BaseTask):
    """Daily hard-delete of vault files past the retention window."""

    _instance = None

    def __new__(cls, *args, **kwargs):
        if not cls._instance:
            cls._instance = super().__new__(cls)
        return cls._instance

    def __init__(self, title, priority):
        if not hasattr(self, "initialized"):
            super().__init__(title, priority)
            self.logger = Logger(__name__).get_logger()
            self._file_storage_service = FileStorageService()
            self.interval = _RUN_INTERVAL_MINUTES
            self.initialized = True

    def run(self):
        """BaseTask contract: returns ``(message, status, interval)``.

        Status conventions:
          * ``Completed`` — sweep ran; count may be 0 (no expired
            files) or >0 (normal operation).
          * ``Failed``    — DB / disk exception; message carries
            the exception class. Next scheduler tick retries.
        """
        try:
            dropped = self._file_storage_service.cleanup_expired_trash(
                retention_days=VAULT_RETENTION_DAYS,
            )
            msg = (
                f"Vault trash cleanup: hard-dropped {dropped} "
                f"file(s) past {VAULT_RETENTION_DAYS}-day retention"
            )
            self.logger.info(msg)
            return msg, "Completed", self.interval
        except Exception as exc:
            # No file contents / filenames in the message — §23 #4.
            err_msg = (
                f"Vault trash cleanup failed: "
                f"{type(exc).__name__}: {str(exc)[:200]}"
            )
            self.logger.error(err_msg, exc_info=True)
            return err_msg, "Failed", self.interval
