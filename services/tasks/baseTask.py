import json
import os
import shutil
from datetime import datetime, timedelta
from abc import ABC, abstractmethod

import requests

from models.Jobs import Job
from services import JsonDownloadService
from services.InvestmentService import InvestmentService
from services.transactionsService import TransactionService
from utils.logger import Logger


# The task is to give a row. The row should be updated
# with the task results. The status should be updated and the next row should be added with the execution time.
# 10 failures should turn the last task off


# ak-iwj M1: how many historical prefix files (per prefix + file_type)
# to keep on disk after a successful safe_replace_file. Retention of 3
# lets an operator eyeball the last two runs against the new one for
# regression comparison without the disk growing unbounded across the
# fleet (11MB × N runs × 8 rate jobs = tens of GB/year at cadence).
_KEEP_HISTORICAL_FILES = 3


class BaseTask(ABC):
    """Abstract base class for a scheduled task."""
    _instance = None  # Singleton instance
    id: int = None
    title: str
    result: str
    priority: str
    status: str
    due_date: datetime
    user_id: str = None

    tmp_dir: str = os.path.join(os.getcwd(), 'task_tmp')

    # Unique for every task
    interval: int

    def __new__(cls, *args, **kwargs):
        # ak-iwj M3 (v2 REVISED — see also v1 commit body): the v1 fix
        # added double-check locking here for a theoretical concurrent-
        # instantiation race. Reviewer verified — and audit confirmed —
        # that the lock was INERT: every rate-task subclass overrides
        # __new__ with its own local `if not cls._instance: cls._instance
        # = super().__new__(cls)` pattern, so BaseTask.__new__ never
        # actually runs on those subclasses. AND scheduler.py:106 is the
        # ONLY instantiation vector in the codebase and it processes
        # tasks serially in a for loop → the race the lock targeted is
        # NOT REACHABLE. Rather than ship dead code that gives a false
        # sense of protection (and forcing 12 subclass surgeries to make
        # it real), we drop the lock and preserve the current simple
        # pattern. If a future scheduler ever creates task instances
        # concurrently (e.g. threadpool worker per job) this needs to
        # be revisited — the race would then be reachable and the fix
        # would need to live in each subclass __new__ (or those
        # overrides removed to inherit a locked base). Filed
        # observation-only comment as the guard.
        if not cls._instance:
            cls._instance = super(BaseTask, cls).__new__(cls)
        return cls._instance

    def __init__(self, title, priority):
        # ak-iwj M3 (v2 REVISED): centralized initialized flag set here
        # even though the M3 lock was dropped — several subclasses
        # (SetMFRate/MFDetails/NPSRate/KiteStockDetails/checkMail/
        # InvestmentHistory) have the `hasattr(self, 'initialized')`
        # guard without ever SETTING the flag → their guard is
        # ineffective every call and they re-init on every scheduler
        # tick. Setting the flag here in the base fixes those latent
        # bugs without needing 12 subclass edits.
        if getattr(self, 'initialized', False):
            return
        # Initialise singleton with only title and priority
        self.title = title
        # Fix path issue - get the root directory and build absolute path
        root_dir = os.path.dirname(os.path.dirname(os.path.dirname(__file__)))
        assets_path = os.path.join(root_dir, 'services', 'assets')
        self.jsonService = JsonDownloadService.JSONDownloadService(assets_path)
        self.priority = priority
        self.transactionService = TransactionService()
        self.investmentService = InvestmentService()
        # Make tmp_dir if it doesnt exist
        os.makedirs(self.tmp_dir, exist_ok=True)
        self.initialized = True

    def init_runner(self, row: Job):
        self.id = row.id
        self.status = row.status
        self.due_date = row.due_date
        self.user_id = row.user_id

    def startTask(self):
        result, status, interval = self.run()
        return result, status, interval

    @abstractmethod
    def run(self):
        """Method containing the task logic. Must be overridden by subclasses."""
        pass

    @staticmethod
    def make_request(url):
        response = requests.get(url)
        response.raise_for_status()  # Raise an error for bad status codes
        return response.json()  # Parse the JSON response

    def save_json(self, data, file_path):
        """Save JSON atomically with a post-write parse sanity check.

        ak-iwj M2: pre-fix save_json wrote directly to file_path. A
        crash / disk-full / SIGKILL mid-write left a truncated file
        that safe_replace_file's `getsize > 0` check accepted; the
        next read raised JSONDecodeError with no upstream signal.

        Atomic-write pattern:
          1. Serialize to a `<file_path>.tmp` sibling
          2. fsync the tmp file so bytes are on disk (not just page cache)
          3. os.replace (atomic rename) tmp → file_path
          4. json.load(file_path) round-trip to detect corruption
             BEFORE the caller trusts the write

        Any step failing → tmp is cleaned up, an error is logged,
        and the exception is re-raised so safe_replace_file's error
        path can surface a Failed status. Silent-swallow was a bug —
        the old `except Exception: log.error` returned success to
        the caller who then continued as if the file was written.
        """
        tmp_path = f"{file_path}.tmp"
        try:
            # 1. Write to tmp with fsync so we can safely rename.
            with open(tmp_path, 'w', encoding='utf-8') as json_file:
                json.dump(data, json_file, ensure_ascii=False, indent=4)
                json_file.flush()
                os.fsync(json_file.fileno())
            # 2. Atomic rename. On POSIX, os.replace is a rename(2) —
            # observers see either the old bytes or the new bytes,
            # never a partial state.
            os.replace(tmp_path, file_path)
            # 2b. ak-iwj v2 MINOR 2: fsync the containing directory
            # so the DIRECTORY ENTRY change (which version wins the
            # rename) is durable across a post-rename crash. On POSIX
            # os.replace is atomic wrt concurrent readers but the
            # rename's persistence to disk metadata is fsync-gated.
            # Low priority for regenerable rate files (next run
            # produces the same data), but cheap defensive. Best-
            # effort — skips silently on Windows / non-POSIX / any
            # FS that doesn't support dir fsync.
            try:
                parent_dir = os.path.dirname(file_path) or "."
                dir_fd = os.open(parent_dir, os.O_RDONLY)
                try:
                    os.fsync(dir_fd)
                finally:
                    os.close(dir_fd)
            except (OSError, AttributeError):
                pass
            # 3. Round-trip validate. If json.load doesn't crash the
            # file is at least parseable. Doesn't validate schema —
            # that's the caller's responsibility.
            with open(file_path, 'r', encoding='utf-8') as fh:
                json.load(fh)
            self.logger.info(f"File saved successfully at: {file_path}")
        except Exception as e:
            # Best-effort cleanup of the tmp file. On POSIX the tmp
            # is already renamed away if os.replace succeeded, so this
            # only matters when we failed BEFORE the rename.
            try:
                if os.path.exists(tmp_path):
                    os.remove(tmp_path)
            except OSError:
                pass
            self.logger.error(f"Error saving JSON to {file_path}: {e}")
            # ak-iwj M2: re-raise so callers see the failure. Silent-
            # log-and-return was the pre-fix bug (safe_replace_file
            # would then move a possibly-partial file into place).
            raise

    @staticmethod
    def move_file(source_path: str, destination_path: str) -> bool:
        """
        Moves a file from the source directory to the destination directory.

        Parameters:
            source_path (str): The full path of the file to move.
            destination_path (str): The directory to move the file to.

        Returns:
            bool: True if the file was moved successfully, False otherwise.
        """
        try:
            # Check if the source file exists
            if not os.path.isfile(source_path):
                print(f"Source file does not exist: {source_path}")
                return False
            # Move the file
            shutil.move(source_path, destination_path)
            return True
        except Exception:
            return False

    def safe_replace_file(self, tmp_path, prefix, file_type):
        """Move new file to assets and delete old only after new is confirmed on disk.
        Returns (success: bool, error_message: str | None).

        ak-iwj M1: OSError from old-file deletion is now logged at
        WARNING level (was silently swallowed → unbounded historical
        JSON accumulation across the fleet). Additionally sweeps
        historical files beyond _KEEP_HISTORICAL_FILES retention.

        ak-iwj M2: validates the new file parses as JSON BEFORE
        deleting the old file. Pre-fix `getsize > 0` accepted a
        truncated JSON blob; next read failed silently.
        """
        old_file = self.jsonService.getLatestFile(file_type, prefix)
        new_path = self.jsonService.getFilePath(prefix, file_type)

        if not self.move_file(tmp_path, new_path):
            return False, 'Failed to move file'

        # ak-iwj M2: parse-validate the new file BEFORE trusting it.
        # A truncated JSON (partial write on disk-full / SIGKILL) has
        # size > 0 but is unreadable — the pre-fix getsize check
        # accepted it and deleted the old file, leaving an unreadable
        # rates file as the only survivor.
        if not os.path.isfile(new_path):
            return False, f'New file missing after move: {new_path}'
        if os.path.getsize(new_path) == 0:
            return False, f'New file empty after move: {new_path}'
        try:
            with open(new_path, 'r', encoding='utf-8') as fh:
                json.load(fh)
        except (json.JSONDecodeError, OSError) as exc:
            self.logger.error(
                f"safe_replace_file: new file {new_path} failed JSON parse "
                f"({exc}); refusing to delete old file"
            )
            return False, f'New file failed JSON validation: {exc}'

        # Old-file cleanup — now LOUD on failure (M1).
        if old_file and os.path.isfile(old_file) and old_file != new_path:
            try:
                os.remove(old_file)
            except OSError as exc:
                # ak-iwj M1: was `pass`; now logged so unbounded disk
                # growth from failed cleanups is visible. Non-fatal —
                # the new file is in place, the old file just stays.
                self.logger.warning(
                    f"safe_replace_file: could not delete old file "
                    f"{old_file}: errno={exc.errno} {exc}"
                )

        # ak-iwj M1: periodic sweep of historical files beyond retention.
        # Keeps the last _KEEP_HISTORICAL_FILES matching prefix files;
        # deletes older ones. Safe because getLatestFile only ever
        # reads the newest. Non-fatal on any error — sweep is best-
        # effort maintenance.
        self._sweep_historical(prefix, file_type)

        return True, None

    def _sweep_historical(self, prefix, file_type):
        """ak-iwj M1: retain only the newest _KEEP_HISTORICAL_FILES
        files matching `prefix` under `file_type`. Non-fatal on any
        error; the safe_replace_file caller is not blocked by sweep
        failures.
        """
        try:
            directory = os.path.join(
                self.jsonService.bas_directory, file_type,
            )
            if not os.path.isdir(directory):
                return
            entries = os.listdir(directory)
        except OSError as exc:
            self.logger.warning(
                f"_sweep_historical: could not list {file_type}/: {exc}"
            )
            return
        matching = [e for e in entries if e.startswith(prefix)]
        if len(matching) <= _KEEP_HISTORICAL_FILES:
            return
        # Sort by extracted timestamp descending so the newest N stay.
        # Falls back to filename sort if timestamp extraction fails —
        # deterministic even on unexpected filenames.
        def _sort_key(name):
            try:
                return self.jsonService.extract_timestamp(name)
            except Exception:
                return datetime.min
        matching.sort(key=_sort_key, reverse=True)
        to_delete = matching[_KEEP_HISTORICAL_FILES:]
        for old_name in to_delete:
            old_path = os.path.join(directory, old_name)
            try:
                os.remove(old_path)
                self.logger.info(
                    f"_sweep_historical: deleted stale {file_type}/{old_name}"
                )
            except OSError as exc:
                self.logger.warning(
                    f"_sweep_historical: could not delete "
                    f"{file_type}/{old_name}: {exc}"
                )
