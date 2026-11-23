"""ak-htnb: regression tests for the first-boot db.create_all
race that caused the 2026-11-22 P0 crash cycle.

Root cause (per ak-htnb bead + journalctl forensics):
  * gunicorn.conf.py runs ``workers = 2`` without
    ``preload_app = True`` — each worker forks and
    independently imports app.py + calls
    ``_setup_database()`` → ``db.create_all()``.
  * With a fresh DDL batch on 2026-11-22 (ak-kxdr + ak-l8a7 +
    ak-05af), two workers raced on CREATE TABLE statements.
    One crashed exit=3 → systemd Restart=on-failure fired →
    second (sequential) boot succeeded. Crash lifted the
    ak-c8l Firebase middleware into memory without env vars
    → 30-min silent 401 cascade.

Fix (this bead): fcntl advisory lock around the DDL window
inside ``_setup_database``. BLOCKING acquire so the second
worker waits for the first rather than racing or skipping.
Belt-and-brace: ``preload_app = True`` gunicorn config is a
separate P2 follow-up.

What these tests pin down:
  (1)  ``_DB_INIT_LOCK_PATH`` constant exists on Akkountant.
  (2)  ``_setup_database`` acquires ``fcntl.LOCK_EX`` (blocking,
       NOT LOCK_NB) — scheduler lock uses LOCK_NB-with-skip;
       DDL guard must NOT.
  (3)  The lock is released in a ``finally`` block — any raise
       inside create_all still releases.
  (4)  Lock acquisition happens BEFORE ``self.db.create_all()``.
  (5)  Lock release includes an explicit ``LOCK_UN`` + close.
  (6)  Concurrent-worker race regression test: two threads
       entering a mock ``_setup_database``-style code path
       serialize on the lock (one holds while the other waits).
"""

from __future__ import annotations

import fcntl
import os
import re
import sys
import tempfile
import threading
import time
import unittest


sys.path.insert(0, os.path.dirname(os.path.abspath(__file__)))


def _read_source(rel_path):
    here = os.path.dirname(os.path.abspath(__file__))
    with open(os.path.join(here, rel_path), "r") as f:
        return f.read()


# ─── Source-grep drift guards ──────────────────────────────────


class TestSetupDatabaseLockShape(unittest.TestCase):
    """Pins the fcntl lock pattern around ``db.create_all()``. If
    someone removes the lock, drops the BLOCKING acquire, or
    relocates the create_all outside the lock window, these red.
    """

    def setUp(self):
        self.src = _read_source("app.py")

    # (1) Named constant for the lock path — keeps the path
    # consistent with the operator-side journal for triage.
    def test_db_init_lock_path_constant_present(self):
        self.assertIn(
            "_DB_INIT_LOCK_PATH", self.src,
            "ak-htnb: named constant for the DDL-lock path "
            "missing — operator triage needs a greppable path",
        )
        self.assertIn(
            "/tmp/akkountant_db_init.lock", self.src,
            "Lock path constant must resolve to "
            "/tmp/akkountant_db_init.lock",
        )

    # (2) BLOCKING LOCK_EX (NOT LOCK_NB) — the scheduler lock
    # above uses LOCK_NB-with-skip (correct for daemon
    # exclusivity); the DDL guard must WAIT so Worker B observes
    # the schema Worker A created.
    def test_setup_database_uses_blocking_lock_ex(self):
        # Slice _setup_database body
        m = re.search(
            r"def _setup_database\(self\):(.*?)(?=\n    def [a-zA-Z_])",
            self.src, re.DOTALL,
        )
        self.assertIsNotNone(m, "_setup_database body not found")
        body = m.group(1)
        # LOCK_EX appears
        self.assertIn(
            "fcntl.LOCK_EX", body,
            "DDL guard must acquire fcntl.LOCK_EX",
        )
        # LOCK_NB does NOT appear — nonblocking would reintroduce
        # the race (worker B skips → serves requests before DDL
        # observed).
        self.assertNotIn(
            "LOCK_NB", body,
            "DDL guard must be BLOCKING (no LOCK_NB flag); "
            "nonblocking would reintroduce the race",
        )

    # (4) Lock acquire happens BEFORE create_all — use AST so
    # docstring mentions of ``create_all()`` don't confuse the
    # position math.
    def test_lock_acquired_before_create_all(self):
        import ast as _ast
        tree = _ast.parse(self.src)
        setup_db = None
        for cls in tree.body:
            if isinstance(cls, _ast.ClassDef) and cls.name == "Akkountant":
                for m in cls.body:
                    if isinstance(m, _ast.FunctionDef) and m.name == "_setup_database":
                        setup_db = m
                        break
                break
        self.assertIsNotNone(setup_db, "_setup_database method missing")
        # Walk the body looking for the first fcntl.flock call and
        # the first self.db.create_all() call. These are both real
        # Call nodes — docstrings are Expr(Constant(str)).
        flock_line = None
        create_all_line = None
        for node in _ast.walk(setup_db):
            if isinstance(node, _ast.Call):
                src_fn = _ast.unparse(node.func) if hasattr(_ast, "unparse") else ""
                if src_fn == "fcntl.flock" and flock_line is None:
                    flock_line = node.lineno
                if src_fn == "self.db.create_all" and create_all_line is None:
                    create_all_line = node.lineno
        self.assertIsNotNone(flock_line, "fcntl.flock() call missing")
        self.assertIsNotNone(create_all_line, "create_all() call missing")
        self.assertLess(
            flock_line, create_all_line,
            f"fcntl.flock() at line {flock_line} MUST precede "
            f"create_all() at line {create_all_line}",
        )

    # (3) Release in finally block
    def test_lock_released_in_finally(self):
        m = re.search(
            r"def _setup_database\(self\):(.*?)(?=\n    def [a-zA-Z_])",
            self.src, re.DOTALL,
        )
        self.assertIsNotNone(m)
        body = m.group(1)
        self.assertIn(
            "finally:", body,
            "_setup_database must have a finally block so a "
            "raise inside create_all still releases the lock",
        )

    # (5) Explicit LOCK_UN + close in release path
    def test_lock_release_has_explicit_unlock(self):
        m = re.search(
            r"def _setup_database\(self\):(.*?)(?=\n    def [a-zA-Z_])",
            self.src, re.DOTALL,
        )
        self.assertIsNotNone(m)
        body = m.group(1)
        self.assertIn(
            "LOCK_UN", body,
            "release path must include explicit LOCK_UN",
        )
        self.assertIn(
            "close()", body,
            "release path must close the fd",
        )

    # Defensive: the None-check before unlock so a failed acquire
    # doesn't double-fault on release.
    def test_release_path_tolerates_failed_acquire(self):
        m = re.search(
            r"def _setup_database\(self\):(.*?)(?=\n    def [a-zA-Z_])",
            self.src, re.DOTALL,
        )
        self.assertIsNotNone(m)
        body = m.group(1)
        self.assertIn(
            "if lock_fd is not None", body,
            "release path must guard against lock_fd=None "
            "(acquire-failed case)",
        )

    # Backward-compat guard: the scheduler lock (different file,
    # different semantic) must stay LOCK_NB. Catches an over-
    # eager refactor that unifies both locks incorrectly.
    def test_scheduler_lock_still_lock_nb(self):
        # Scheduler lock lives in _setup_schedulers; its NB
        # semantics are correct (daemon-exclusivity). If someone
        # changes it to BLOCKING, every worker would start the
        # daemons — a different regression.
        m = re.search(
            r"def _setup_schedulers\(self\):(.*?)(?=\n    def [a-zA-Z_])",
            self.src, re.DOTALL,
        )
        self.assertIsNotNone(m, "_setup_schedulers body not found")
        sched_body = m.group(1)
        self.assertIn(
            "LOCK_NB", sched_body,
            "Scheduler lock must stay LOCK_NB (daemon "
            "exclusivity) — only the DDL lock is blocking",
        )


# ─── Concurrency simulation ────────────────────────────────────


class TestFcntlLockSerialization(unittest.TestCase):
    """Exercises the actual fcntl lock semantics in-process to
    confirm the fix works mechanically — two threads acquiring
    the same ``LOCK_EX`` on the same path serialize correctly.

    This is a MECHANICAL pin-down, not a mock of the gunicorn
    multi-process world. The lock's cross-process semantics are
    guaranteed by Linux fcntl; the test verifies our usage
    pattern (acquire + do-work + release) sequences correctly.
    """

    def setUp(self):
        self._tmpdir = tempfile.mkdtemp(prefix="akhtnb_")
        self._lock_path = os.path.join(self._tmpdir, "lock")

    def tearDown(self):
        try:
            os.remove(self._lock_path)
        except OSError:
            pass
        try:
            os.rmdir(self._tmpdir)
        except OSError:
            pass

    def _simulated_setup_database(self, work_duration, call_times):
        """Mirror the production ``_setup_database`` locking shape:
        open file, acquire LOCK_EX blocking, do work, release.
        Records entry + exit timestamps for the serialization
        assertion."""
        lock_fd = None
        try:
            lock_fd = open(self._lock_path, "w")
            fcntl.flock(lock_fd, fcntl.LOCK_EX)
            entered = time.monotonic()
            # "Do work" — simulate the DDL window
            time.sleep(work_duration)
            exited = time.monotonic()
            call_times.append((entered, exited))
        finally:
            if lock_fd is not None:
                try:
                    fcntl.flock(lock_fd, fcntl.LOCK_UN)
                except (OSError, ValueError):
                    pass
                try:
                    lock_fd.close()
                except OSError:
                    pass

    # (6) Serialization regression test — two threads on the
    # same lock cannot overlap their work windows.
    def test_two_workers_serialize_on_lock(self):
        call_times = []
        work = 0.1  # 100ms "DDL window"
        threads = [
            threading.Thread(
                target=self._simulated_setup_database,
                args=(work, call_times),
            )
            for _ in range(2)
        ]
        for t in threads:
            t.start()
        for t in threads:
            t.join()

        self.assertEqual(len(call_times), 2)
        # Sort by entry time; the second thread must have entered
        # AFTER the first thread exited (strict serialization).
        call_times.sort(key=lambda x: x[0])
        first_entered, first_exited = call_times[0]
        second_entered, second_exited = call_times[1]
        self.assertGreaterEqual(
            second_entered, first_exited,
            f"Two workers overlapped their DDL windows "
            f"(first: {first_entered}-{first_exited}, "
            f"second: {second_entered}-{second_exited}) — the "
            f"race is NOT closed",
        )

    # Mutation guard: if someone changes LOCK_EX to LOCK_SH,
    # concurrent SH acquires are allowed → work windows WOULD
    # overlap. This test pins the exclusive-lock semantic.
    def test_lock_sh_would_allow_overlap(self):
        """Not a regression test proper — documents that LOCK_SH
        would allow overlap. Serves as a trail marker for the
        design rationale. ``LOCK_EX`` is required."""
        call_times = []
        work = 0.1

        def shared_work():
            lock_fd = open(self._lock_path, "w")
            fcntl.flock(lock_fd, fcntl.LOCK_SH)  # SHARED — allows overlap
            entered = time.monotonic()
            time.sleep(work)
            exited = time.monotonic()
            call_times.append((entered, exited))
            fcntl.flock(lock_fd, fcntl.LOCK_UN)
            lock_fd.close()

        threads = [threading.Thread(target=shared_work) for _ in range(2)]
        for t in threads:
            t.start()
        for t in threads:
            t.join()
        # SH overlap is EXPECTED — threads run concurrently.
        # This isn't a regression; it's a documentation test.
        call_times.sort(key=lambda x: x[0])
        # Within one millisecond they entered concurrently — proving
        # LOCK_SH would NOT close the DDL race.
        first_entered, _ = call_times[0]
        second_entered, _ = call_times[1]
        self.assertLess(
            second_entered - first_entered, work,
            "LOCK_SH test premise failed — check fcntl impl",
        )


# ─── Docstring + comment drift guards ──────────────────────────


class TestRootCauseDocumented(unittest.TestCase):
    """Pin the root-cause docstring content so a future refactor
    that keeps the lock but silently drops the explanation can't
    leave future reviewers without context."""

    def setUp(self):
        self.src = _read_source("app.py")

    def test_docstring_mentions_concurrent_gunicorn_workers(self):
        self.assertIn(
            "gunicorn", self.src.lower(),
            "Root-cause docstring must name gunicorn (the "
            "actual process model that causes the race)",
        )

    def test_docstring_mentions_preload_app_followup(self):
        """preload_app = True is the long-term fix; docstring
        pins it as a known follow-up so reviewers don't
        reinvent the analysis."""
        self.assertIn(
            "preload_app", self.src,
            "Docstring must pin the preload_app = True follow-up "
            "fix candidate",
        )

    def test_docstring_mentions_ak_htnb(self):
        """Bead cross-ref so grep-by-bead-id surfaces this fix."""
        self.assertIn(
            "ak-htnb", self.src,
            "Code must carry ak-htnb bead reference",
        )


if __name__ == "__main__":
    unittest.main()
