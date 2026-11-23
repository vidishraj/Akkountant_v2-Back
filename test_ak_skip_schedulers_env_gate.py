"""ak-dzla: regression tests for the AK_SKIP_SCHEDULERS env gate
that prevents the ak-kite-ws.service entrypoint from spawning a
duplicate set of CronAgent / CheckMailUnifiedTask /
InvestmentSnapshotTask daemon threads.

Root cause (per ak-dzla bead + journalctl forensics 2026-11-23
08:23Z, post-ak-uvjk install):
  * scripts/kite_ws_daemon_entrypoint.py imports
    ``from app import app`` to bootstrap Flask app context
    (DB session + config).
  * Akkountant.__init__ → self._setup_schedulers() spawns
    CronAgent, CheckMailUnifiedTask, InvestmentSnapshotTask
    daemon threads.
  * The fcntl lock inside _setup_schedulers is per-process-group
    scope (LOCK_NB-with-skip for daemon exclusivity), so
    ak-kite-ws.service — a SEPARATE systemd unit — does NOT see
    app.service's lock holder and happily spawns a second copy.
  * Worst consequence: 2x InvestmentSnapshot rows per day at
    11PM IST → §2 accounting-correctness violation.

Fix (this bead): Overseer-selected Option B — environment-gate
inside _setup_schedulers, symmetric to the existing
``ENV=='LOCAL'`` guard. The ak-kite-ws unit file sets
``Environment=AK_SKIP_SCHEDULERS=1``; the BE honors it with an
early return BEFORE the fcntl lock + daemon spawn.

What these tests pin down:
  (1)  _setup_schedulers body contains the AK_SKIP_SCHEDULERS
       guard with the exact ``'1'`` match semantics.
  (2)  The env guard early-returns BEFORE the fcntl lock-file
       is opened (positional — a later refactor that moves it
       AFTER the lock acquire would defeat the fix).
  (3)  deploy/ak-kite-ws.service contains the
       ``Environment=AK_SKIP_SCHEDULERS=1`` line under [Service].
  (4)  Behavior: with the env var set to '1', invoking the
       method on a mock self never opens the lock file, never
       enters app_context, and never constructs any daemon
       class.
  (5)  Behavior: with the env var UNSET (or any value other
       than '1'), the method proceeds past the gate (lock-file
       open is attempted).

Belt-and-brace: the existing ENV=='LOCAL' guard stays untouched
(preserves local-dev opt-out); the fcntl scheduler lock stays
LOCK_NB (preserves daemon exclusivity invariant from ak-htnb).
"""
from __future__ import annotations

import ast
import os
import re
import sys
import unittest
from unittest import mock


sys.path.insert(0, os.path.dirname(os.path.abspath(__file__)))


def _read_source(rel_path):
    here = os.path.dirname(os.path.abspath(__file__))
    with open(os.path.join(here, rel_path), "r") as f:
        return f.read()


def _slice_setup_schedulers_body(src):
    """Return the _setup_schedulers method body as a string
    (everything between ``def _setup_schedulers(self):`` and the
    next sibling ``def ``)."""
    m = re.search(
        r"def _setup_schedulers\(self\):(.*?)(?=\n    def [a-zA-Z_])",
        src, re.DOTALL,
    )
    assert m, "_setup_schedulers body not found in app.py"
    return m.group(1)


# ─── Source-grep drift guards ─────────────────────────────────


class TestSetupSchedulersEnvGateShape(unittest.TestCase):
    """Pins the AK_SKIP_SCHEDULERS gate inside _setup_schedulers.
    If someone removes the gate, changes its match semantics, or
    moves it AFTER the fcntl lock acquire, these red.
    """

    def setUp(self):
        self.src = _read_source("app.py")
        self.body = _slice_setup_schedulers_body(self.src)

    # (1) Env gate present with exact '1' match.
    def test_ak_skip_schedulers_guard_present(self):
        self.assertIsNotNone(
            re.search(
                r"os\.getenv\(\s*['\"]AK_SKIP_SCHEDULERS['\"]\s*\)"
                r"\s*==\s*['\"]1['\"]",
                self.body,
            ),
            "ak-dzla: _setup_schedulers must guard on "
            "os.getenv('AK_SKIP_SCHEDULERS') == '1' — the "
            "ak-kite-ws unit file sets this to force an early "
            "return. Exact '1' match is required; truthy-check "
            "would let 'false'/'0' through.",
        )

    # (2) Guard early-returns BEFORE the fcntl lock-file open.
    #     Positional pin — a refactor that moves the guard AFTER
    #     the lock acquire would defeat the fix (ak-kite-ws
    #     would still open the lock, still try to spawn daemons
    #     after the lock resolves).
    def test_ak_skip_schedulers_guard_before_lock_acquire(self):
        guard_match = re.search(
            r"AK_SKIP_SCHEDULERS", self.body,
        )
        lock_match = re.search(
            r"_scheduler_lock_fd\s*=\s*open\(", self.body,
        )
        self.assertIsNotNone(
            guard_match,
            "AK_SKIP_SCHEDULERS guard not found in "
            "_setup_schedulers body",
        )
        self.assertIsNotNone(
            lock_match,
            "scheduler lock-file open() not found in "
            "_setup_schedulers body — shape changed, review "
            "this drift guard",
        )
        self.assertLess(
            guard_match.start(), lock_match.start(),
            "ak-dzla: AK_SKIP_SCHEDULERS guard must come BEFORE "
            "the scheduler lock-file open — otherwise "
            "ak-kite-ws still opens the lock and proceeds to "
            "spawn duplicate daemons after fcntl resolves.",
        )

    # The guard must early-return (not just log + continue).
    def test_ak_skip_schedulers_guard_returns(self):
        # Slice from the guard match to the next blank line /
        # next if — the guard block must contain a `return`.
        guard_slice = re.search(
            r"os\.getenv\(\s*['\"]AK_SKIP_SCHEDULERS['\"]\s*\)"
            r"\s*==\s*['\"]1['\"]\s*:(.*?)(?=\n        [a-zA-Z_#]|\n    def )",
            self.body, re.DOTALL,
        )
        self.assertIsNotNone(
            guard_slice,
            "AK_SKIP_SCHEDULERS guard body not parseable — "
            "expected an `if` block with `return`",
        )
        self.assertIn(
            "return", guard_slice.group(1),
            "ak-dzla: AK_SKIP_SCHEDULERS guard must `return` "
            "to short-circuit scheduler setup — a log-only "
            "guard would still spawn the duplicate daemons.",
        )

    # Backward-compat: the pre-existing ENV=='LOCAL' guard must
    # stay (local-dev opt-out).
    def test_local_env_guard_preserved(self):
        self.assertIsNotNone(
            re.search(
                r"os\.getenv\(\s*['\"]ENV['\"]\s*\)\s*==\s*['\"]LOCAL['\"]",
                self.body,
            ),
            "ak-dzla must NOT remove the ENV=='LOCAL' guard — "
            "that path is the local-dev opt-out and is "
            "orthogonal to AK_SKIP_SCHEDULERS.",
        )

    # Backward-compat: the scheduler fcntl lock must stay
    # LOCK_NB (daemon exclusivity invariant from ak-htnb).
    def test_scheduler_lock_still_lock_nb(self):
        self.assertIn(
            "LOCK_NB", self.body,
            "ak-dzla must NOT change the scheduler fcntl lock "
            "from LOCK_NB — that semantic (daemon exclusivity "
            "within a gunicorn worker group) was pinned by "
            "ak-htnb and is orthogonal to the AK_SKIP_"
            "SCHEDULERS gate.",
        )


# ─── Systemd unit file drift guard ────────────────────────────


class TestKiteWsServiceUnitHasEnvGate(unittest.TestCase):
    """Pins the matching Environment= line in the ak-kite-ws
    unit file. If someone removes it, the BE gate stops firing
    on the standalone daemon, and duplication returns.
    """

    def test_unit_file_sets_ak_skip_schedulers_env(self):
        src = _read_source("deploy/ak-kite-ws.service")
        self.assertIsNotNone(
            re.search(
                r"^Environment\s*=\s*AK_SKIP_SCHEDULERS\s*=\s*1\s*$",
                src, re.MULTILINE,
            ),
            "ak-dzla: deploy/ak-kite-ws.service must contain "
            "`Environment=AK_SKIP_SCHEDULERS=1` under [Service] "
            "so the standalone daemon's imported Akkountant "
            "app bypasses _setup_schedulers. Without this line "
            "the BE-side gate is dormant and daemon "
            "duplication returns.",
        )


# ─── Behavior pin — AST-lift + mock ───────────────────────────
#
# _setup_schedulers lives on the Akkountant(Flask) class and
# depends on a long __init__ chain (DB, Firebase, Google auth,
# KiteService, …) we don't want to boot in a unit test. Lift
# just the method AST, re-emit as a top-level function, and
# exercise it against a MagicMock self. This matches the AST-
# lift pattern used in test_kite_ws_daemon.py (entrypoint
# providers) and test_db_init_race_fix.py.


def _lift_setup_schedulers_as_function():
    """Parse app.py, extract the `_setup_schedulers` method,
    re-emit it as a standalone function `_lifted(self)` so a
    MagicMock self can drive it. Preserves module-level imports
    (os, fcntl) but skips the Flask class scaffolding."""
    src = _read_source("app.py")
    tree = ast.parse(src)
    target = None
    for node in ast.walk(tree):
        if (isinstance(node, ast.FunctionDef)
                and node.name == "_setup_schedulers"):
            target = node
            break
    assert target is not None, (
        "_setup_schedulers not found by AST walk"
    )
    # Rename to _lifted and strip decorators so we can exec it
    # as a free function.
    target.name = "_lifted"
    target.decorator_list = []
    module = ast.Module(body=[target], type_ignores=[])
    code = compile(module, filename="<ast-lift>", mode="exec")
    ns = {}
    # Provide the imports the method body uses. We inject
    # mockable `os`, `fcntl` so the test can swap them, but
    # default them to real modules so unrelated attrs work.
    import fcntl as _fcntl
    ns["os"] = os
    ns["fcntl"] = _fcntl
    # CronAgent / CheckMailUnifiedTask / InvestmentSnapshotTask
    # are referenced only AFTER the gate fires on env-set; for
    # env-unset we stop at the lock-file open() before they're
    # reached. Still, inject stubs so an attribute lookup in
    # the compiled function doesn't NameError even if the fix
    # ever drifts.
    ns["CronAgent"] = mock.MagicMock(name="CronAgent")
    ns["CheckMailUnifiedTask"] = mock.MagicMock(
        name="CheckMailUnifiedTask",
    )
    exec(code, ns)
    return ns["_lifted"]


class TestSetupSchedulersEnvGateBehavior(unittest.TestCase):
    """Exercises the AST-lifted method against a mock self."""

    def setUp(self):
        self._lifted = _lift_setup_schedulers_as_function()

    # (4) With AK_SKIP_SCHEDULERS=1, the method must return
    # before opening the lock file.
    def test_env_set_returns_before_lock(self):
        fake_self = mock.MagicMock(name="self")
        with mock.patch.dict(
            os.environ,
            {"AK_SKIP_SCHEDULERS": "1", "ENV": "PROD"},
            clear=False,
        ):
            # Confirm the LOCAL guard is NOT the one firing.
            with mock.patch("builtins.open") as open_mock:
                self._lifted(fake_self)
        self.assertFalse(
            open_mock.called,
            "ak-dzla: with AK_SKIP_SCHEDULERS=1, "
            "_setup_schedulers must NOT open the scheduler "
            "lock file — the gate must short-circuit before "
            "the fcntl stanza.",
        )
        # self.app_context() must NOT have been entered —
        # daemons are only constructed inside the `with
        # self.app_context():` block.
        self.assertFalse(
            fake_self.app_context.called,
            "ak-dzla: with AK_SKIP_SCHEDULERS=1, "
            "_setup_schedulers must return before entering "
            "self.app_context() — otherwise daemon "
            "construction could still proceed.",
        )
        # self.logger.info must have been called with a message
        # that mentions the skip reason (operator visibility).
        info_calls = [
            str(c) for c in fake_self.logger.info.call_args_list
        ]
        self.assertTrue(
            any("AK_SKIP_SCHEDULERS" in c for c in info_calls),
            "ak-dzla: the env-gate skip must log a message "
            "naming AK_SKIP_SCHEDULERS so operators see why "
            "the standalone unit is scheduler-less in the "
            "journal. Logs seen: " + repr(info_calls),
        )

    # (5) With AK_SKIP_SCHEDULERS unset (and ENV!='LOCAL'), the
    # method must proceed past the gate — attested by the lock
    # file open() being attempted.
    def test_env_unset_proceeds_past_gate(self):
        fake_self = mock.MagicMock(name="self")
        # Clear both guards' env vars; mark ENV as PROD.
        env_patch = {"ENV": "PROD"}
        with mock.patch.dict(os.environ, env_patch, clear=False):
            # Also delete AK_SKIP_SCHEDULERS if it leaked in.
            os.environ.pop("AK_SKIP_SCHEDULERS", None)
            with mock.patch("builtins.open") as open_mock:
                # The fcntl.flock call will raise IOError in
                # our environment (fake file) — simulate the
                # "another worker owns the lock" skip path so
                # we exit cleanly without touching app_context.
                open_mock.return_value = mock.MagicMock()
                with mock.patch(
                    "fcntl.flock",
                    side_effect=IOError("simulated lock held"),
                ):
                    self._lifted(fake_self)
        self.assertTrue(
            open_mock.called,
            "ak-dzla: with AK_SKIP_SCHEDULERS unset (and "
            "ENV!='LOCAL'), _setup_schedulers must proceed to "
            "the fcntl lock-file open() — if it short-circuits "
            "here the fix has regressed into always-skipping.",
        )

    # (6) A value other than exact '1' (e.g. '0', 'false') must
    # NOT trigger the skip — exact-match semantics.
    def test_env_set_to_zero_does_not_skip(self):
        fake_self = mock.MagicMock(name="self")
        with mock.patch.dict(
            os.environ,
            {"AK_SKIP_SCHEDULERS": "0", "ENV": "PROD"},
            clear=False,
        ):
            with mock.patch("builtins.open") as open_mock:
                open_mock.return_value = mock.MagicMock()
                with mock.patch(
                    "fcntl.flock",
                    side_effect=IOError("simulated lock held"),
                ):
                    self._lifted(fake_self)
        self.assertTrue(
            open_mock.called,
            "ak-dzla: AK_SKIP_SCHEDULERS='0' must NOT trigger "
            "the skip — exact '1' match is required so a "
            "truthy-check regression ('any non-empty string "
            "skips') would be caught here.",
        )


if __name__ == "__main__":
    unittest.main()
