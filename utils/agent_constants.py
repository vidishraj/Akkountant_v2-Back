"""ak-h36s MINOR (2026-11-12): single source of truth for agent-model pin.

ak-rde9 Bead A (2026-11-12) pinned the Claude model ID across all 3
agent dispatchers (interactive chat / cron scheduler / wealth digest)
to prevent alias-drift (same incident class as ak-znk gold
unit-drift). The initial ship had the literal `"claude-sonnet-4-7"`
duplicated in 3 files with cross-reference comments.

Reviewer MINOR on fc006c1: a future bump that updates one and misses
another re-introduces exactly the alias-drift the pin was defending
against — just at the string-literal level instead of the alias
level. Consolidated here so a model bump touches ONE file.

## Usage

::

    from utils.agent_constants import AGENT_MODEL_ID

    options = ClaudeAgentOptions(model=AGENT_MODEL_ID, ...)

## Bump procedure

To bump the model: edit this file only. All three dispatchers (and
any future consumer) will pick up the new value on next deploy.
Verify downstream behavior with the usual rate-task smoke test
after pin changes — the model's structured-output behavior under
``output_format={"type": "json_schema", ...}`` is the surface most
sensitive to version shifts.
"""

# The pinned Claude model ID used by every agent dispatcher in the
# codebase. See module docstring for the ak-znk / ak-rde9 / ak-h36s
# policy chain.
AGENT_MODEL_ID = "claude-sonnet-4-7"
