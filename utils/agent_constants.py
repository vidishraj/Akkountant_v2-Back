"""ak-yg2o FIX2 (2026-11-12): single source of truth for agent-model pin.

ak-rde9 Bead A (2026-11-12) pinned the Claude model ID across all 3
agent dispatchers (interactive chat / cron scheduler / wealth digest)
to prevent alias-drift (same incident class as ak-znk gold
unit-drift). The initial ship had the pre-ak-yg2o literal (see the
smoke matrix below for the exact value) duplicated in 3 files with
cross-reference comments.

ak-h36s MINOR (2026-11-12) consolidated the literal here: a future
bump that updates one file and misses another re-introduces exactly
the alias-drift the pin was defending against — just at the
string-literal level instead of the alias level. One file now; all
dispatchers import.

ak-yg2o FIX2 (2026-11-12): mayor gated Bead A merge on a live-SDK
smoke. Lead ran that smoke against the initial pin value (the
sonnet-4-7 alias form, first row of the matrix below); the SDK
returned ``unrecognized_model``. The mocked unit tests (which assert
pin presence but stub the SDK) did not — and could not — catch this.
Overseer re-picked a known-good ID from the live-smoke matrix:

    claude-sonnet-4-7            ❌ unrecognized_model  (original pin)
    claude-sonnet-4-7-20251101   ❌ unrecognized_model
    claude-sonnet-4-latest       ❌ unrecognized_model
    claude-sonnet-4              ❌ unrecognized_model
    claude-sonnet-4-5            ✅
    claude-sonnet-4-5-20250929   ✅
    sonnet                       ✅
    claude-opus-4-7              ✅  ← Overseer pick (class switch to Opus)

Class switch rationale (Overseer, 2026-11-12 08:40Z): strongest
reasoning in the current lineup; acceptable cost/latency trade for
the investment agent's role. All three dispatchers (chat / cron /
digest) now run on Opus, not Sonnet. The constant name
``AGENT_MODEL_ID`` is model-class-agnostic; no rename needed.

## Usage

::

    from utils.agent_constants import AGENT_MODEL_ID

    options = ClaudeAgentOptions(model=AGENT_MODEL_ID, ...)

## Bump procedure

To bump the model: edit the literal below only. All three dispatchers
(and any future consumer) pick up the new value on next deploy.

**Before shipping a bump, run the live-SDK smoke** — mocked unit
tests cannot catch an unrecognized model ID. The ak-yg2o incident is
the reference failure here. The smoke is the test
``test_pinned_model_id_resolves_against_live_sdk`` in
``test_investment_agent_foundation.py``; it is gated behind
``SMOKE_SDK=1`` so local pytest runs don't incur SDK cost or require
credentials.

Local-dev smoke (one real SDK call against the pinned ID, ~60s)::

    SMOKE_SDK=1 pytest test_investment_agent_foundation.py -k smoke -v

CI should set ``SMOKE_SDK=1`` on the branch-protection run so every
bump is live-verified before merge.

In addition, verify downstream behavior with the usual rate-task
smoke after pin changes — the model's structured-output behavior
under ``output_format={"type": "json_schema", ...}`` is the surface
most sensitive to version shifts.
"""

# The pinned Claude model ID used by every agent dispatcher in the
# codebase. See module docstring for the ak-znk / ak-rde9 / ak-h36s /
# ak-yg2o policy chain and the live-smoke matrix behind this value.
AGENT_MODEL_ID = "claude-opus-4-7"
