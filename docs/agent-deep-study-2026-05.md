# Akkountant Backend — Claude SDK Agent Deep Study

**Date:** 2026-05-08
**Branch studied:** `Personal` (worktree: `agent/akkountant_backend`)
**Author:** akkountant_backend agent (research-only; no code changes)

---

## On-fire finding (flagged separately)

`utils/AIHelper.py:31 fetch_via_ai` has the same shape as the April outage:
`permission_mode='bypassPermissions'` + a system prompt that says "Use web
search to find the requested data" + **no `allowed_tools`**. Caller is
`services/StocksService.py:337` (IPO allotment price lookup for orphan-sell
reconciliation). The agent silently returns zero TextBlocks → `fetch_via_ai`
returns `None` → `StocksService` logs `"AI returned no results for IPO price
lookup"` and produces zero synthetic_buys. Orphan sells then appear
unreconciled with no surfaced error to the user. This was flagged via mail to
`akkountant_lead` before continuing the study. It also appears as **R1**
(trivial / high impact) in the recommendations table.

---

## Table of Contents

1. [SDK call-site inventory](#1-sdk-call-site-inventory)
2. [Transactions agents](#2-transactions-agents)
   - 2.1 [Agent-1: email classification](#21-agent-1-email-classification-mailprocessorservicepy386)
   - 2.2 [Text-email batch extractor](#22-text-email-batch-extractor-mailprocessorservicepy648)
   - 2.3 [PDF text-mode extractor (structured output)](#23-pdf-text-mode-extractor-mailprocessorservicepy1432)
   - 2.4 [PDF image-mode extractor + retry paths](#24-pdf-image-mode-extractor--retry-paths-mailprocessorservicepy1807-2003-2035)
3. [Investments agents](#3-investments-agents)
   - 3.1 [Rate fetchers (gold / EPF / PPF)](#31-rate-fetchers-airattaskpy--setibjagoldrate--setepfrate--setppfrate)
   - 3.2 [Stocks IPO allotment lookup](#32-stocks-ipo-allotment-lookup-utilsaihelperpy31)
   - 3.3 [Cron orchestrator](#33-cron-orchestrator-cronagentpy139)
4. [User-facing chat agent (transaction / investment / freelance personas)](#4-user-facing-chat-agent-agentservicepy132)
5. [Freelance scope](#5-freelance-scope)
6. [Job-applications email classifier (out of explicit scope but inventoried)](#6-job-applications-email-classifier-jobemailservicepy221)
7. [Cross-cutting analysis](#7-cross-cutting-analysis)
   - 7.1 [Model selection matrix](#71-model-selection-matrix)
   - 7.2 [`permission_mode` × `allowed_tools` matrix](#72-permission_mode--allowed_tools-matrix)
   - 7.3 [System prompts](#73-system-prompts)
   - 7.4 [Hooks](#74-hooks)
   - 7.5 [MCP servers](#75-mcp-servers)
   - 7.6 [Prompt caching](#76-prompt-caching)
   - 7.7 [Error capture pattern](#77-error-capture-pattern)
   - 7.8 [Health probes](#78-health-probes)
   - 7.9 [Observability](#79-observability)
8. [Prioritized recommendations table](#8-prioritized-recommendations-table)

---

## 1. SDK call-site inventory

Nine `query()` invocations across six files. Every site uses the
`claude_agent_sdk.query` async iterator pattern (no `ClaudeSDKClient`
sessions); MCP servers are always built in-process via `create_sdk_mcp_server`.

| # | File:line | Purpose | Model | max_turns | tools | permission_mode | output_format | comments |
|---|---|---|---|---|---|---|---|---|
| 1 | `utils/AIHelper.py:31` | Stocks IPO/allotment price lookup | sonnet | 6 | **none** | bypassPermissions | – | **🔥 same gap as April outage** |
| 2 | `services/tasks/AIRateTask.py:30` | Gold/EPF/PPF rate fetch (base for 3) | sonnet | 8 | WebSearch, WebFetch | bypassPermissions | – | recently fixed (`f15ef18`) |
| 3 | `services/agentService.py:132` | User-facing chat (3 personas) | sonnet | 20 | WebSearch, WebFetch + MCP | bypassPermissions | – | SSE streaming, dest-tool confirm |
| 4 | `services/cronAgent.py:139` | Autonomous data-freshness cron | sonnet | 10 | MCP only | bypassPermissions | – | rolling 5-cycle history |
| 5 | `services/mailProcessorService.py:386` | Email classifier (Agent-1) | opus | 1 | none | bypassPermissions | – | no tools needed at `max_turns=1` |
| 6 | `services/mailProcessorService.py:648` | Text-email batch extract | haiku | 50 | MCP only | bypassPermissions | – | 5-min asyncio timeout |
| 7 | `services/mailProcessorService.py:1432` | PDF text-mode extract | sonnet | 3 | none | bypassPermissions | **json_schema** | only site using `output_format` |
| 8 | `services/mailProcessorService.py:1807, 2003, 2035` | PDF image-mode extract + retries | sonnet | 50 | MCP only | bypassPermissions | – | three near-identical option blocks |
| 9 | `services/jobEmailService.py:221` | Job-application email classifier | (default) | 1 | none | (default) | – | older shape; no model pin |

Aggregate: 1 opus / 5 sonnet / 1 haiku / 1 unspecified (job emails) / 1 sonnet
(IPO). All sites live in synchronous code that wraps the SDK with
`anyio.run()`; concurrency comes from outer Flask threads, not SDK sessions.

---

## 2. Transactions agents

### 2.1 Agent-1: email classification (`mailProcessorService.py:386`)

**A. What it does**

Receives a batch of up to `CLASSIFICATION_BATCH_SIZE = 50` raw emails
(sender + subject + snippet + date), returns a JSON array
`[{gmail_id, category}, …]`. This *replaces* the older domain-allowlist
pre-filter — the model is the gate for "is this a financial email." Output
feeds into the text-email extractor (2.2) and PDF extractor (2.3/2.4).

**B. SDK feature inventory**

`mailProcessorService.py:386–391`
```python
ClaudeAgentOptions(
    model="opus",
    system_prompt=CLASSIFICATION_SYSTEM_PROMPT,   # ~70 lines, line 51
    max_turns=1,
    permission_mode="bypassPermissions",
)
```
- No `allowed_tools`, no MCP, no `output_format`. Pure text-in / JSON-out.
- 120-second outer timeout via `asyncio.wait_for` in `run_with_timeout`
  (`mailProcessorService.py:422-430`).
- `rate_limit` raises `RuntimeError` upstream; other errors return `[]`.

**C. Failure modes audit**

- ❌ **Silent `[]` on any non-rate-limit error** (line 433–436). A "Classification error: …"
  warning is logged but pipeline continues with empty result → financial
  emails dropped silently for that batch.
- ⚠️ **JSON parse fallback is forgiving** (line 445–475: direct → fenced → first
  `[`/`]`). On total parse failure returns `[]` and logs `"Could not parse
  classification response"` — same drop-on-floor.
- ⚠️ **`max_turns=1` + `bypassPermissions`** is *not* the April-outage shape
  (no tools requested in the system prompt), but it does mean any model
  refusal is invisible: the model can output zero TextBlocks and we'd see
  "empty response" + return `[]`.
- ⚠️ Model choice (**opus**) is the heaviest in the codebase for what is
  effectively a multi-class classification problem. Cost-per-batch is real.
- ✅ `rate_limit` correctly bubbles up as `RuntimeError` so the orchestrator
  can back off.

**D. Score against the four bars**

| Bar | Score | Note |
|---|---|---|
| No silent failures | ⛔ | Empty parse / non-rate-limit errors silently degrade |
| Measurable accuracy | ⛔ | No labelled fixtures, no confusion matrix; any "missed financial email" is invisible until a user notices missing transactions |
| Self-healing / retries | ⛔ | Single-shot; no retry on parse failure |
| Smarter outputs | ⛔ | Could enforce schema via `output_format`; doesn't |

**E. Recommended changes**

- **Trivial:** Switch to `output_format={"type":"json_schema","schema":...}`
  with `{ "classifications": [{ "gmail_id": "...", "category": "..." }] }`.
  Eliminates the 30 lines of regex JSON-extract; `structured_output` becomes
  authoritative; can downgrade model to **haiku** with the constraint.
- **Trivial:** When parse fails, mark *the whole batch* as
  `status='unprocessed_classification_failure'` in `processedEmails` so it
  retries next pipeline run, instead of dropping silently.
- **Structural:** Build a labelled fixture set (a few hundred real emails
  hand-labelled into the existing categories) and a daily test that runs
  the classifier and reports precision/recall per category. This is the
  single biggest "measurable accuracy" lever in the codebase.
- **Ambitious:** Replace this entire call with `prompt_caching` on the
  category list + few-shot examples (the prompt is huge, mostly static —
  the only varying part is the email batch). Should drop p50 latency
  significantly and is essentially free since the cache pays for itself
  within ~3 invocations.

---

### 2.2 Text-email batch extractor (`mailProcessorService.py:648`)

**A. What it does**

Takes classified financial emails that don't have PDF attachments and
extracts structured data via MCP tools (`insert_transaction`,
`insert_investment`, `insert_epf_deposit`, `mark_invoice_paid`,
`report_result`). Loop is "for each email in batch, call appropriate tool,
then call `report_result`".

**B. SDK feature inventory**

`mailProcessorService.py:648–654`
```python
ClaudeAgentOptions(
    model="haiku",
    system_prompt=TEXT_EMAIL_SYSTEM_PROMPT,   # mailProcessorTools.py:332
    max_turns=MAX_TURNS,                       # 50
    mcp_servers={MCP_SERVER_NAME: mcp_server},
    permission_mode="bypassPermissions",
)
```
- `_build_sdk_tools()` builds an in-process MCP server with seven tools
  (insert_transaction, insert_batch_transactions, insert_investment,
  insert_epf_deposit, insert_gold_purchase, mark_invoice_paid,
  report_result) — see `mailProcessorService.py:2092+`.
- 300-second outer timeout (line 689–697).

**C. Failure modes audit**

- ⚠️ **No `allowed_tools` set** — under `bypassPermissions`, MCP tools are
  *always* allowed (this is the SDK's contract), so this is technically OK,
  but it relies on the agent service's own "MCP tools registered via
  `mcp_servers` are always available" assumption. If that contract changes
  in a future SDK version, the silent-failure mode reappears here.
- ❌ **`return 0` on any non-rate-limit error** (line 700–703). Successful
  inserts that *did* happen mid-batch are still applied (DB writes already
  committed by the tool handler), but the count is lost.
- ⚠️ **`tool_calls_count = [0]`** is declared (line 664) but never
  incremented anywhere. Dead variable; observability gap.
- ⚠️ **Model = haiku, MAX_TURNS = 50.** Haiku may not have the reasoning
  depth to handle 15-email batches of mixed categories at peak. No
  fallback to sonnet on retry.
- ⚠️ **`is_error` from `ResultMessage` clobbers any partial success info**
  (line 685–687). The agent may have processed 12/15 emails before the
  stream errored, but the function returns 0.
- ⚠️ **No structured contract** for what the agent must produce — relies
  entirely on system prompt to ensure `report_result` is called. There is
  no enforcement that a `report_result` was emitted per email; if it
  wasn't, the email's row in `processedEmails` stays at its prior status.

**D. Score**

| Bar | Score | Note |
|---|---|---|
| No silent failures | ⛔ | Returns 0 on stream error, loses partial work |
| Measurable accuracy | ⛔ | No coverage check (X emails in vs Y reports out) |
| Self-healing / retries | ⛔ | None at this layer |
| Smarter outputs | 🟡 | Has MCP, but no caching, no structured-output assertion |

**E. Recommended changes**

- **Trivial:** Add `allowed_tools=[...]` listing the MCP tool names by
  fully-qualified name (`f"mcp__{MCP_SERVER_NAME}__insert_transaction"`,
  …). Belt-and-braces against the SDK contract drift mentioned above.
- **Trivial:** Track `tool_calls_count` in the MCP handler closures, log
  it at end-of-batch. Cheap observability win.
- **Structural:** After the batch, query `processedEmails` for any input
  gmail_id not in `report_result` events, and either retry just those or
  mark them `status='unreported'`. Closes the per-email coverage gap.
- **Structural:** Use a `Stop` hook (per SDK feature menu) to fail-loudly
  when the agent finishes without calling `report_result` for every
  expected gmail_id. Alternative to the post-hoc query above.
- **Ambitious:** Promote this from haiku to sonnet for batches > N% non-
  trivial categories (freelance_contract, brokerage_statement). Run
  haiku for transaction_alert-heavy batches.

---

### 2.3 PDF text-mode extractor (`mailProcessorService.py:1432`)

**A. What it does**

For text-extractable PDFs (HDFC e-statements, etc.) we run PyMuPDF to
extract text, send it to Claude with `output_format=json_schema`, get a
strict `{transactions: [...]}` array back, and feed it directly into
`_handle_insert_batch_transactions`. **This is the only call site in the
codebase that uses `output_format`.**

**B. SDK feature inventory**

`mailProcessorService.py:1432–1441`
```python
ClaudeAgentOptions(
    model="sonnet",
    system_prompt=PDF_SYSTEM_PROMPT,           # mailProcessorTools.py:388
    max_turns=3,
    permission_mode="bypassPermissions",
    output_format={
        "type": "json_schema",
        "schema": self._TXN_OUTPUT_SCHEMA,     # line 1585
    },
)
```
- The schema requires `date / description / amount / reference_number`
  per transaction (line 1585–1603).
- Per-chunk reconciliation (`_run_text_chunk` at line 1634) reads
  `message.structured_output` first, falls back to
  `AssistantMessage` text + JSON parse on failure.

**C. Failure modes audit**

- ✅ Best error capture in the codebase — `structured_output` then text
  fallback then "no structured output and no parseable fallback" with
  return `{called: False, inserted: 0, duplicates: 0}`.
- ⚠️ **`max_turns=3`** is "allow retries for structured output validation"
  per the comment (line 1435), but if the model emits invalid output
  three times we silently get zero transactions. No alert is raised.
- ⚠️ **Bank-format rules injection is dynamic** (line 1667–1680). If
  `get_format_rules()` returns `None` for an unknown bank, the prompt
  still works but the model has less context — no warning logged.
- ✅ Reference-number requirement is explicit; very good for dedup.
- ⚠️ **`reconciliation_service` is passed positionally** at line 1773.
  This was the bug in `d1e68e0` — fragile pattern.
- ⚠️ Coverage check (line 1517) compares `_count_expected_transactions`
  vs actual inserts; on `< 80%` coverage triggers one retry. **The retry
  does not change parameters** (same prompt, same options) — so the
  effective behavior is "shake it once and hope". If it's a stable model
  miss, retry is wasted spend.
- ⚠️ **No retry between text and image mode.** Text-mode failure dumps
  the chunk into `failed_chunks` and moves on. The image-mode path is
  technically available but never triggered as a fallback.

**D. Score**

| Bar | Score | Note |
|---|---|---|
| No silent failures | 🟡 | structured_output is checked; coverage-check exists; but `failed_chunks` is silent |
| Measurable accuracy | 🟢 | `coverage = actual / expected` is the closest thing to an in-flight accuracy metric |
| Self-healing / retries | 🟡 | One-shot retry exists, but doesn't vary prompt/params/model |
| Smarter outputs | 🟢 | Only call site using structured output — exemplar |

**E. Recommended changes**

- **Trivial:** When the same-prompt retry also produces `coverage < 0.8`,
  emit a `WARN: chunk-coverage-failed` log line with chunk range, bank,
  and `expected` vs `actual` so it's grep-able.
- **Structural:** On second-retry failure, fall through to image-mode
  (`_run_pdf_chunk_async`) for that chunk specifically. Two paths exist;
  the failover wiring doesn't.
- **Structural:** Vary the second retry — bump `max_turns` to 5,
  raise model to opus, or add a "you missed transactions on the previous
  attempt; here is the count we expected: N" prefix.
- **Ambitious:** Use prompt caching on `PDF_SYSTEM_PROMPT` + bank rules
  (both static for a given bank) — the only varying part is the
  extracted_text. This is a high-frequency code path during overnight
  scans; caching pays back fast.

---

### 2.4 PDF image-mode extractor + retry paths (`mailProcessorService.py:1807, 2003, 2035`)

**A. What it does**

Same goal as 2.3, but for PDFs where text extraction is unreliable (image-
based statements, scanned documents). The agent calls a `get_pdf_pages` MCP
tool that returns image data, then calls `insert_batch_transactions` on what
it sees. Three near-identical `ClaudeAgentOptions` blocks: initial,
stream-error retry, "agent didn't call tool" follow-up.

**B. SDK feature inventory**

`mailProcessorService.py:1807–1813` (and 2003–2009, 2035–2041 are byte-for-
byte identical):
```python
ClaudeAgentOptions(
    model="sonnet",
    system_prompt=system_prompt,               # = PDF_SYSTEM_PROMPT
    max_turns=MAX_TURNS,                       # 50
    mcp_servers={MCP_SERVER_NAME: mcp_server},
    permission_mode="bypassPermissions",
)
```
- 300-second timeout per attempt.
- Manual stream-error detection via keyword search in `error_msg` and
  `agent_output` (line 1979–1985). On match: rebuild MCP server (line
  1996–2009), retry once.
- "Agent talked but didn't call tool" follow-up (line 2018–2069) — a
  shorter prompt re-injecting the extracted text (text-mode only) and
  the strict "call insert_batch_transactions IMMEDIATELY" instruction.

**C. Failure modes audit**

- ❌ **Three duplicate `ClaudeAgentOptions` blocks** — drift risk. If
  someone fixes one (e.g., adds `allowed_tools`), the other two stay
  broken. This is a textbook case for a `_make_pdf_chunk_options(...)`
  helper.
- ⚠️ **Stream-error detection is keyword-string matching** ("stream
  closed", "broken pipe", …) on logs. Brittle to log message changes
  in the SDK.
- ⚠️ **Follow-up prompt has fewer guardrails** (no `total_pages`, no
  reconciliation hint) — fine in practice because the original chunk
  metadata is preserved server-side, but if the model now has less
  context for what chunk it's on, results may degrade.
- ⚠️ **`insert_called["value"]` mutation through closure** is the only
  signal that the tool ran. If the MCP server is rebuilt mid-flow (line
  1997), the *new* closure has its own `insert_called`, so the *old*
  one's success info is lost. Worked-this-time-but-fragile.
- ⚠️ Same "no allowed_tools" remark as 2.2 — relies on MCP-tools-always-
  allowed contract.

**D. Score**

| Bar | Score | Note |
|---|---|---|
| No silent failures | 🟡 | Has retries; final state when both retries fail returns `{"called": False, …}` and is silent at chunk level |
| Measurable accuracy | 🟡 | Coverage check exists for text mode, NOT replicated here for image mode |
| Self-healing / retries | 🟢 | Two distinct retry strategies (stream error vs no-tool-call). Best self-healing in repo |
| Smarter outputs | 🟡 | Could use structured output for known-good banks before falling back to image mode |

**E. Recommended changes**

- **Trivial:** Extract a `_make_pdf_chunk_options(system_prompt, mcp_server)`
  helper. Three identical blocks → one. Defends against fix drift.
- **Trivial:** Add `allowed_tools` listing the MCP tool names. Eliminates
  the contract-drift risk.
- **Structural:** A `Stop` hook that asserts `insert_called["value"] is
  True` on completion would surface the silent "no tool call" case at
  SDK boundary instead of after the fact.
- **Structural:** Replicate the text-mode coverage check
  (`_count_expected_transactions`) here — it's currently text-mode only.
- **Ambitious:** PreToolUse hook to validate `insert_batch_transactions`
  payload before execution (e.g., reject if any transaction has a
  null/empty `reference_number`, since dedup depends on it).

---

## 3. Investments agents

### 3.1 Rate fetchers (`AIRateTask.py` → `SetIBJAGoldRate` / `SetEPFRate` / `SetPpfRate`)

**A. What it does**

Three scheduled tasks that ask Claude to web-search for IBJA gold rates,
EPFO interest rate, and PPF rate respectively, and parse the JSON
response into the rate cache (`*.json` files in `tmp_dir`).

- `SetIBJAGoldRate.py:23–96` — daily, 5-hour interval
- `SetEPFRate.py:23–69` — weekly (10080 min)
- `SetPpfRate.py` — every 4 days (6000 min)

All three subclass `AIRateTask` and call `self.fetch_rates_via_ai(prompt)`.

**B. SDK feature inventory**

`AIRateTask.py:50–56`
```python
ClaudeAgentOptions(
    model="sonnet",
    system_prompt="You are a data extraction assistant. … Return raw JSON only.",
    max_turns=8,
    permission_mode="bypassPermissions",
    allowed_tools=["WebSearch", "WebFetch"],
)
```
- 120-second outer `anyio.run` timeout (default).
- Returns `(dict | None, str)` — the second slot is the error detail that
  gets stuffed into `jobs.result`. This is the **gold-standard error
  capture pattern** in the codebase (post-`f15ef18`).
- `is_error` from `ResultMessage` is captured (line 86–88).

**C. Failure modes audit**

- ✅ Empty-response detection with explicit "(no TextBlocks — likely
  missing allowed_tools or model refused)" hint (line 101).
- ✅ JSON-extract failure includes head of response in error string.
- ⚠️ **No retry on transient web-search failure.** A flaky upstream
  (ibjarates.com being slow) returns "Failed to get Gold Rates via AI:
  Empty response" on the next cron run; user-visible failure on the jobs
  table.
- ⚠️ **No model fallback.** If sonnet refuses or returns garbage twice
  in a row, the rate file is stale until manual intervention.
- ⚠️ Per-rate tasks hard-code their own JSON shape in their `prompt`
  string — drift between subclasses is possible. EPFRate uses a
  flat-list shape; GoldRate uses a deeply nested shape. No shared
  schema validation.
- ⚠️ **No per-task accuracy check.** "JSON had a `data` key" or "had a
  `24 Carat` key" is the only validation — the model could return
  yesterday's rates and we'd accept them.

**D. Score**

| Bar | Score | Note |
|---|---|---|
| No silent failures | 🟢 | `(jsonData, ai_err)` is the cleanest pattern in the codebase; failure detail lands in `jobs.result` |
| Measurable accuracy | ⛔ | Checks shape, not freshness. Stale rates pass silently |
| Self-healing / retries | ⛔ | None |
| Smarter outputs | 🟡 | Has WebSearch; could use citations / source verification |

**E. Recommended changes**

- **Trivial:** Add a date-recency check in each task. E.g., for
  GoldRate, `assert date in jsonData['ibja_data']['date'] is within last
  7 days`. Failure → bump `failures` counter on the job.
- **Structural:** Replace per-task ad-hoc JSON shapes with
  `output_format=json_schema`. Eliminates the "shape drifted" class
  entirely. Prompt becomes "search for X and fill in this schema".
- **Structural:** Promote the `(dict | None, str)` tuple-return to a
  shared base helper `(value, error_detail) → DiagnosticResult` and use
  it from `AIHelper.fetch_via_ai` (after R1) and a future `mail_processor`
  helper. Standardize the *contract*; don't let each call site invent.
- **Ambitious:** Citations. The SDK supports source-grounded outputs.
  For rate data, "what URL did you read this from" is auditable; today
  we have zero traceability when a wrong rate makes it into the cache.

---

### 3.2 Stocks IPO allotment lookup (`utils/AIHelper.py:31`)

**A. What it does**

`StocksService` reconciles "orphan sells" — sell orders with no matching
buy in the local data. For these, it asks Claude to find the IPO
allotment price (or first-day listing price for corporate actions) so it
can fabricate a synthetic buy record. Caller: `StocksService.py:337`.

**B. SDK feature inventory**

`utils/AIHelper.py:31–37`
```python
ClaudeAgentOptions(
    model="sonnet",
    system_prompt="You are a financial data assistant. Use web search …",
    max_turns=6,
    permission_mode="bypassPermissions",
)
```
- **No `allowed_tools`** despite a system prompt that says "Use web search".

**C. Failure modes audit**

- 🔥 **April-outage shape.** With `bypassPermissions` + no `allowed_tools`,
  the model has no tools and the system prompt forbids non-JSON text,
  so the response is zero TextBlocks → empty raw → `_extract_json`
  returns `None` → `fetch_via_ai` returns `None`.
- ❌ Caller (`StocksService.py:339-342`) logs a generic warning and
  returns `[]`. Orphan sells stay unreconciled with no surfaced error.
- ❌ Returns plain `dict | None` — no error detail propagates back, so
  not even a debug log can tell *why* this failed. No `(value, detail)`
  tuple like `AIRateTask`.
- ⚠️ Even after fixing R1, accuracy is unmeasured: "did the model find
  the right IPO" is unverified. Wrong allotment date / price will
  silently produce a wrong synthetic_buy.

**D. Score**

| Bar | Score | Note |
|---|---|---|
| No silent failures | ⛔ | Currently broken; even after fix, returns `None` with no detail |
| Measurable accuracy | ⛔ | None |
| Self-healing / retries | ⛔ | None |
| Smarter outputs | ⛔ | None — duplicates AIRateTask's pattern but worse |

**E. Recommended changes**

- **R1 (trivial, urgent):** Add `allowed_tools=["WebSearch","WebFetch"]`
  and bump return type to `(dict | None, str)` to match `AIRateTask`.
  This is literally the AIRateTask fix copy-pasted.
- **Structural:** Once R1 lands, **delete this file** and call
  `AIRateTask.fetch_rates_via_ai` (or a renamed shared helper) from
  `StocksService`. There is no good reason to maintain two web-searching
  helpers with the same shape but divergent guardrails.
- **Structural:** Use `output_format=json_schema` for the
  `{results: [{symbol, isin, allotment_price, allotment_date, source}]}`
  contract. Removes the bespoke regex.

---

### 3.3 Cron orchestrator (`cronAgent.py:139`)

**A. What it does**

Daemon thread that wakes every 30 minutes (within an allowed 1-7 AM IST
window — see `cronAgent.py:99-105`), asks Claude to (1) check rate-cache
freshness, (2) check pending jobs, (3) trigger refresh jobs for stale
data — and produces a one-paragraph summary that's appended to a rolling
5-cycle history. The history is fed back into the next prompt.

**B. SDK feature inventory**

`cronAgent.py:139–145`
```python
ClaudeAgentOptions(
    model="sonnet",
    system_prompt=SYSTEM_PROMPT,        # line 30, ~30 lines incl. freshness table
    max_turns=10,
    mcp_servers={MCP_SERVER_NAME: mcp_server},
    permission_mode="bypassPermissions",
)
```
- Three MCP tools: `get_rate_freshness`, `get_jobs_status`,
  `trigger_rate_refresh` (line 200–286).
- No outer timeout beyond Python defaults — cron cycle just blocks on
  `anyio.run`.
- On error, history gets `"[ERROR] {error_msg}"` so future cycles see
  that the previous one failed. Good context-passing pattern.

**C. Failure modes audit**

- ✅ Restricted-tool design — MCP exposes only the three "look at jobs
  table" tools; no general-purpose Bash etc.
- ⚠️ No `allowed_tools` listing the MCP tool names — same MCP-tools-
  always-allowed contract reliance as 2.2 / 2.4.
- ⚠️ **`max_turns=10`** with "check freshness, check jobs, trigger N
  refreshes" — typically uses 4–6 turns. If something goes wrong (e.g.,
  the model loops asking for the same data) we exhaust turns and
  silently produce a partial history entry. No alert.
- ⚠️ **No verification that triggered jobs actually ran.** Agent says
  "I triggered SetMfRate" → history records that → next cycle the
  agent sees stale data and may re-trigger. Job-table churn risk.
- ⚠️ **History grows unbounded by content size, not just count.** Five
  long summaries can blow up the prompt. No truncation per-entry.
- ⚠️ **No model fallback.** If sonnet is rate-limited at 1 AM, the
  whole cron skips a cycle.

**D. Score**

| Bar | Score | Note |
|---|---|---|
| No silent failures | 🟡 | Errors land in history but not in any alerting channel |
| Measurable accuracy | ⛔ | "Did the cron cycle correctly skip everything?" is unaudited |
| Self-healing / retries | ⛔ | None — a rate-limited cycle is a missed cycle |
| Smarter outputs | 🟡 | Has memory across cycles via rolling history; could be more compact |

**E. Recommended changes**

- **Trivial:** Truncate each history entry to ~500 chars before
  appending. The model only needs "did stuff / errored" not full prose.
- **Trivial:** Add a "what jobs did I trigger" structured tail in the
  agent's output (forced via `output_format`), persist that, and on
  next cycle pass it explicitly so the model doesn't have to remember.
- **Structural:** Health probe — if 3 consecutive cycles error, mail
  the user. Today the cron silently skips and rate caches go stale.
- **Structural:** Replace the natural-language "what's stale" reasoning
  with a deterministic Python check (the freshness table is already in
  Python in DateTimeUtil), and use Claude *only* for the trigger
  decision when the deterministic check is ambiguous. Cuts most cycles
  to zero LLM cost and makes the system testable.
- **Ambitious:** Move to a `Stop` hook that validates the agent's
  output structure before it returns. If the model triggers a refresh
  for a job that was already Pending, the hook can reject and force
  a re-think within `max_turns`.

---

## 4. User-facing chat agent (`agentService.py:132`)

**A. What it does**

Entry point: `controllers/agentEP.py:30` (`/api/agent/chat?type=…`,
SSE stream). One implementation, three personas selected by `agent_type`
(`investment` / `transaction` / `freelance`) — each persona has its own
`system_prompt` and tool list defined in `agent_tools.py:8 / 137 / 150`.
This is the only agent that streams text to the user, and the only one
that gates "destructive" tools behind a confirm step.

**B. SDK feature inventory**

`agentService.py:132–139`
```python
ClaudeAgentOptions(
    system_prompt=config["system_prompt"],
    mcp_servers={MCP_SERVER_NAME: mcp_server},
    permission_mode="bypassPermissions",
    max_turns=MAX_TURNS,                         # 20
    model="sonnet",
    allowed_tools=["WebSearch", "WebFetch"],
)
```
- **Only chat agent that pins both `model` AND `allowed_tools`** —
  reflects the post-April lesson.
- MCP server has 30+ tools (get_investments, get_transactions,
  get_invoices, create_invoice, …); see `agent_tools.py`.
- Destructive tools (line 29–39) are intercepted in the handler and
  return an `is_error: True` content block with a confirm message.
- Mutations (line 42–59) are accumulated and returned in the `done` SSE
  event so the frontend can refresh.

**C. Failure modes audit**

- ✅ Has the explicit "missing allowed_tools" log warning (line 184–187)
  if it ever returns zero text + zero tool events. Active sentinel.
- ⚠️ **Conversation context is flattened to a single string prompt**
  (`_format_conversation` at line 261). Multi-turn nuance like tool
  results from prior turns is lost — only `user`/`assistant` text
  survives. Agent loses memory of prior tool calls within the same
  chat session.
- ⚠️ **Bash explicitly excluded** for safety (line 129–131) — good.
  But that means agents can't even do trivial calculations; everything
  must go through MCP tools. May be too tight; opens an avenue for
  hallucinated math.
- ⚠️ **Confirmation flow is fragile.** When the user confirms, the
  *next* user message includes `confirmed_tools=[...]` and we re-run
  the entire conversation from scratch (line 105). The agent sees the
  full prior turn including the "Action cancelled" tool result. State
  reconstruction is implicit; no formal session.
- ⚠️ **`MAX_TURNS=20`** with 30+ MCP tools — the agent can spend a lot
  of turns hunting through `get_*` tools to answer a simple question.
  No per-tool-call cost telemetry.
- ⚠️ **No streaming partial assistant text.** The whole query is
  awaited (`anyio.run`) then the full text is yielded. The SSE
  abstraction is wasted — the user gets it as one chunk. This is a
  known issue from the comment at line 141.

**D. Score**

| Bar | Score | Note |
|---|---|---|
| No silent failures | 🟢 | Good logging; explicit "missing allowed_tools" warning |
| Measurable accuracy | ⛔ | No eval set; "did the agent answer the user's question correctly" is purely vibes |
| Self-healing / retries | 🟡 | `is_error` from `ResultMessage` propagates as SSE error event; user can retry |
| Smarter outputs | 🟡 | Has MCP, has WebSearch; missing real streaming + caching |

**E. Recommended changes**

- **Trivial:** Per-tool-call telemetry — the existing tool handler
  closures already record `tool_events`; add timing. Surface in logs.
- **Structural:** Real token streaming. The SDK supports streaming text
  chunks; the current `anyio.run → collect-all` pattern (forced because
  of the SDK stdin-close bug in MCP mode, per the comment at line 148)
  could be revisited now that the SDK has matured.
- **Structural:** Prompt caching on `config["system_prompt"]` per
  agent_type (it's static per persona). System prompts are the largest
  static chunk; a `cache_control: ephemeral` block here saves tokens
  on every turn.
- **Structural:** Maintain a real session per user — `ClaudeSDKClient`
  with `resume` rather than re-feeding the entire history as a flat
  prompt every turn. The current pattern is N² in conversation length.
- **Ambitious:** A small eval suite: ~30 hand-written user questions
  per persona with expected tool-call patterns. Run nightly. The
  signal is "did the agent call the right tool in the right order",
  which is much cheaper to grade than free-text QA.

---

## 5. Freelance scope

**Inventory finding:** there is no separate Claude SDK "freelance agent."
What "freelance" touches the LLM are:

1. The chat agent's `freelance` persona (covered in §4) —
   `FREELANCE_SYSTEM_PROMPT` at `agent_tools.py:150`, with 14 freelance-
   specific tools.
2. Three `freelance_*` categories in `mailProcessorService` (Agent-1
   classifier in §2.1 and the text-extractor in §2.2):
   - `freelance_payment` — payment confirmations from PayPal / Wise /
     Stripe / Razorpay → calls `mark_invoice_paid`.
   - `freelance_contract` — SOW / NDA / agreement emails → recorded in
     `extraction_summary` only (no DB tool wired up — tracked as data
     blob, not normalized).
   - `freelance_correspondence` — general client emails → also
     `extraction_summary` only.

**No LLM is invoked anywhere in `services/signatureService.py`,
`services/pdfService.py`, `services/invoiceService.py`,
`services/customerService.py`, `services/dashboardService.py`,
`services/InvoiceTemplateService.py`, `services/customFieldService.py`, or
`services/customerEmailService.py`.** Signature processing, invoice PDF
rendering, and customer management are all deterministic Python.

**Implication for the recommendations table:** freelance has no
dedicated agent to score. Its quality is bound by whatever §2.1 / §2.2 /
§4 inherit. The two `extraction_summary`-only categories (contract,
correspondence) effectively log structured data to `processedEmails` and
nowhere else; if the user wants those reflected in the freelance UI,
that's product work, not SDK work.

---

## 6. Job-applications email classifier (`jobEmailService.py:221`)

Strictly outside the bead's "transactions / investments / freelance" axes,
but listed here for SDK config-audit completeness, since it's a Claude
SDK call site that deserves the same scrutiny.

**B. SDK feature inventory**

`jobEmailService.py:221–242`
```python
ClaudeAgentOptions(
    max_turns=1,
    system_prompt="""You are a precise email classifier …""",
)
```
- **No `model` set** — relies on SDK default (likely sonnet, but not
  pinned).
- **No `permission_mode`** — defaults to `default` (which prompts for
  every tool use; harmless here since no tools are requested).
- **No `allowed_tools`, no MCP, no `output_format`** — pure text-out.
- Uses the **string-prompt** form of `query()` (line 246), not the
  AsyncIterable workaround used elsewhere — because there's no MCP, the
  stdin-close bug doesn't apply.
- On empty text response: `raise Exception("Claude returned empty response")`
  (line 259). Caller handles by retrying. Good.

**C. Failure modes audit**

- ⚠️ Pre-`f15ef18` shape — but mitigated by absence of any tool-implying
  language in the system prompt and `max_turns=1`. Won't reproduce the
  April outage in current config; would if anyone added a "use web search"
  line to the prompt.
- ⚠️ Model not pinned; behavior could shift across SDK versions.
- ⚠️ Same un-enforced JSON contract as Agent-1 (§2.1) — would benefit
  identically from `output_format=json_schema`.

**E. Recommended changes** (low priority, since this isn't on the
critical path):

- **Trivial:** Pin `model="haiku"` (this is a classifier — haiku is
  fine and cheap).
- **Trivial:** Pin `permission_mode="bypassPermissions"` to match the
  rest of the codebase (style consistency, even though it's a no-op
  without tools today).
- **Structural:** Same `output_format` migration as Agent-1 (§2.1).

---

## 7. Cross-cutting analysis

### 7.1 Model selection matrix

| Site | Model | Justification (current) | Recommendation |
|---|---|---|---|
| Agent-1 classifier (2.1) | **opus** | "Need accuracy on category boundaries" (inferred) | **haiku** + `output_format` — classification doesn't need opus |
| Text-email extractor (2.2) | haiku | Speed | Keep, but A/B against sonnet on a fixture set |
| PDF text-mode (2.3) | sonnet | Reasoning over text + schema | Keep |
| PDF image-mode (2.4) | sonnet | Vision + reasoning | Keep |
| User chat (4) | sonnet | "Right" for tool-using chat | Keep |
| Cron orchestrator (3.3) | sonnet | Reasoning over freshness rules | **haiku** — rules are simple; or move to deterministic Python |
| Rate fetchers (3.1) | sonnet | Web search + structured extract | Keep |
| Stocks IPO (3.2) | sonnet | Web search + extract | Keep (after R1) |
| Job-emails (6) | (default) | Inertia | Pin **haiku** |

Net: 1 opus → haiku, 1 sonnet → haiku, 1 (default) → haiku. About 70% of
the volume in this codebase is classification or one-shot extraction — opus
is overkill for it.

### 7.2 `permission_mode` × `allowed_tools` matrix

| Site | permission_mode | allowed_tools | MCP | April-outage shape? |
|---|---|---|---|---|
| AIHelper.py (3.2) | bypassPermissions | **none** | – | **YES** |
| AIRateTask.py (3.1) | bypassPermissions | WebSearch, WebFetch | – | no (fixed) |
| agentService.py (4) | bypassPermissions | WebSearch, WebFetch | yes | no |
| cronAgent.py (3.3) | bypassPermissions | – | yes | no (MCP-only is OK) |
| mailProcessorService.py 386 (2.1) | bypassPermissions | – | – | no (max_turns=1) |
| mailProcessorService.py 648 (2.2) | bypassPermissions | – | yes | no (MCP-only) |
| mailProcessorService.py 1432 (2.3) | bypassPermissions | – | – | no (output_format) |
| mailProcessorService.py 1807/2003/2035 (2.4) | bypassPermissions | – | yes | no (MCP-only) |
| jobEmailService.py (6) | (default) | – | – | no (no tool-implying prompt) |

**Conclusion:** Exactly one site reproduces the April outage shape today —
**AIHelper.py (3.2)**. Captured as **R1**.

The "MCP-only is OK" pattern is *contractually* OK per the SDK ("MCP tools
are always available regardless of `allowed_tools`" — comment at
`agentService.py:131`), but **fragile**: if that contract changes, four
call sites silently break. Defensive recommendation: list MCP tool names
in `allowed_tools` everywhere. Cheap and removes the contract dependency.

### 7.3 System prompts

- **DRY-ness:** Two prompts are reused well — `TEXT_EMAIL_SYSTEM_PROMPT`
  and `PDF_SYSTEM_PROMPT` live in `mailProcessorTools.py` and are
  imported. The chat-agent personas are also lifted to
  `agent_tools.py`. Good.
- **Cache-friendliness:** *None* of the system prompts are wrapped in
  `cache_control: ephemeral`. Every invocation pays the full prompt
  token cost. This is the single biggest cost lever in the codebase
  (see §7.6).
- **Structured-output enforcement:** Only `mailProcessorService.py:1432`
  uses `output_format`. All other "return JSON only" prompts rely on
  natural-language directives that the model may or may not follow,
  with regex extraction as the fallback. This is the **second** biggest
  reliability gap after R1.
- **Prompt size:** `CLASSIFICATION_SYSTEM_PROMPT` is ~70 lines; `PDF_SYSTEM_PROMPT`
  is ~60 lines; chat personas are 100+ lines each. All static. All
  uncached.

### 7.4 Hooks

**None used.** Zero `PreToolUse`, `PostToolUse`, `Stop`, `Notification`,
`UserPromptSubmit`, `PreCompact`, `SubagentStop`, or `SessionStart` hooks
are defined anywhere in the repo. This is the largest unused lever.

Specific places hooks would earn their keep:

- **`Stop` hook in 2.2 / 2.4** — assert that `report_result` was called
  for every input gmail_id; assert `insert_called["value"]` is `True`.
  Currently the only signal is post-hoc closure mutation.
- **`PreToolUse` hook in 2.4** — validate `insert_batch_transactions`
  payload (every transaction has a non-empty `reference_number`) before
  the DB write. Currently we just trust the model.
- **`Stop` hook in 3.1** — validate the JSON shape against the schema
  before the task accepts it. Same goal as `output_format` but works
  without changing the model contract.
- **`Notification` hook globally** — Claude CLI auth error → mail the
  user. The April 4-day silent outage was rooted in exactly this gap.

### 7.5 MCP servers

All MCP usage is **in-process** via `create_sdk_mcp_server`. No external
MCP servers (custom tools, resources, or prompts running outside the
Python process). Three distinct MCP servers exist:

- `agent_tools` (chat agent, ~30 tools) — `agentService.py`
- `cron-agent-tools` (3 tools) — `cronAgent.py`
- `mail_processor_tools` (~7 tools) — `mailProcessorService.py`

All three are rebuilt per-query (every chat turn, every cron cycle, every
PDF chunk). For chat and PDF this is necessary (tool closures bind
per-request state). For cron the rebuild is unnecessary churn — the tools
are stateless.

**Opportunity:** Real-time-rate MCP for investments. Today, rate fetchers
ask Claude to web-search and parse — every fetch is a new model call. A
custom MCP tool `get_market_rate(symbol, type)` that hits a known stable
API (e.g., a paid bullion-rate API, or NSE's official market-data feed)
would be **deterministic** and **free per call**, leaving Claude as a
fallback for non-standard cases.

### 7.6 Prompt caching

**Zero usage.** No `cache_control: ephemeral` anywhere. Highest-value
caching candidates, ranked by `(invocation_frequency × prompt_size)`:

| Rank | Site | Reason |
|---|---|---|
| 1 | `PDF_SYSTEM_PROMPT` (2.3, 2.4) | ~60-line prompt × every chunk × every PDF × every email batch — overnight scans cache-thrash this constantly |
| 2 | Chat persona prompts (4) | 100+ lines × every chat turn — interactive, so latency benefit is also user-visible |
| 3 | `CLASSIFICATION_SYSTEM_PROMPT` (2.1) | ~70 lines × every classification batch (50 emails) |
| 4 | `TEXT_EMAIL_SYSTEM_PROMPT` (2.2) | ~50 lines × every text-email batch |
| 5 | Bank format rules (2.3, 2.4) | Per-bank static blob × every chunk for that bank |
| 6 | Cron `SYSTEM_PROMPT` (3.3) | 30 lines × 48 invocations/day in window |

The 5-min ephemeral TTL works well for chunked PDFs (chunks process
back-to-back) and for chat (user keeps typing) and for batched
classification (50 emails per batch). For cron, the 30-min interval is
too long to benefit; explicit caching would miss every cycle.

### 7.7 Error capture pattern

Three patterns in the codebase, in descending quality:

1. **`(value, error_detail)` tuple** (`AIRateTask.py`) — the gold
   standard. Caller never has to look at logs for failure detail; it's
   in the return.
2. **`raise Exception` on empty** (`jobEmailService.py`) — works, but
   loses structure.
3. **`return None` / `return []` / `return 0`** (everywhere else) —
   logs the error, drops the work on the floor, the caller can't tell
   what went wrong without grepping `journalctl`.

**Recommendation:** Standardize on (1). Promote `AIRateTask`'s tuple
return to a small `DiagnosticResult` dataclass shared across:

```
utils/sdk_runner.py:
  DiagnosticResult = namedtuple("DiagnosticResult", "value detail latency_ms")
  def run_query_collect_text(options, prompt) -> DiagnosticResult: ...
```

This is the central change that the rest of the recommendations table
plugs into.

### 7.8 Health probes

**One implicit probe today**: `AIRateTask` fails-loudly when an empty
response comes back, and the failure detail (now post-`f15ef18`) lands in
`jobs.result`. If the user opens the jobs UI they'd see "Empty response
(no TextBlocks — likely missing allowed_tools or model refused)" — that
is enough to catch a Claude CLI auth outage *eventually*.

**No active probe.** The April 4-day outage happened because:
1. Claude CLI auth silently expired.
2. AIRateTask wasn't surfacing `allowed_tools`-shape failures.
3. Nothing was actively pinging the SDK health.

Post-(1) and (2) the system is now *eventually-consistent* about detecting
auth outages — the next failed cron job lands a useful message in the
jobs table. But there's still no proactive probe that fires before the
user notices.

**Recommendation:** A 15-minute cron that runs a one-shot Claude query
(`"reply with the literal word 'ok'"`, `max_turns=1`, no tools, 30s
timeout). On failure, write to a `health_probes` table and (if 2 in a
row) mail the user. Cost: < $0.01/day. See **R6**.

### 7.9 Observability

- **Logs:** Every site logs to `Logger(__name__).get_logger()`. Mostly
  free-text. A grep for "Claude error" finds 5 distinct phrasings;
  there's no canonical event name.
- **Run history:** Cron agent has a deque of 5 summaries — that's the
  only persisted "what did the agent do last cycle" record in the
  codebase. There's no equivalent for chat agent, mail processor, or
  rate fetchers.
- **Metrics:** None. No latency, no token count, no per-tool-call
  count, no model-version. Cost attribution is impossible.
- **Job table bloat:** STATUS.md notes ~200 rows / no cleanup. This is
  separate from the SDK story but worth folding in: the same table
  that *should* be the run history is also the dispatch queue, so
  cleanup needs to keep historical records distinct from pending work.

**Recommendation:** A single shared "agent_run" log line emitted at the
end of every SDK invocation: `agent={site} model={...} turns={N}
tools_called={N} latency_ms={N} status={ok|error} error_class={...}`.
Structured-log-grep into a daily summary. Cheap; transforms the system
from "look in logs when a user complains" to "see at a glance how the
SDK fleet is performing."

---

## 8. Prioritized recommendations table

Ordered by (impact / effort). **Trivial** = single small file change,
fits in one PR. **Structural** = touches multiple files / changes a
contract. **Ambitious** = new infrastructure / eval set / metrics.

| ID | Recommendation | Agent | Bucket | Impact | Effort | Depends on |
|---|---|---|---|---|---|---|
| **R1** | Add `allowed_tools=["WebSearch","WebFetch"]` to `AIHelper.py` and bump return to `(dict\|None, str)` (mirror `AIRateTask`) | 3.2 Stocks IPO | trivial | **HIGH** (silent IPO reconciliation broken today) | 30 min | – |
| **R2** | Helper to dedupe the three identical `ClaudeAgentOptions` blocks in §2.4 | 2.4 PDF image | trivial | medium | 30 min | – |
| **R3** | Pin `model="haiku"` and `permission_mode="bypassPermissions"` on `jobEmailService.py:221` | 6 jobs | trivial | low | 15 min | – |
| **R4** | List MCP tool names in `allowed_tools` everywhere MCP is used | 2.2, 2.4, 3.3, 4 | trivial | medium (defensive) | 1–2 h | – |
| **R5** | Track per-batch `tool_calls_count` in mail processor; emit one structured `agent_run` log line per SDK invocation | all | trivial | high (observability) | 2–3 h | – |
| **R6** | Add a 15-minute Claude-CLI health probe cron with mail-on-fail | global | structural | high (replays April outage prevention) | half day | R5 (uses same log shape) |
| **R7** | Migrate `mailProcessorService.py:386` (Agent-1) to `output_format=json_schema` and downgrade to haiku | 2.1 | structural | high (cost + reliability) | half day | – |
| **R8** | Migrate `jobEmailService.py:221` to `output_format=json_schema` | 6 | structural | medium | 2 h | R3 |
| **R9** | Migrate rate fetcher prompts to `output_format=json_schema`; eliminate the per-task ad-hoc shape strings | 3.1 | structural | medium (drift safety) | 1 day | – |
| **R10** | Delete `AIHelper.py` after R1 lands; route `StocksService.py:337` through a shared web-search helper | 3.2 | structural | medium (DRY) | 2–3 h | R1 |
| **R11** | `Stop` hook on 2.2 to assert `report_result` was emitted for every input gmail_id | 2.2 | structural | high (silent partial-batch failures) | half day | R4, R5 |
| **R12** | Replicate the text-mode coverage check (`_count_expected_transactions`) into image-mode 2.4 | 2.4 | structural | medium | half day | – |
| **R13** | Promote `(value, error_detail, latency_ms)` tuple to a shared `DiagnosticResult`; route every SDK call site through it | global | structural | high (uniform error capture) | 1 day | R5 |
| **R14** | Prompt caching (`cache_control: ephemeral`) on `PDF_SYSTEM_PROMPT`, chat persona prompts, classification prompt | 2.1, 2.3, 2.4, 4 | structural | high (token cost; latency for chat) | 1 day | – |
| **R15** | Daily cron-agent → deterministic Python rule check; reserve LLM call for ambiguous cases only | 3.3 | structural | medium (cost) | 1 day | – |
| **R16** | Real session per chat user (`ClaudeSDKClient` + `resume`), drop the flat-history string-prompt pattern | 4 | ambitious | high (UX, cost, fidelity) | 2–3 days | – |
| **R17** | Real-time-rate MCP tool (`get_market_rate(symbol, type)`) backed by a stable API; LLM is fallback only | 3.1 | ambitious | high (deterministic data) | 2–3 days | requires API selection |
| **R18** | Labelled fixtures + nightly precision/recall eval for Agent-1 classification | 2.1 | ambitious | high (the only "measurable accuracy" lever in the repo) | 2–3 days | R7 |
| **R19** | Per-persona chat eval: ~30 user questions × expected tool-call sequence, run nightly | 4 | ambitious | high | 3–5 days | R16 |
| **R20** | Citations on rate fetchers — record the source URL the model claims it used | 3.1 | ambitious | medium (auditability) | 1 day | R9 |

**Recommended ship order if you only do five things:**
R1 → R5 → R6 → R7 → R14. That's the on-fire fix, the observability bedrock,
the health probe, the highest-impact reliability migration, and the
biggest cost lever — all in roughly a week of focused work.

---

*End of study. Output: `docs/agent-deep-study-2026-05.md` in this repo.
Hand-off to Commit Agent follows.*
