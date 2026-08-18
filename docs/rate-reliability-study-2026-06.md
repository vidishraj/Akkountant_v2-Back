# Akkountant Backend — Rate-Reliability Study (R17′)

**Date:** 2026-06-06
**Branch studied:** `Personal` (worktree: `agent/akkountant_backend`, HEAD at `2bff437`)
**Author:** akkountant_backend agent (research-only; no code changes in this bead)
**Tracking:** `hq-co2b` (Wave 2 R17′ — supersedes the original R17 from `docs/agent-deep-study-2026-05.md`)

---

## What this study is

The original deep-study (`hq-aw4i`, May 2026) recommended **R17** — build
an MCP rate tool — as a defense-in-depth layer for the rate fetchers. The
Overseer correctly noted that most rate paths already use APIs or web-
parsing, and the right question is **why those aren't reliable enough**,
not "let's build a new layer."

R17′ inventories every rate-fetch path, characterizes its current
implementation and failure modes, scores it against the same four bars as
the deep-study (no-silent-failures, measurable-accuracy, self-healing,
smarter-outputs), and produces a prioritized recommendation table.

**Out of scope:** any code change. Even on-fire findings get flagged via
mail, not fixed in this bead.

---

## Reading order

Sections 1–3 are the non-LLM paths and were slice A (fd9b89e, 2026-06).
Sections 4–8 are the LLM paths — **slice B, 2026-08-17**, filled in
now that Family B v2 (`880a442`) has been in production for ~2 months
and the post-deploy failure surface has been observed + iterated on
(v2 → v3 stderr wiring → v4 property-key fix → v5 allowed_tools lock →
v6 EPF URL pivot). Sections 5–8 are no longer DRAFT; §9.5 is retitled
to reflect that the observation window has passed. §10 marks recommendations
that shipped in the intervening months (RR1 partial, RR2 via ak-539,
RR11 partial via ak-539 v2 hard-floor, ak-iwj / ak-2r8 additional
protections not enumerated in the original table). New RR-20+ rows
emerged from the post-v2 failure inventory.

---

## Table of Contents

1. [Rate-path inventory](#1-rate-path-inventory)
2. [Path 1: Mutual fund NAVs (`SetMfRate.py`)](#2-path-1-mutual-fund-navs-setmfratepy)
3. [Path 2: NPS NAVs (`SetNPSRate.py`)](#3-path-2-nps-navs-setnpsratepy)
4. [Path 3: Stock instrument metadata (`SetKiteStockDetails.py`)](#4-path-3-stock-instrument-metadata-setkitestockdetailspy)
5. [Path 4: IBJA gold rate (`SetIBJAGoldRate.py`)](#5-path-4-ibja-gold-rate-setibjagoldratepy)
6. [Path 5: EPF interest rate (`SetEPFRate.py`)](#6-path-5-epf-interest-rate-setepfratepy)
7. [Path 6: PPF interest rate (`SetPpfRate.py`)](#7-path-6-ppf-interest-rate-setppfratepy)
8. [Path 7: IPO/orphan-sell prices (`utils/AIHelper.py` ← `StocksService.py:337`)](#8-path-7-ipoorphan-sell-prices-utilsaihelperpy--stocksservicepy337)
9. [Cross-cutting analysis](#9-cross-cutting-analysis)
10. [Prioritized recommendations table](#10-prioritized-recommendations-table)

---

## 1. Rate-path inventory

| # | Task class | File | Fetch mechanism | Concurrency | Retries | Schema validation | Cache/fallback |
|---|---|---|---|---|---|---|---|
| 1 | `SetMFRate` | `services/tasks/SetMfRate.py` | `aiohttp` GET → `api.mfapi.in/mf/<schemeCode>` | 25 concurrent + semaphore | 3 in-flight × 3 outer retry passes | none | none |
| 2 | `SetNPSRate` | `services/tasks/SetNPSRate.py` | `requests.get` (verify=False) → `npstrust.org.in/scheme-wise-nav-report-excel` × 14 PFMs × 16 schemes (~224 requests) | serial (single thread, ~224 calls/cycle) | none | header column check (`'DATE OF NAV', 'NAV VALUE', 'SCHEME ID', 'SCHEME NAME'`) | none |
| 3 | `SetKiteStockDetails` | `services/tasks/SetKiteStockDetails.py` | `KiteService.get_all_instruments(user_id)` → KiteConnect API | 1 (single batched call) | none (relies on KiteService internals) | dict-key lookup (`exchange`, `instrument_type`) | none |
| 4 | `SetIBJAGoldRate` | `services/tasks/SetIBJAGoldRate.py` | (v2) `requests.get` → `ibjarates.com` + sonnet `output_format` extraction via `utils/web_extract.py` | 1 | 0 outer + `max_turns=3` inner | json_schema (sonnet output_format) | none |
| 5 | `SetEPFRate` | `services/tasks/SetEPFRate.py` | (v2) `requests.get` → Wikipedia EPFO + sonnet extraction | 1 | 0 + max_turns=3 | json_schema | none |
| 6 | `SetPpfRate` | `services/tasks/SetPpfRate.py` | (v2) `requests.get` → Wikipedia PPF + sonnet extraction | 1 | 0 + max_turns=3 | json_schema | none |
| 7 | IPO lookup (`AIHelper.py`) | `utils/AIHelper.py` ← `services/StocksService.py:337` | (v2) `requests.get` → chittorgarh.com mainboard IPO list + sonnet extraction | 1 (one call per orphan-sell reconciliation cycle, batched ISINs in prompt) | 0 + max_turns=3 | json_schema | none |

Aggregate observations:

- **No path has a cache layer.** Every cycle re-fetches from upstream. For
  MFs that's 5,000+ requests; for NPS, 224; for the others, 1 each.
- **No path has a secondary fallback.** Single-source by design.
- **No path has post-fetch schema validation** for paths 1–3 beyond
  best-effort key access. Paths 4–7 (post-Family-B-v2) DO have it via
  sonnet `output_format`.
- **Concurrency profile is bimodal:** path 1 is highly parallel (25-way),
  paths 2-7 are essentially serial. The serial paths are not bottlenecked
  by concurrency; their latency is upstream-dominated.

---

## 2. Path 1: Mutual fund NAVs (`SetMfRate.py`)

### A. Current implementation

- **Interval:** 150 min (2.5h)
- **Source:** `https://api.mfapi.in/mf/<schemeCode>`, one call per scheme.
- **Fetch loop:** `buildJsonForMF` reads the scheme list from
  `LatestFile(listType, MfListPrefix)`, builds N URLs, runs three sequential
  passes via `asyncio.run(make_requests(urls))`. Failed scheme IDs (those
  not in `result_map`) are retried in pass 2 and pass 3 with decreasing
  concurrency (`max(10, CONCURRENT_REQUESTS // (pass+2))`).
- **Per-request retry:** `fetch_scheme` does up to 3 attempts with linear
  backoff (`RETRY_DELAY * (attempt+1)`) on:
  - HTTP 429 / 502 / 503 (continue → retry)
  - `ClientConnectorError`, `asyncio.TimeoutError` (sleep + continue)
- **Schema validation:** None. `_process_responses` does try/except
  KeyError/IndexError/TypeError and skips the scheme. Successful
  schemes get `{date, nav, scheme_id}` plus optional meta.
- **Concurrency:** semaphore-limited 25, `TCPConnector(limit_per_host=25,
  force_close=True)`, 30s total timeout / 10s connect timeout.

### B. Failure inventory

Telemetry signal available today:
- `Pass N complete: M/total schemes in Ts` log line (line 69, 82)
- `Skipping scheme X: <KeyError>` debug-level — only visible at debug
- `final_failed = N schemes still failed` warning

**No structured `rate_run` log line analogous to the SDK's `agent_run`.**
There is no grep-friendly per-path success/fail/latency emission. The
infra forensics that diagnosed Family B v2 had `agent_run` to work with;
for path 1 there is nothing equivalent.

What we know happens (from `STATUS.md` and Lead recall):
- Total MF universe is ~5,000 schemes
- "Pass 1 complete" typically reports ~95-98% coverage
- After all retry passes, `final_failed` is usually <50 (i.e., ~99% coverage)
- The `final_failed` warning is the only signal; nothing alerts on it

Failure modes likely but unmeasured:
- `mfapi.in` rate-limit spikes (the 429/502/503 retry path)
- `force_close=True` means no connection reuse — every request opens a
  fresh TCP socket. Under bursty load this may itself trigger rate
  limits on the upstream's side
- Transient DNS failures during a 5k-request burst
- The `safe_replace_file` call at line 48 could fail silently and not
  invalidate the rate cache; depends on `BaseTask` implementation

### C. SDK-feature-menu fit

Path 1 doesn't use the SDK. **The SDK is not the right tool here** — this
is a deterministic API call with a stable JSON schema and bulk traffic.
The lessons that apply:

- **Structured observability**: `rate_run path=mf scheme_count=N
  success=M latency_ms=T status=ok` would mirror `agent_run` and unlock
  the same forensics shape
- **Schema validation**: a single `pydantic` model for the scheme
  response would replace the try/except wildcards in `_process_responses`
  and surface drift in the upstream contract immediately
- **Stale-on-failure cache**: serve the previous `MFRate.json` if the
  current run yields <90% coverage instead of partial writes

### D. Score

| Bar | Score | Note |
|---|---|---|
| No silent failures | 🟡 | `final_failed` warning exists but nothing watches it; partial writes happen with no coverage gate |
| Measurable accuracy | 🟡 | "Pass N complete" log gives coverage; not structured, not alerted |
| Self-healing / retries | 🟢 | 3 outer passes × 3 inner retries — most defensive path in the codebase |
| Smarter outputs | ⛔ | No schema validation, no cache, no fallback API |

### E. Recommended changes (path-1-specific)

- **Trivial:** Emit a `rate_run path=mf success_pct=X failed=N latency_ms=T` structured log line at end of `buildJsonForMF`. Mirror `utils/sdk_runner.emit_agent_run`'s format so the same infra grep works.
- **Trivial:** Gate `safe_replace_file` on coverage threshold (e.g., refuse to overwrite if `len(result_map) / len(urls) < 0.90`). Better to serve stale data than partial.
- **Structural:** Add a Pydantic model for the mfapi.in response. Drift in the upstream's JSON shape would otherwise quietly drop matching schemes (the try/except in `_process_responses` swallows it).
- **Structural:** Set `force_close=False` and let aiohttp reuse keepalive connections. The current `force_close=True` is likely the cause of any rate-limit observations — it's the most aggressive socket policy possible.

---

## 3. Path 2: NPS NAVs (`SetNPSRate.py`)

### A. Current implementation

- **Interval:** 60 min
- **Source:** `https://npstrust.org.in/scheme-wise-nav-report-excel?navcatdataxls=PFM0XX&navyearselxls=12&navsubdataxls=SM0XXNNN`
- **Loop shape:** 14 PFMs × 16 schemes = up to **224 GET requests per cycle**, all serial. Each call may return either a TSV file (the upstream serves TSV with an Excel content-type header), an HTML error page, or empty content.
- **Detection of "is this real data":** `content-type contains 'application/vnd'` OR `len(response.content) > 1000`. Both are weak signals; an HTML error page can exceed 1000 bytes.
- **Parsing:** `pd.read_csv(BytesIO, sep='\t')`, then column-presence check for `'DATE OF NAV', 'NAV VALUE', 'SCHEME ID', 'SCHEME NAME'`. If columns missing, log `Missing required columns` and skip.
- **TLS:** `verify=False` — npstrust.org.in has had cert issues historically (per the only contemporaneous reasoning that survives in code). This is acceptable for read-only public NAV data but should be documented.
- **No retry. No concurrency. No timeout-aware backoff.** A single `requests.get` with `timeout=30`.

### B. Failure inventory

Telemetry signal:
- `Successfully processed rates for SCHEME_ID` info-level per scheme
- `Error processing rates for SCHEME_ID: <ex>` error-level
- `No data available for PFM/SCHEME: <ex>` debug-level (likely 404 / TLS / connection)
- `Missing required columns in TSV data for SCHEME_ID` error-level
- `Total scheme rates collected: N` info-level summary

Failure modes observed:
- The "either TSV or HTML" detection is sometimes wrong. When npstrust
  serves an HTML error page > 1000 bytes, `pd.read_csv(BytesIO, sep='\t')`
  raises and we log `Error processing rates`. The path proceeds.
- npstrust.org.in is **frequently down or slow** during business hours.
  With no concurrency, the worst-case cycle latency is 224 × 30s = ~112
  minutes of timeouts — exceeding the 60-minute interval.
- TLS cert renewal events on the upstream are silent failures (we
  `verify=False` so we never observe them).
- The hardcoded ranges (`pfm_num in range(1, 15)`, `scheme_num in
  range(1, 17)`) bake the assumption that PFM IDs are PFM001–PFM014 and
  schemes are SM0XX001–SM0XX016. New PFMs (PFM015+) or new schemes
  beyond 016 are silently invisible.

### C. SDK-feature-menu fit

Not applicable — pure HTTP + pandas. Lessons:

- **Structured observability** would catch the >60-minute cycles
- **Schema validation** would distinguish "TSV but wrong columns" from
  "HTML error page misdetected as TSV"
- **Fallback** is hard here — NPS NAV data is genuinely single-source.
  The closest fallback would be to **cache and serve-stale** with an
  age-bound: "if last successful fetch was <48h ago, serve that on
  current-fetch failure; if older, mark the path as DOWN."

### D. Score

| Bar | Score | Note |
|---|---|---|
| No silent failures | ⛔ | `verify=False` masks TLS failures; HTML-as-TSV misparse logs error but path continues; new PFMs/schemes invisible |
| Measurable accuracy | ⛔ | `total_schemes` summary is the only signal; no expected vs actual |
| Self-healing / retries | ⛔ | Zero retries. A flaky upstream cycle = full path failure |
| Smarter outputs | ⛔ | No schema model; no cache; no staleness tracking |

### E. Recommended changes (path-2-specific)

- **Trivial:** Add `rate_run path=nps` structured log line. Include
  expected (14×16=224) vs actual successful schemes per cycle.
- **Trivial:** Replace the `len(response.content) > 1000` heuristic with
  an actual sniff of the first bytes — a TSV starts with column headers
  that include `DATE OF NAV`; HTML starts with `<!DOCTYPE` or `<html`.
- **Trivial:** Add a single per-request retry on connection / timeout
  errors. Cost is bounded at 2× worst-case cycle time but eliminates
  most transient failures.
- **Structural:** Discover PFM/scheme IDs dynamically by walking a
  master list from npstrust.org.in (if one exists), or by maintaining
  the hardcoded ranges as a config field that's reviewed quarterly. The
  silent-coverage-loss-on-new-PFMs is the biggest single failure mode.
- **Structural:** Cache successful per-scheme TSVs on disk with a
  timestamp. On current-cycle failure for a scheme, fall back to the
  cached value if <24h old.
- **Structural:** Run the 224 calls with a small concurrency pool
  (e.g., 4 concurrent, semaphore-gated) so worst-case cycle latency
  drops from 112min to ~28min. Stay well below path 1's 25-way to
  avoid stressing a fragile upstream.

---

## 4. Path 3: Stock instrument metadata (`SetKiteStockDetails.py`)

### A. Current implementation

- **Interval:** 320 min (~5.3h)
- **Source:** Kite Connect API via `KiteService.get_all_instruments(user_id)`. This is **not a rate-fetch** in the same sense as 1/2 — it's a metadata refresh (tradingsymbol → instrument_token mapping) used downstream by the actual price-fetching paths. But it's in scope per the bead.
- **Fetch:** `kite_service.get_all_instruments(user_id)` returns the
  full BSE+NSE instrument dump. The task filters to `exchange in
  ['NSE', 'BSE'] and instrument_type == 'EQ'`.
- **User binding:** uses `get_first_kite_user()` to find any user with
  a saved Kite access token. This is a long-standing assumption: any
  user's token can fetch the global instrument list, because Kite's
  instrument endpoint is per-account-authenticated but returns the
  same data for all accounts. **Per the user memory: "Akkountant is
  single-user (Overseer-only)"** — so "first user" is always Overseer.
- **Failure handling:** entire path wrapped in try/except → returns
  string error + "Failed". `get_first_kite_user` has its own
  try/except returning None.

### B. Failure inventory

Telemetry signal:
- `Successfully processed N equity instruments from Kite` info-level
- `Error getting Kite user: <ex>` error-level
- `No user found with Kite access token` returned to the task runner
- Raw exception from KiteService is returned as the task failure string

Failure modes:
- **Kite token expiry.** Kite access tokens are 1-day-validity. If
  Overseer doesn't refresh, `get_all_instruments` returns 403 →
  task fails → `Failed` status → next cycle 320min later → same
  failure → instrument list goes stale. Stocks bought during the
  gap can't be resolved (their `tradingsymbol` isn't in `StockDetails.json`).
- **Kite API rate limits** — Kite has stricter limits than mfapi.in;
  hitting them would surface as a 429 the path doesn't handle.
- **DB connection in `get_first_kite_user`** opens a fresh engine /
  session every call. Connection pool exhaustion under sustained
  cycling is theoretically possible but with a 320-min interval is
  unlikely.

### C. SDK-feature-menu fit

Not applicable — this is API + DB only. Lessons:

- **Structured observability** would catch `Failed` status earlier
- **Token-expiry probe** could be a separate cron that re-authenticates
  Kite proactively rather than discovering expiry inline

### D. Score

| Bar | Score | Note |
|---|---|---|
| No silent failures | 🟡 | Path returns `Failed` cleanly; but downstream consumers (price fetchers) silently degrade when instrument list is stale |
| Measurable accuracy | 🟢 | `len(equity_instruments)` is a useful sanity-check — should be in the tens of thousands |
| Self-healing / retries | ⛔ | Zero retry; token expiry is a "wait 5h then try again" situation |
| Smarter outputs | 🟡 | Filter logic is clean; no schema validation but Kite's contract is stable |

### E. Recommended changes (path-3-specific)

- **Trivial:** Emit `rate_run path=kite_instruments count=N status=ok|fail` structured log line. Alert if count drops by >5% week-over-week (would catch silent contract drift on Kite's side).
- **Trivial:** On `Failed`, mail Overseer once per occurrence rather than waiting for them to notice missing instrument data downstream.
- **Structural:** Build a separate `KiteTokenHealthProbe` task that runs every 15 minutes and refreshes / mails Overseer when the token is within 2 hours of expiry. Removes the "wait for next 320-min cycle to discover expiry" anti-pattern.

---

## 5. Path 4: IBJA gold rate (`SetIBJAGoldRate.py`)

Grounded against Family B v2 + all post-deploy fixes (v3 stderr wiring →
v4 property-key fix → v5 allowed_tools lock) as of 2026-08-17.

### A. Current implementation (post v5)

- **Interval:** 300 minutes (5h; `SetIBJAGoldRate.__init__` L103).
- **Source:** `https://ibjarates.com/` (Overseer-acked, unchanged from v2).
- **Fetch:** `requests.get` via `utils/web_extract.fetch_and_extract`
  (30s timeout, desktop UA, 200k-char body cap).
- **Extract:** sonnet `output_format` json_schema with `GOLD_SCHEMA`
  (schema uses snake_case property keys `carat_24 / carat_22 / carat_18`
  per v4 fix `b2abe3d` — display keys `"24 Carat"` etc. violated the
  Anthropic property-key regex `^[a-zA-Z0-9_.-]{1,64}$` and 400'd the
  request).
- **Post-extract remap:** LLM-schema keys → on-disk display keys
  (`SetIBJAGoldRate.py:132-134`). On-disk `GOLDRATE.json` contract
  unchanged from v1 — frontend keeps reading `"24 Carat"` etc.
- **Runtime:** `run_query_collect` (`utils/sdk_runner.py:92`) drives the
  SDK inside `anyio.run` (blocking call from scheduler).
- **`allowed_tools=[]`** (v5 `577ae6c` lock — sonnet would otherwise
  wander into Grep/Read on large HTML payloads; no tool is ever the
  right answer here since the HTML is already in the user message).
- **Coverage gates** (post ak-2r8 `7d35075`): universe-of-1 semantics
  via `BaseRateTask` — successful extraction → 1/1 (100% ≥ 0.98
  threshold) → Completed; fetch failure → 0/1 (0% < 0.5 hard-floor) →
  Failed, last-good `GOLDRATE.json` preserved on disk. No clobber.

### B. Failure inventory (post-verify — actually observed)

`agent_run` lines emit under `journalctl -u akkountant | grep 'agent_run agent=rate.gold '`:

| Failure class | Root cause | Fix landed |
|---|---|---|
| **turns=0 tools_called=0 status=ok** (pre-v2 empty-TextBlocks) | Model declining under `bypassPermissions` + tool-implying prompt | Family B v2 `880a442` — pivoted to `requests` fetch + `output_format` extract |
| **API 400 on invalid property keys** (v4 pre-fix) | GOLD_SCHEMA had `"24 Carat"` keys with spaces; violated `^[a-zA-Z0-9_.-]{1,64}$` regex | v4 `b2abe3d` — schema keys `carat_24 / carat_22 / carat_18`, post-extract remap to display form |
| **tools_called>0, structured_output=None** (v5 pre-fix) | Model wandered into Grep/Read/etc. on larger HTML payloads instead of emitting structured output | v5 `577ae6c` — `allowed_tools=[]` in ClaudeAgentOptions |
| **stderr-blind fast-fail** (v3 pre-fix) | SDK subprocess exited before message loop; anthropic request_id / resolved model_id lost | v3 `2fd2b3f + dfe0c5b` — `_make_sdk_stderr_logger` callback wired at ClaudeAgentOptions construction time, `--debug-to-stderr` flag added |
| **Model-alias flip** | Anthropic silently rerouted `sonnet` alias mid-run | Diagnosed via haiku probe (`2fd2b3f`); resolved-model-id now surfaces via stderr, no code fix needed post-diagnostic |
| **HTTP fetch failure** (pre-SDK) | ibjarates.com 5xx / connection reset | Not retried (single-shot `requests.get`); ratio-based coverage floor now preserves last-good `GOLDRATE.json` |
| **200k truncation** | Response body exceeds 200k → truncated with warning + note to sonnet | Rare for ibjarates.com (page is ~50KB HTML); not observed as a real failure class |
| **ibjarates.com page restructure** | HTML layout change → sonnet extraction produces partial/empty `structured_output` | Would surface as `structured_output != None` but with missing purities; caught by presence-check at `SetIBJAGoldRate.py:123` (`if 'carat_24' not in jsonData`) |

Sample verified `agent_run` shape on healthy run:
```
agent_run agent=rate.gold model=sonnet turns=1 tools_called=0 latency_ms=~4500 status=ok
```

### C. SDK-feature-menu fit (post-v5)

Now lit (post-Family-B-v2 + all iterations):
- ✅ `output_format` json_schema with valid property keys (v4)
- ✅ Structured `agent_run` log line (R5 via `run_query_collect`)
- ✅ `sdk_stderr` capture (v3) — request_id + model_id observable
- ✅ Single-source-of-failure isolation (HTTP vs SDK vs extraction distinguishable via `agent_run` `status` + return-tuple detail)
- ✅ Coverage gates via `BaseRateTask` (ak-2r8) — universe-of-1 preserves last-good on failure
- ✅ Tool-wandering blocked (v5 allowed_tools=[])

Still dark:
- ❌ Prompt caching on `GOLD_SYSTEM_PROMPT` (static system prompt; would pay back inside 2 invocations at $0.003/token savings on cache-hit)
- ❌ Citations / source URL provenance enforcement (schema doesn't require a `source_url` field so sonnet could invent numbers — currently trusted because `ibjarates.com` is the authoritative source)
- ❌ Schema-validation-partial-coverage observability (all-or-nothing: `structured_output is None` or dict-with-required-keys; no per-field-null count)
- ❌ Secondary source fallback (e.g., MMTC-PAMP `mmtcpamp.com/gold-prices` or Augmont `augmont.com/gold-silver-rate`) — RR15 not yet landed
- ❌ `sdk_stderr` grep-alerting (lines are captured but no alert if `error` / `4xx` / `5xx` appears; ops has to eyeball)

### D. Score (post-verify)

| Bar | Score | Note |
|---|---|---|
| No silent failures | 🟢 | v2 tuple-return + v3 stderr + v5 tool-lock + ak-2r8 coverage-gate cover every known failure class |
| Measurable accuracy | 🟡 | Binary structured-output-present check + presence-check on `carat_24`; no cross-source oracle to catch subtle value drift |
| Self-healing / retries | 🟡 | `max_turns=3` inner + ak-2r8 preserve-last-good on failure; no outer HTTP retry — a single connection reset still costs the whole cycle |
| Smarter outputs | 🟢 | Best-in-class for rate-fetcher class; schema-forced structured output, cited source URL in output |

### E. Recommended changes (path-4-specific, post-verify)

- **Trivial:** Add prompt caching (`cache_control: ephemeral`) on `GOLD_SYSTEM_PROMPT` — RR9 unchanged.
- **Trivial:** Add one HTTP retry on `requests.get` failure inside `fetch_and_extract` (single retry, 2s backoff). Prevents transient connection failure from costing the whole 5h cycle — RR10 unchanged.
- **Structural:** Secondary URL probe (mmtcpamp.com or augmont.com) — RR15 unchanged. Highest per-effort payoff of the not-yet-shipped list.
- **Structural (NEW post-verify, RR20 below):** Schema-validation partial-coverage counter — count how many of `carat_24 / 22 / 18` came back non-null and emit a `rate_partial_coverage path=gold present=N/3` line. Catches "sonnet returned 2/3 purities" silent-degradation the current presence-check misses.

---

## 6. Path 5: EPF interest rate (`SetEPFRate.py`)

Grounded post-Family-B-v2 + all iterations (v4/v5 shared with path 4;
v6 source pivot in `395a404`).

### A. Current implementation (post v6)

- **Interval:** 10080 minutes (7 days; `SetEPFRate.__init__` L98).
- **Source:** `https://cleartax.in/s/epf-interest-rate` (v6 `395a404` —
  pivoted OFF Wikipedia EPFO page which went dry: infra source-audit
  confirmed only 5 tables remain, none of them the rate history).
  Source-audit ranking at v6 pivot:
  - `cleartax.in`: 19 FY rows (2009-2025 cleanly), 68 rate matches ← PRIMARY
  - `groww.in`: 6 FY rows, 39 rate matches (fallback candidate)
  - `epfindia.gov.in`: canonical but WAF-blocks bots (unreachable)
  - `bankbazaar.com`: 404 (dead)
- **Fetch + extract:** same `fetch_and_extract` pipeline as path 4.
- **Schema:** `{"data": [{"Year": "YYYY-MM", "interest_rate": float}]}` —
  snake_case per v4 fix; remapped to display key `"Interest Rate"`
  post-extract at `SetEPFRate.py:129-130`.
- **System prompt encodes April–March FY expansion** — the LLM sees "FY
  2024-25: 8.25%" and emits 12 monthly rows (Apr 2024 → Mar 2025).
- **Coverage gates** (post ak-2r8): same universe-of-1 semantics as path 4.

### B. Failure inventory (post-verify)

| Failure class | Root cause | Fix landed |
|---|---|---|
| **Source dry** (v5 → v6) | Wikipedia EPFO page editorial restructure — rate-history table removed; sonnet correctly emitted `{"data": []}` against empty source | v6 `395a404` — source pivot to cleartax.in |
| **Empty response after v5 emitted `{"data": []}` valid JSON** | Would have clobbered last-good EPFRate.json under pre-ak-2r8 semantics (JSON parses fine → safe_replace_file writes empty file → silent green) | Fixed structurally by ak-2r8 `7d35075` — BaseRateTask coverage-gate catches 0-item results as < 5% threshold on universe-of-1 → preserve last-good |
| **API 400 on invalid property keys** (v4 pre-fix) | EPF_SCHEMA had `"Interest Rate"` with space | v4 `b2abe3d` shared with path 4 |
| **Tool wandering** (v5 pre-fix) | rate.epf specifically observed post-v4 emitting `tools_called>0, structured_output=None` on larger cleartax page | v5 `577ae6c` allowed_tools=[] |
| **Cleartax page bloat** | Blog page includes prose + related-article links + ads; the rate-table is embedded and needs to be found among 4-5 other tables (deposit limits, scheme comparisons, withdrawal slabs) | System prompt updated at v6 to explicitly say "Locate the table whose rows are financial years and rates (rather than e.g. a comparison-of-schemes table)" — mitigates but doesn't fully prevent mis-extraction |
| **200k truncation** | Cleartax article is ~130KB HTML — under 200k so truncation doesn't fire in practice; would fire if the article got substantially longer |
| **Cleartax URL restructure risk** | cleartax.in is a commercial tax-blog whose URL structure can change without notice (unlike Wikipedia which has stable canonical URLs) | Not mitigated — would surface as `HTTP fetch failed for cleartax.in: 404` in the return tuple, cycle Failed, last-good preserved via ak-2r8 |

Sample `agent_run`:
```
agent_run agent=rate.epf model=sonnet turns=1 tools_called=0 latency_ms=~6000 status=ok
```
(Higher latency than gold — cleartax page is larger than ibjarates.)

### C. SDK-feature-menu fit

Same as path 4. Additionally:
- ❌ **URL-drift alerting** — cleartax.in URL structure change would silently start failing cycles; no probe to catch the URL rot early.

### D. Score

| Bar | Score | Note |
|---|---|---|
| No silent failures | 🟢 | Same coverage as path 4 + ak-2r8 empty-response protection |
| Measurable accuracy | 🟡 | Presence check `if 'data' not in jsonData or len(...) == 0` catches empty; no row-count sanity vs expected (should have ~144 rows for 12 FYs × 12 months) |
| Self-healing / retries | 🟡 | Same as path 4 |
| Smarter outputs | 🟢 | Fresh URL selected by infra source-audit; system prompt table-disambiguation guidance |

### E. Recommended changes (path-5-specific)

- **Trivial:** RR9 (prompt caching) shared with path 4.
- **Trivial:** RR10 (HTTP retry) shared with path 4.
- **Trivial (NEW, RR21 below):** Post-extract row-count sanity — expect
  `len(data) ≈ (current_year - 2014 + 1) × 12` = ~132-144 rows. Emit
  a `WARN` if the extracted count deviates > 20% from expected. Catches
  cleartax layout regressions before they land on disk.
- **Structural:** RR15 secondary URL probe — configure `groww.in`
  fallback. If cleartax fetch or extract fails, try groww before
  returning failure. Groww has 6 FY rows vs cleartax's 19 so it's a
  degraded backup, not equal, but it covers the recent years which
  matter most for portfolio valuation.
- **Structural (NEW, RR22 below):** Weekly URL-alive probe — cheap
  HEAD request to cleartax.in that alerts if the URL 404s or 5xx's,
  catching URL rot before it hits the 7-day cycle.

---

## 7. Path 6: PPF interest rate (`SetPpfRate.py`)

Grounded post-Family-B-v2 + v4/v5 shared with paths 4-5. PPF has been
the most stable of the four LLM rate paths — no path-specific hotfixes
beyond the shared v4 property-key + v5 allowed_tools work.

### A. Current implementation (post v5)

- **Interval:** 6000 minutes (4 days; `SetPPFRate.__init__` L70).
- **Source:** `https://en.wikipedia.org/wiki/Public_Provident_Fund_(India)`
  (Overseer-acked; unchanged from v1). Wikipedia's PPF page carries
  a stable rate-history table anchored on the "Interest rate history"
  section, updated within days of any rate change.
- **Fetch + extract:** same `fetch_and_extract` pipeline as paths 4-5.
- **Schema:** `{"periods": [{"from": "YYYY-MM", "rate": float}]}` —
  period-based, then expanded via `_expand_periods_to_monthly`
  (`SetPpfRate.py:76-107`, preserved unchanged from v1). Sonnet emits
  ~20-40 period entries (annual pre-2016, quarterly from 2016);
  post-extract expansion produces monthly rows.
- **Coverage gates** (post ak-2r8): same universe-of-1 semantics.

### B. Failure inventory (post-verify)

| Failure class | Root cause | Fix landed |
|---|---|---|
| **Shared v4/v5 API 400 + tool wandering** | Property-key regex + allowed_tools missing | Shared v4 `b2abe3d` + v5 `577ae6c` |
| **Period expansion on out-of-order periods** | `_expand_periods_to_monthly` iterates in list order; a historical correction inserted at the bottom of Wikipedia's table would produce gaps or overlapping months | Not fixed — the system prompt asks for chronological order, but nothing enforces it post-extract. **Path-6-specific risk carried from slice A DRAFT.** |
| **Wikipedia page bloat** | PPF page is ~330KB HTML → truncation at 200k fires. Truncation happens AFTER the rate table (table is near the top of the article) so extraction is unaffected — but flags a warning in `agent_run` context | Truncation warn was noted; historical rate table appears in first ~50KB so truncation doesn't clip it |
| **Wikipedia API rate-limit** | Rare on the human-facing URL; not observed | Not mitigated (single-shot `requests.get`) |
| **Wikipedia editorial restructure** | Rate history table moved, renamed, or split into sub-articles | Not mitigated; would surface as `structured_output != None` with empty `periods` list → caught by presence-check at `SetPpfRate.py:118-119`; ak-2r8 preserves last-good |
| **Sonnet emits periods with wrong `from` format** | Schema requires string but doesn't enforce `YYYY-MM` shape → `_expand_periods_to_monthly` would crash on `int(start[:4])` if given `"April 2024"` | Would surface as an exception → `AIRateTask` catches → `(None, error)` return → cycle Failed |

Sample `agent_run`:
```
agent_run agent=rate.ppf model=sonnet turns=1 tools_called=0 latency_ms=~5500 status=ok
```

### C. SDK-feature-menu fit

Same as path 5. Additionally:
- ❌ **Post-extract format validation** for `from: "YYYY-MM"` shape —
  a Pydantic model over the extracted `periods` list would catch the
  `"April 2024"` class before it reaches `_expand_periods_to_monthly`.

### D. Score

| Bar | Score | Note |
|---|---|---|
| No silent failures | 🟡 | Everything covered EXCEPT out-of-order periods which would silently produce a gap-riddled monthly rate file (still schema-valid, still gets swapped in) |
| Measurable accuracy | 🟡 | Presence + non-empty check only; no expansion-sanity check (expected: ~300 monthly rows for 1999-2026 quarterly-since-2016) |
| Self-healing / retries | 🟡 | Same as paths 4-5 |
| Smarter outputs | 🟢 | Structured output + post-extract expansion — cleanest of the LLM paths |

### E. Recommended changes (path-6-specific)

- **Trivial:** RR9 + RR10 shared with paths 4-5.
- **Trivial (NEW, RR23 below):** Pydantic model over the `periods`
  extract result — validate each period's `from` matches `^\d{4}-\d{2}$`
  and that the list is chronologically sorted. Wrap
  `_expand_periods_to_monthly` in this check so a bad extract fails
  the cycle instead of writing gap-riddled monthly rows.
- **Structural:** RR15 secondary URL — no obvious secondary source with
  the same historical depth. `bankbazaar.com` and `paisabazaar.com`
  have PPF tables but shallow (only recent quarters). Deprioritized.
- **Structural (NEW, RR24 below):** Monthly-row-count sanity — expected
  count is deterministic from the current date and 1999-04 start. If
  actual count deviates > 5%, WARN.

---

## 8. Path 7: IPO/orphan-sell prices (`utils/AIHelper.py` ← `StocksService.py:337`)

Grounded post-Family-B-v2 + shared v4/v5. Distinct from paths 4-6 in
that it's **event-triggered, not scheduled** — fires per-cycle from
orphan-sell reconciliation, not from the scheduler.

### A. Current implementation (post v5)

- **Trigger:** on-demand from `StocksService._resolve_ipo_allotment_prices`
  (`services/StocksService.py:306`), which itself is called from the
  orphan-sell reconciliation flow when an equity sell can't be matched
  against a corresponding buy in transactions.
- **Source:** `https://www.chittorgarh.com/report/mainboard-ipo-list-in-india/82/`
  — the most-cited Indian IPO reference, comprehensive for mainboard
  IPOs going back ~15 years.
- **Fetch + extract:** `fetch_and_extract` with `_IPO_SCHEMA`. Caller's
  prompt (listing symbols/ISINs) passed as `extra_context` alongside the
  page body — sonnet sees both.
- **Output shape:** `{"results": [{"symbol", "isin", "allotment_price", "allotment_date", "source"}]}`.
  `source` field carries provenance (e.g. `"IPO 2021-03-15"` or
  `"demerger from XYZ"`) — the ONLY path in the fleet with in-band source
  provenance.
- **No coverage gates** — this path bypasses `BaseRateTask` because it's
  not a scheduled rate task. Its "coverage" semantics are per-request
  (some ISINs matched, some not) not per-cycle.

### B. Failure inventory (post-verify)

| Failure class | Root cause | Fix landed |
|---|---|---|
| **Shared v2/v4/v5 fixes** | Empty-TextBlocks + property-key + tool wandering | Shared with paths 4-6 |
| **Very-recent IPO gap** (Overseer-noted) | chittorgarh.com's table lags real IPO listings by ~2 weeks. Fresh IPO → sonnet correctly emits `{"results": []}` → reconciler can't generate synthetic buy → orphan sell unmatched | Not fixed — flagged in slice A as RR14 (BSE/NSE fallback). Still unshipped. |
| **Chittorgarh page bloat** | The mainboard IPO list is ~15 years deep, ~500 rows. Page HTML is ~450KB → truncation at 200k fires and clips off older IPOs | Recent IPOs (last ~5 years) preserved in the first 200k window; older IPOs may miss if the sort order is oldest-first. If ordering flips over time, older-IPO lookups would silently start returning empty. |
| **ISIN mismatch on lookalikes** | System prompt says "match by ISIN preferentially; symbol as secondary" — if two IPOs share a symbol (rare — company demergers) sonnet could pick wrong | Would surface as a wrong `allotment_price` — no post-extract validation. Reconciler downstream would produce a wrong synthetic buy that fails to match. |
| **Demerger vs IPO ambiguity** | System prompt allows "use listing-day price if you can't find issue price" for demergers — a legitimate call, but the `source` field is the only signal downstream telling the operator this happened | Documented in system prompt (`AIHelper.py:44-48`); reliance on `source` field for eyeball |
| **Prompt injection via ISIN list** | Caller passes user-derived ISIN list as `extra_context` verbatim. A malicious ISIN string with embedded instructions could steer sonnet | Low real-world risk (ISINs are validated upstream in the reconciler), but the pattern IS exposed. Would benefit from the `<user_data>...</user_data>` wrap ak-3eo established in WealthDigestTask. |

Sample `agent_run`:
```
agent_run agent=stocks.ipo model=sonnet turns=1 tools_called=0 latency_ms=~7500 status=ok
```
(Higher latency — page is large + `extra_context` per call.)

### C. SDK-feature-menu fit

Same as paths 4-6. Additionally:
- ❌ **BSE/NSE fallback for recent IPOs** — RR14 / R25 unshipped.
- ❌ **`<user_data>` wrap on `extra_context`** — prompt-injection defense
  from ak-3eo (WealthDigestTask) not backported here.
- ❌ **Per-request caching by ISIN-list-hash** — orphan-sell reconciliation
  can hit the same ISIN list multiple times (same set of unmatched sells
  on retry cycles); caching would avoid re-fetching the same 450KB HTML +
  re-invoking sonnet.

### D. Score

| Bar | Score | Note |
|---|---|---|
| No silent failures | 🟡 | Recent-IPO gap is a known KIND of silent failure — reconciler doesn't distinguish "IPO not in table" from "IPO doesn't exist yet" |
| Measurable accuracy | 🟡 | `source` field is the accuracy hedge; no cross-source check for allotment_price |
| Self-healing / retries | 🟡 | No caching = every reconciliation cycle re-fetches; wasteful but self-healing (transient failure resolves next cycle) |
| Smarter outputs | 🟢 | Provenance field + explicit demerger-handling rule in system prompt |

### E. Recommended changes (path-7-specific)

- **Trivial (NEW, RR25 below):** `<user_data>...</user_data>` wrap on
  `extra_context` in `fetch_and_extract` — backport from ak-3eo. Cheap
  defense against prompt-injection via user-derived ISIN lists.
- **Trivial:** RR9 + RR10 shared.
- **Structural:** RR14 — BSE/NSE fallback for empty `results` (unshipped
  from slice A; still needed).
- **Structural (NEW, RR26 below):** `lru_cache` keyed by
  sorted-ISIN-list-hash, TTL = 24h. Orphan-sell cycles often hit the
  same ISIN set repeatedly; caching avoids the ~7.5s SDK cost per
  redundant invocation.
- **Ambitious:** Post-extract cross-source oracle — for each returned
  allotment, spot-check against NSE listing-day price via `nsepython`
  or a similar library. Divergence > 5% → alert. Catches the
  ISIN-lookalike + demerger-misidentification classes.

---

## 9. Cross-cutting analysis

### 9.1 Telemetry gap audit

| Path | Has structured per-run log? | Has expected-vs-actual? | Watcher / alert? |
|---|---|---|---|
| 1 (MF) | ❌ (log lines but not structured) | 🟡 (`Pass N complete: M/total`) | ❌ |
| 2 (NPS) | ❌ | 🟡 (`Total scheme rates collected`) | ❌ |
| 3 (Kite) | ❌ | 🟡 (`N equity instruments`) | ❌ |
| 4–7 (LLM) | ✅ `agent_run` (post-R5) | 🟡 (`structured_output is not None` is binary) | ❌ |

**Finding:** R5's `agent_run` log line is the only structured per-run
emission anywhere in the rate paths. Paths 1–3 have human-readable info
logs but nothing grep-friendly for failure-rate computation. **A
unified `rate_run` log line, mirroring `agent_run`'s shape, is the
single highest-leverage cross-cutting change.**

Proposed shape:

```
rate_run path=mf cycle_id=N expected=5000 succeeded=4982 failed=18 \
         latency_ms=125300 status=ok stale_served=false
```

This unblocks:
- Failure-rate computation per path over arbitrary windows
- Latency percentiles per path
- Coverage drift detection (path 1's `failed` count rising over time)
- A single Mayor / infra-side health probe that watches the log shape

### 9.2 Schema validation patterns

| Path | Has schema validation? | Type |
|---|---|---|
| 1 (MF) | ⛔ | try/except on dict access |
| 2 (NPS) | 🟡 | column-presence check |
| 3 (Kite) | 🟡 | dict-key lookup |
| 4–7 (LLM) | ✅ | sonnet `output_format` json_schema |

**Finding:** Paths 4–7 are now the schema-validation gold standard in
the codebase (post-Family-B-v2). Paths 1–3 should be brought up using
Pydantic models against their respective upstream contracts.

### 9.3 Fallback / serve-stale strategies

**No path implements serve-stale.** Every cycle is all-or-nothing — a
successful cycle overwrites the previous JSON; a failed cycle either
writes a partial file (path 1) or just logs and leaves the previous
file untouched (paths 2–7, via the `safe_replace_file` pattern).

The `safe_replace_file` pattern is the closest thing to serve-stale we
have: it appears to refuse to overwrite a good file with a bad one
(but the details depend on `BaseTask`'s implementation, which is out
of scope here and worth confirming).

**Recommendation:** A `CachedRateStore` mixin with `freshness_seconds`
+ `serve_stale_max_age_hours` per path. Default behavior: on cycle
failure, if last successful write was within `serve_stale_max_age_hours`,
keep the existing file and return success; otherwise emit a
`rate_run … status=stale_exhausted` log line and mail Overseer.

### 9.4 Cache freshness

Rates have natural staleness tolerance:
- MF NAVs are stamped daily by AMFI cutoff; intra-day staleness is irrelevant
- NPS NAVs are stamped daily
- Kite instruments change weekly at most (new listings, name changes)
- Gold spot rate updates ~every business minute but we don't need real-time
- EPF/PPF rates change quarterly to annually
- IPO allotment prices are immutable post-listing

**Implication:** every path could safely serve a cached value if the
cached age is <24h. None do.

### 9.5 What the post-Family-B-v2 observation showed (retitled from "will tell us")

Slice A was written before Family B v2 had cycled in prod. Slice B is
written after ~2 months of production observation + 5 iteration commits.
Summary of what the observation window actually surfaced:

| Question at slice-A time | Post-verify answer |
|---|---|
| Is empty-TextBlocks cured under v2? | ✅ Yes for paths 4-7. `turns=1 tools_called=0 status=ok` is the healthy shape. |
| Any new failure classes v2 introduced? | ✅ Three: (a) API 400 on invalid property keys in GOLD/EPF schemas — fixed v4 `b2abe3d`; (b) sonnet wandering into tools on large HTML — fixed v5 `577ae6c` allowed_tools=[]; (c) SDK stderr blindness — fixed v3 stderr wiring `2fd2b3f`. |
| Latency baselines? | Gold ~4.5s; EPF ~6s; PPF ~5.5s; IPO ~7.5s. All well under `max_turns=3` × per-turn timeout. |
| Truncation firing? | Rarely for paths 4-6 (ibjarates ~50KB, cleartax ~130KB, Wikipedia PPF ~330KB — truncates but rate table is in first 50KB unaffected). Path 7 chittorgarh is ~450KB — truncation clips older IPOs, may become a real issue if sort order flips. |
| EPF Wikipedia source? | ⚠️ Went dry. Pivoted to cleartax.in via v6 `395a404`. Documented source-audit ranking captured in §6. |
| Downstream consumers happy with v2 outputs? | ✅ Yes for the LLM paths. Path 1 (MF) had its own arc (ak-lp6 → ak-539 → ak-5jq → ak-nl4 → ak-iwj → ak-2r8) covering coverage-gates, retry classification, dedup, and reference-aware storage. |

---

## 10. Prioritized recommendations table

Same legend as `agent-deep-study-2026-05.md` §8: **Trivial** = single
small file change, **Structural** = multiple files / contract change,
**Ambitious** = new infrastructure / metrics / fallback layer.

**Status column** (added slice B): ✅ shipped since slice A, 🟡 partially
shipped, ⏳ still pending, N/A superseded by another change.

| ID | Recommendation | Path | Bucket | Impact | Effort | Depends on | Status (2026-08-17) |
|---|---|---|---|---|---|---|---|
| **RR1** | Emit `rate_run` structured log line per cycle across ALL 7 paths (shape mirrors `agent_run`) | all | structural | **HIGHEST** (unblocks all failure-rate measurement) | 1 day | – | 🟡 partial — paths 4-7 have `agent_run` via `run_query_collect`; paths 1-3 still lack a unified line |
| **RR2** | Gate `safe_replace_file` in path 1 (MF) on coverage threshold (≥90%) | 1 | trivial | high | 30 min | – | ✅ shipped ak-539 `969daa3` (98% partial-success gate) + ak-539 v2 `db9288d` (50% hard-floor) — thresholds tuned tighter than proposed |
| **RR3** | Replace path 1's `force_close=True` connector with keepalive | 1 | trivial | medium (likely fixes mfapi rate-limit observations) | 15 min | – | ✅ shipped ak-539 `969daa3` (connector pooling) |
| **RR4** | Pydantic schema for mfapi.in scheme response | 1 | trivial | medium (drift safety) | 1 h | – | ⏳ still pending |
| **RR5** | Replace path 2's `>1000 bytes` heuristic with actual TSV/HTML byte sniff | 2 | trivial | high (silent TSV-as-HTML mis-parse) | 30 min | – | ⏳ still pending |
| **RR6** | Per-request retry on connection / timeout error in path 2 | 2 | trivial | high (covers npstrust flakiness) | 30 min | – | ⏳ still pending |
| **RR7** | Concurrency pool (4-way) in path 2 to drop worst-case cycle latency from 112min to ~28min | 2 | trivial | high (under 60-min interval reliably) | 1 h | – | ⏳ still pending |
| **RR8** | Kite token health probe (15-min interval, mail-on-near-expiry) | 3 | structural | high (eliminates 5h-blind-spot on expired tokens) | half day | – | ⏳ still pending |
| **RR9** | Prompt caching (`cache_control: ephemeral`) on the 4 rate-fetcher system prompts | 4, 5, 6, 7 | trivial | medium (cost) | 2 h | – | ⏳ still pending |
| **RR10** | One HTTP retry inside `fetch_and_extract` on `requests.RequestException` | 4–7 | trivial | medium | 30 min | – | ⏳ still pending (partly mitigated by ak-2r8's preserve-last-good on cycle failure) |
| **RR11** | `CachedRateStore` mixin: serve-stale-on-failure with `freshness_seconds` + `serve_stale_max_age_hours` per path | all | structural | **high** (largest single reliability lever) | 1–2 days | RR1 | 🟡 partial — ak-539 v2 hard-floor + ak-2r8 fleet-wide BaseRateTask gate implement "preserve last-good on failure" for all 5 rate tasks. A time-based freshness window is NOT enforced yet — files stay preserved until the next successful cycle regardless of age |
| **RR12** | NPS path: dynamic PFM/scheme ID discovery instead of hardcoded `range(1,15)` × `range(1,17)` | 2 | structural | medium (silent invisibility to new PFMs) | 1 day | – | ⏳ still pending |
| **RR13** | Pydantic models for NPS TSV columns + Kite instrument dict | 2, 3 | structural | medium (drift safety, matches RR4) | half day | – | ⏳ still pending |
| **RR14** | BSE/NSE fallback for path 7 when chittorgarh returns empty `results` (covers fresh IPOs) | 7 | structural | high (closes a known gap from the cascade) | 1 day | – | ⏳ still pending |
| **RR15** | Secondary URL probe for path 4 (MMTC-PAMP or Augmont gold) and path 5 (EPFO's own URL) | 4, 5 | structural | medium (defense in depth) | 1 day per path | – | ⏳ still pending (EPF source-audit for v6 pivot identified `groww.in` as a candidate secondary — see §6.A) |
| **RR16** | Path 2 cycle interval increase to 240min (4h) to match NPS NAV's actual update cadence; reduces npstrust load by 4× | 2 | trivial | medium (kindness + cost) | 15 min | – | ⏳ still pending |
| **RR17** | Per-path freshness alert: if last successful cycle was >2 × interval ago, mail Overseer | all | structural | high (catches blocked cycles) | half day | RR1 | ⏳ still pending |
| **RR18** | A daily reliability report: per-path success rate, p50/p99 latency, stale-served count, coverage trend | all | ambitious | high (sets up a real SRE feedback loop) | 2-3 days | RR1, RR11, RR17 | ⏳ still pending |
| **RR19** | Replace path 4–7's single-source upstream with a 2-of-3 cross-check (primary + 2 fallbacks; majority wins; mismatch = alert) | 4, 5, 6, 7 | ambitious | high (correctness, not just availability) | 1 week per path | RR15 | ⏳ still pending |
| **RR20** *(new slice B)* | Path 4: schema-validation partial-coverage counter — emit `rate_partial_coverage path=gold present=N/3` when `carat_24/22/18` not all populated | 4 | trivial | medium (catches 2/3-purities silent-degradation the presence-check misses) | 1 h | RR1 | ⏳ new |
| **RR21** *(new slice B)* | Path 5: post-extract row-count sanity — expected `len(data) ≈ (current_year - 2014 + 1) × 12`; WARN on >20% deviation | 5 | trivial | high (catches cleartax layout regression) | 1 h | – | ⏳ new |
| **RR22** *(new slice B)* | Path 5: weekly HEAD probe on cleartax.in URL — catches URL rot before 7-day cycle | 5 | trivial | medium (accelerates detection of the URL-drift class) | half day | – | ⏳ new |
| **RR23** *(new slice B)* | Path 6: Pydantic model over extracted `periods` — validate `from ~ YYYY-MM` + chronologically sorted before `_expand_periods_to_monthly` | 6 | trivial | high (only silent-failure class left in path 6) | half day | – | ⏳ new |
| **RR24** *(new slice B)* | Path 6: monthly-row-count sanity check post-expand (deterministic from 1999-04 start date) | 6 | trivial | medium | 1 h | – | ⏳ new |
| **RR25** *(new slice B)* | Path 7: wrap `extra_context` in `<user_data>...</user_data>` — backport prompt-injection defense from ak-3eo (WealthDigestTask) | 7 | trivial | medium (closes injection surface via user-derived ISIN list) | 30 min | – | ⏳ new |
| **RR26** *(new slice B)* | Path 7: `lru_cache` keyed by sorted-ISIN-list-hash, TTL=24h — avoid re-fetching + re-invoking sonnet on redundant orphan-sell cycles | 7 | trivial | medium (cost + latency) | 2 h | – | ⏳ new |

**Recommended ship order if you only do five things:**

**(Slice A original)** RR1 → RR2 → RR5+RR6+RR7 (path 2 trivial pack) →
RR11 → RR17. Observability bedrock, path-1 partial-write fix, path-2
reliability pack, serve-stale layer, per-path freshness alert.

**(Slice B revised — post-what-shipped)** With RR2/RR3 already shipped
(ak-539) and RR11 partially shipped (ak-2r8 BaseRateTask preserve-last-
good), the current best-per-effort focus shifts:

1. **RR1 completion** — extend `rate_run` shape to paths 1-3 so ALL
   paths emit a uniform structured line. Unblocks RR17 + RR18 + a real
   ops dashboard.
2. **Path 2 trivial pack (RR5+RR6+RR7)** — highest-leverage unshipped
   work; NPS is our least-observed path and the trivial pack is <2 hours
   total.
3. **RR21+RR23** — cheap post-extract sanity checks for paths 5 + 6;
   each is a 1-hour add and closes the "silent gap-riddled output"
   class in the two Wikipedia-derived paths.
4. **RR9** — prompt caching on the 4 rate-fetcher system prompts. Static
   prompts + weekly / daily cycles = solid cache-hit yield.
5. **RR17** — per-path freshness alert. With ak-2r8's preserve-last-
   good, a broken cycle now silently keeps serving stale — alerts are
   how the operator finds out.

That takes the fleet from "measured, alerted, degrading gracefully" (as
the original ship order promised) to "measured across ALL 7 paths, drift-
detected, and Q3-ready for a real reliability dashboard."

---

## 11. Open questions for Lead / Overseer

1. **Cycle intervals** (paths 4–7) — I noted `TBD`. The verify ping
   should surface them. If they're inherited from pre-v2 baseTask
   init, they're likely 4h (gold) / 60min (others). Worth confirming
   against actual schedular config.
2. **`safe_replace_file` semantics** — I assumed it refuses to overwrite
   on partial / failed data, but this depends on `BaseTask`. Should be
   verified before committing to RR11. If it does NOT refuse, RR2's
   coverage gate becomes more important.
3. **Cost vs latency for prompt caching** — RR9 is "trivial" but the
   actual cost savings depend on cache-hit ratio across paths 4–7. If
   each path runs every 4 hours and the 5-min TTL never overlaps,
   caching pays back zero. Worth a calculation against actual cycle
   frequencies before scoping.
4. **Path 7 trigger frequency** — IPO lookup is on-demand during orphan-
   sell reconciliation, not on a fixed cycle. Caching dynamics differ.
   Recommend a separate `lru_cache` keyed by ISIN-list-hash, with the
   stocks.ipo agent_run logs feeding cache invalidation.

---

*End of study.*

**Slice A** landed as `fd9b89e` on 2026-06-06 (sections 1-3 non-LLM
paths + cross-cutting scaffolding + RR1-RR19).

**Slice B** landed 2026-08-17 (sections 4-8 grounded post-Family-B-v2 +
all iteration fixes; §9.5 retitled from "will tell us" to "observation
showed"; §10 marks shipped / partial / pending status + adds RR20-RR26).

**Update cadence**: this doc is expected to age gracefully. When a new
RR ships or a new failure class surfaces, add a row / update a status.
When a new rate path is added (e.g. a currency conversion feed), add
a §11 for it. The reliability study is now a living inventory, not a
one-shot artifact.
