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

Sections 1–3 are the non-LLM paths and are ground truth as of this
commit. Sections 4–7 are the LLM paths; **they are DRAFT pending the
Family B v2 post-deploy verify ping** (~5 hours after the schedular
restart that follows `880a442` landing). The Family B v2 architecture
rewrite changes the failure surface materially, so characterizing the
LLM paths *before* observing the new architecture's behavior would
produce a study that's stale on landing. Section 9 will be updated when
the verify data lands; the table in §10 reserves slots for it.

---

## Table of Contents

1. [Rate-path inventory](#1-rate-path-inventory)
2. [Path 1: Mutual fund NAVs (`SetMfRate.py`)](#2-path-1-mutual-fund-navs-setmfratepy)
3. [Path 2: NPS NAVs (`SetNPSRate.py`)](#3-path-2-nps-navs-setnpsratepy)
4. [Path 3: Stock instrument metadata (`SetKiteStockDetails.py`)](#4-path-3-stock-instrument-metadata-setkitestockdetailspy)
5. [Path 4: IBJA gold rate (`SetIBJAGoldRate.py`) — DRAFT](#5-path-4-ibja-gold-rate-setibjagoldratepy--draft)
6. [Path 5: EPF interest rate (`SetEPFRate.py`) — DRAFT](#6-path-5-epf-interest-rate-setepfratepy--draft)
7. [Path 6: PPF interest rate (`SetPpfRate.py`) — DRAFT](#7-path-6-ppf-interest-rate-setppfratepy--draft)
8. [Path 7: IPO/orphan-sell prices (`utils/AIHelper.py` ← `StocksService.py:337`) — DRAFT](#8-path-7-ipoorphan-sell-prices-utilsaihelperpy--stocksservicepy337--draft)
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

## 5. Path 4: IBJA gold rate (`SetIBJAGoldRate.py`) — DRAFT

**DRAFT pending Family B v2 post-deploy verify.** This section is
sketched against the architecture but will be ground-truthed once the
post-deploy `agent_run` lines for `rate.gold` start appearing in
journalctl (~5h after the schedular bounce that follows `880a442`).

### A. Current implementation (v2)

- **Interval:** TBD (read from baseTask init; was 4h pre-v2; verify post-deploy)
- **Source:** `https://ibjarates.com/` (Overseer-acked)
- **Fetch:** `requests.get` via `utils/web_extract.fetch_and_extract`
- **Extract:** sonnet `output_format` json_schema (`GOLD_SCHEMA` in `SetIBJAGoldRate.py`)
- **Error capture:** `(dict | None, str)` tuple-return from `AIRateTask.fetch_rates_via_ai`

### B. Failure inventory (to fill post-verify)

Expected `agent_run` shape post-v2:
- Success: `agent_run agent=rate.gold model=sonnet turns≥1 tools_called=0 latency_ms=N status=ok`
- HTTP failure: returned as `(None, "HTTP fetch failed for ibjarates.com: <e>")`, `agent_run` not emitted (failure happens before SDK call)
- Sonnet extraction failure: `agent_run … status=error error_class=...`
- Output-format missing: `(None, "Sonnet returned no structured_output (output_format ignored)")`

Failure modes to characterize post-verify:
- ibjarates.com response latency (slow upstream → SDK call sees truncated body)
- ibjarates.com page restructure (sonnet would still attempt extraction; coverage drops in `result.structured_output`)
- 200k-char body truncation rate (if ibjarates.com response < 200k, truncation never fires)

### C. SDK-feature-menu fit (post-v2)

Now lit (post-Family-B-v2):
- ✅ `output_format` json_schema
- ✅ Structured `agent_run` log line
- ✅ Single-source-of-failure isolation (HTTP and extraction errors don't blur)

Still dark:
- ❌ Prompt caching on `GOLD_SYSTEM_PROMPT` (static; pays back inside 2 invocations)
- ❌ Citations / source URL provenance enforcement
- ❌ Schema-validation-failure observability (right now the only signal is `structured_output is None`)
- ❌ Cache / serve-stale on failure
- ❌ Secondary source fallback (e.g., MMTC-PAMP gold prices)

### D. Score (provisional)

| Bar | Score | Note |
|---|---|---|
| No silent failures | 🟢 | v2's `(None, detail)` tuple-return surfaces all known failure modes |
| Measurable accuracy | 🟡 | `structured_output is not None` is binary; no per-field-coverage check |
| Self-healing / retries | 🟡 | `max_turns=3` inner retry on schema validation; no outer HTTP retry |
| Smarter outputs | 🟢 | Best-in-class for the rate-fetcher class as of `880a442` |

### E. Recommended changes (provisional)

- **Trivial:** Add prompt caching (`cache_control: ephemeral`) on `GOLD_SYSTEM_PROMPT`.
- **Trivial:** Add one HTTP retry on `requests.get` failure inside `fetch_and_extract` (single retry, 2s backoff). Currently a transient connection failure = entire cycle lost.
- **Structural:** Cache the last successful `GOLDRATE.json` on disk with timestamp. On failure, serve-stale if <24h old.
- **Structural:** Add a secondary URL probe (mmtcpamp.com or augmont.com) — if ibjarates.com fails, try the secondary before returning the failure.

---

## 6. Path 5: EPF interest rate (`SetEPFRate.py`) — DRAFT

**DRAFT pending Family B v2 post-deploy verify.** Same shape as §5.

### A. Current implementation (v2)

- **Interval:** TBD
- **Source:** `https://en.wikipedia.org/wiki/Employees%27_Provident_Fund_Organisation` (Overseer-acked; chose Wikipedia over EPFO's own URLs as more stable)
- **Fetch + extract:** same `fetch_and_extract` pipeline as path 4
- **Schema:** `{"data": [{"Year": "YYYY-MM", "Interest Rate": float}]}`
- **System prompt encodes April–March FY expansion** (so the model has to expand a single-year `8.25%` into 12 monthly rows)

### B. Failure inventory (to fill post-verify)

Wikipedia is far more stable than EPFO's own URL but has its own
failure surfaces:
- Editorial restructure (table moved / renamed) → sonnet would
  attempt extraction and may produce partial results
- Wikipedia API rate-limit for the human-facing URL (rare)
- Page bloat (Wikipedia EPFO page is large; 200k truncation may fire
  more often here than for path 4)

### C–E

Same recommendations as path 4 — caching, HTTP retry, serve-stale,
secondary URL (e.g., EPFO's own `epfindia.gov.in` despite Wikipedia
being chosen as primary for stability).

---

## 7. Path 6: PPF interest rate (`SetPpfRate.py`) — DRAFT

**DRAFT pending Family B v2 post-deploy verify.** Same shape as §5–6.

### A. Current implementation (v2)

- **Interval:** TBD
- **Source:** `https://en.wikipedia.org/wiki/Public_Provident_Fund_(India)` (Overseer-acked)
- **Schema:** `{"periods": [{"from": "YYYY-MM", "rate": float}]}` — period-based, then expanded via `_expand_periods_to_monthly` (preserved unchanged from v1)
- **Post-extract:** `_expand_periods_to_monthly` walks the period list and emits one rate row per month between consecutive `from` dates

### B–E

Same as paths 4–5. **Additional path-6-specific risk:** the period
expansion logic depends on the upstream period list being chronologically
ordered. If Wikipedia's PPF page lists periods out of order (e.g., a
historical correction inserted at the bottom of the table), the
expansion produces gaps or duplicates. Worth testing the expansion
post-verify with a deliberately-shuffled period input.

---

## 8. Path 7: IPO/orphan-sell prices (`utils/AIHelper.py` ← `StocksService.py:337`) — DRAFT

**DRAFT pending Family B v2 post-deploy verify** + the specific
post-deploy follow-up Lead acknowledged: **BSE/NSE fallback for
very-recent IPOs not on chittorgarh.com.**

### A. Current implementation (v2)

- **Trigger:** orphan-sell reconciliation cycle in `StocksService._lookup_ipo_prices`. Called with a prompt listing ISINs that the reconciler needs IPO allotment prices for.
- **Source:** `https://www.chittorgarh.com/report/mainboard-ipo-list-in-india/82/`
- **Fetch + extract:** `fetch_and_extract` with the IPO-specific schema; the caller's prompt is passed as `extra_context` so sonnet sees both the page body and the list of stocks to look up.
- **Output shape:** `{"results": [{"symbol", "isin", "allotment_price", "allotment_date", "source"}]}`
- **Known limitation (Overseer-noted, scoped out of Family B v2):** the chittorgarh table doesn't include IPOs from the last ~2 weeks. When the reconciler hits a fresh IPO, the lookup returns empty `results`. The reconciler then can't generate the synthetic buy and the orphan sell remains unmatched.

### B–E

Same recommendations as paths 4–6, plus:

- **R25 (from `agent-deep-study-2026-05.md` §9.3):** BSE/NSE fallback.
  When chittorgarh returns 0 matching ISINs, query NSE's bhavcopy archive
  or BSE's issue-history API for the listing-day price. Implementation
  would slot into `AIHelper.fetch_via_ai` as a secondary path after the
  primary returns empty `results`.

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

### 9.5 What the post-Family-B-v2 verify will tell us

Specific data points to populate sections 5–8 once the verify ping
lands:

- `agent_run agent=rate.{gold,epf,ppf} turns=N tools_called=0` — N >= 1
  confirms the empty-TextBlocks failure mode is cured; N = 0 means v2
  has a new failure mode worth diagnosing
- `agent_run … latency_ms=L` — to compare against pre-v2 baselines
  (which were artificially low because the model was no-op'ing)
- `agent_run … status=error error_class=...` for any failures —
  enumerate the failure classes to populate §5–8 D-tables
- The persisted `GOLDRATE.json` / `EPFRATE.json` / `PPFRATE.json`
  contents — should match the schemas declared in code
- For path 7: chittorgarh.com response size (to verify the 200k
  truncation isn't kicking in for the IPO list)

---

## 10. Prioritized recommendations table

Same legend as `agent-deep-study-2026-05.md` §8: **Trivial** = single
small file change, **Structural** = multiple files / contract change,
**Ambitious** = new infrastructure / metrics / fallback layer.

| ID | Recommendation | Path | Bucket | Impact | Effort | Depends on |
|---|---|---|---|---|---|---|
| **RR1** | Emit `rate_run` structured log line per cycle across ALL 7 paths (shape mirrors `agent_run`) | all | structural | **HIGHEST** (unblocks all failure-rate measurement) | 1 day | – |
| **RR2** | Gate `safe_replace_file` in path 1 (MF) on coverage threshold (≥90%) | 1 | trivial | high | 30 min | – |
| **RR3** | Replace path 1's `force_close=True` connector with keepalive | 1 | trivial | medium (likely fixes mfapi rate-limit observations) | 15 min | – |
| **RR4** | Pydantic schema for mfapi.in scheme response | 1 | trivial | medium (drift safety) | 1 h | – |
| **RR5** | Replace path 2's `>1000 bytes` heuristic with actual TSV/HTML byte sniff | 2 | trivial | high (silent TSV-as-HTML mis-parse) | 30 min | – |
| **RR6** | Per-request retry on connection / timeout error in path 2 | 2 | trivial | high (covers npstrust flakiness) | 30 min | – |
| **RR7** | Concurrency pool (4-way) in path 2 to drop worst-case cycle latency from 112min to ~28min | 2 | trivial | high (under 60-min interval reliably) | 1 h | – |
| **RR8** | Kite token health probe (15-min interval, mail-on-near-expiry) | 3 | structural | high (eliminates 5h-blind-spot on expired tokens) | half day | – |
| **RR9** | Prompt caching (`cache_control: ephemeral`) on the 4 rate-fetcher system prompts | 4, 5, 6, 7 | trivial | medium (cost) | 2 h | – |
| **RR10** | One HTTP retry inside `fetch_and_extract` on `requests.RequestException` | 4–7 | trivial | medium | 30 min | – |
| **RR11** | `CachedRateStore` mixin: serve-stale-on-failure with `freshness_seconds` + `serve_stale_max_age_hours` per path | all | structural | **high** (largest single reliability lever) | 1–2 days | RR1 |
| **RR12** | NPS path: dynamic PFM/scheme ID discovery instead of hardcoded `range(1,15)` × `range(1,17)` | 2 | structural | medium (silent invisibility to new PFMs) | 1 day | – |
| **RR13** | Pydantic models for NPS TSV columns + Kite instrument dict | 2, 3 | structural | medium (drift safety, matches RR4) | half day | – |
| **RR14** | BSE/NSE fallback for path 7 when chittorgarh returns empty `results` (covers fresh IPOs) | 7 | structural | high (closes a known gap from the cascade) | 1 day | – |
| **RR15** | Secondary URL probe for path 4 (MMTC-PAMP or Augmont gold) and path 5 (EPFO's own URL) | 4, 5 | structural | medium (defense in depth) | 1 day per path | – |
| **RR16** | Path 2 cycle interval increase to 240min (4h) to match NPS NAV's actual update cadence; reduces npstrust load by 4× | 2 | trivial | medium (kindness + cost) | 15 min | – |
| **RR17** | Per-path freshness alert: if last successful cycle was >2 × interval ago, mail Overseer | all | structural | high (catches blocked cycles) | half day | RR1 |
| **RR18** | A daily reliability report: per-path success rate, p50/p99 latency, stale-served count, coverage trend | all | ambitious | high (sets up a real SRE feedback loop) | 2-3 days | RR1, RR11, RR17 |
| **RR19** | Replace path 4–7's single-source upstream with a 2-of-3 cross-check (primary + 2 fallbacks; majority wins; mismatch = alert) | 4, 5, 6, 7 | ambitious | high (correctness, not just availability) | 1 week per path | RR15 |

**Recommended ship order if you only do five things:**

**RR1 → RR2 → RR5+RR6+RR7 (path 2 trivial pack) → RR11 → RR17.** That's
the observability bedrock, the path-1 partial-write fix, the path-2
reliability pack, the serve-stale layer, and the per-path freshness alert.
Together they take a path from "best-effort, silent on failure" to
"measured, alerted, degrading gracefully."

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

*End of study (slice A complete; slice B sections 5–8 are DRAFT pending
Family B v2 post-deploy verify). Output:
`docs/rate-reliability-study-2026-06.md`. Sections 5–8 + 9.5 will be
updated in a follow-up commit once `agent_run` for `rate.{gold,epf,ppf}`
and `stocks.ipo` lines start appearing in journalctl.*
