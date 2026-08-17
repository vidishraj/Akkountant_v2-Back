# ak-32o v3 HDFC re-parse runbook — mailProcessor path

**Bead**: ak-32o
**Approach**: v3 — mailProcessor `_process_single_pdf_email` path (Q1 (A) confirmed).
**Prior**: v1/v2 (agent/ak32o-fire-script @ 950be49 / bef7505 — reprocess_pdf path). REFERENCE ONLY.
**Scope**: 12 canonical HDFC_DEBIT statement files, Vidish's user_id (Q4 confirmed).
**Fire authority**: Infra fires. Backend ships the tool; Lead verifies; Reviewer signs off;
Overseer greenlights; Mayor merges. **No destructive prod write from backend or from this doc.**

## Q-gate status

| Q | Topic | Status | Blocking? |
|---|-------|--------|-----------|
| Q1 | Invocation path | GO — `_process_single_pdf_email` (path A) | No |
| Q4 | User scope | GO — Vidish's user_id | No |
| Q2 | Delta semantics (per-file vs inter-file) | HELD — Overseer to answer | **Blocks Phase 4** (preview only) |
| Q3 | Pin exact 12 fileIDs vs whichever-query-returns | HELD — Overseer to answer | Softens Phase 1 to WARN |

Phase 4 preview runs unconditionally and emits both delta_per_file and delta_inter_file per
file — Overseer picks semantics from real numbers.

## Reviewer v2 fix summary (post hq-wisp-soqod7)

| Fix | Location | Note |
|-----|----------|------|
| MAJOR — Phase 4 sums exclude synthetic OPENING_BALANCE rows | `phase4_preview` in orchestrator | Verify result: current backstop (`_run_hdfc_file_level_reconciliation`) does NOT insert any OPENING_BALANCE-class row — DIVERGED path only re-runs chunks under `force_no_mask=True`, producing more real `CLAUDE_CODE` bank rows. The filter is defensive against Phase 4's OWN future insert polluting a re-run of the preview. `excluded_openbal_by_tag/by_ref/union` counts emitted per file so eyeball can confirm zero on first run, non-zero after any Phase 4 destructive insert lands. |
| M1 — pre-write intent emit for Phase 2 wipe | `wipe_hdfc` in orchestrator | `phase2.deleting` event fires BEFORE the DELETE so mid-flight crashes leave a forensic trail. |
| M2 — scope rollback DELETE to 12 fileIDs | Step 8 (this runbook) | `AND fileID IN ($FIDS)` — never unscoped. |
| M3 — scope manual verify to 12 fileIDs | Step 4 (this runbook) | `AND fileID IN ($FIDS)` — false-alarm suppression when Vidish has HDFC_DEBIT outside the 12. |
| M4 — preview edges | subsumed by MAJOR fix | Sign convention + None-opening + zero-value paths already correct; MAJOR filter is the missing piece. |

## Reviewer v3 fix summary (post hq-wisp-9d9bxk)

| Fix | Location | Note |
|-----|----------|------|
| NEW MAJOR — NULL-tag exclusion silently zeroed sums | `phase4_preview._exclude` (orchestrator) | v2's `tag != 'OPENING_BALANCE'` failed on NULL tags (SQL three-valued logic: `NULL != 'X'` → NULL → row EXCLUDED). Since `Transactions.tag` is NULLABLE and normal rows land tag=NULL, every normal row was silently dropped from debit_sum/credit_sum → reconstructed_closing ≈ stated_opening → both deltas garbage. Reviewer take (a): DROPPED the tag clause; referenceID PK-prefix filter (`~referenceID.like('OPENING_BALANCE_%')`) alone is null-safe (referenceID is the non-null PK) AND sufficient since Phase 4's synthetic insert uses `referenceID='OPENING_BALANCE_<fileID>'`. |
| Behavioral guard — NULL-tag regression lock | `phase4.sanity` emit (orchestrator) | Every file emits filtered_debit_sum + unfiltered_debit_sum + excluded_debit_magnitude (and credit-side). Invariant: `filtered + excluded == unfiltered` per side (tolerance 0.5 paise). If it fails: loud `phase4.error` — sums UNTRUSTWORTHY, do NOT surface to Overseer. Locks the v2 NULL-logic bug shape from recurring. |

## Reviewer v4 fix summary (post hq-wisp-1t049i)

| Fix | Location | Note |
|-----|----------|------|
| BUG — --dry-run silently skipped phase4_preview | `main()` at previews line | Inverted ternary `previews = [] if args.dry_run else phase4_preview(...)` meant --dry-run never called the preview. Runbook + --help both promised preview under --dry-run; code did the opposite. Fix: unconditional call. Destructive INSERT path stays gated on `_PHASE4_AWAITING_Q2_GO` independent of the ternary. |

## v5 fix summary (post hq-wisp-cpkbaf — Option A per Lead's GO)

Diagnosis confirmed the ak-ifc backstop in mailProcessorService.py:1975-1976 uses the SAME `parse_hdfc_savings_summary` parser as phase4_preview and has been silent-nooping on these 12 files for months — a latent observability defect in the live ingest pipeline surfaced by ak-32o. Parser fix scoped to follow-up bead **ak-19d** (P2, Lead-filed). This v5 delivers observability + decrypt wire; NO parser changes.

| Fix | Location | Note |
|-----|----------|------|
| (2) Encrypted-PDF decrypt wire | `phase4_preview` (orchestrator) | Reuses `app.mailProcessor._get_statement_password(user_id, email_dict, pdf_path)` + `doc.authenticate(password)` — in-memory only, never mutates the persisted PDF. Enables preview to parse encrypted files like #10 (19c248db3a7b7d55). |
| (3a) Per-file skip emit | `phase4_preview` on every continue path | Emits `phase4.skip {file_id, gmail_id, reason, note}` for each of: `pdf_unresolved` / `pdf_read_fail` / `decrypt_failed` / `summary_unparseable`. Operators see per-file coverage in the stream — no more silent-drop. |
| (3b) Aggregate summary emit | End of `phase4_preview` | Emits `phase4.summary {files_seen, parsed, skipped, skip_reasons, trustworthy_for_q2}` — one look tells the operator/Lead/Overseer whether Q2 preview data is trustworthy. `trustworthy_for_q2` is `parsed > 0`. |
| (4) Sanity emit already scoped to parsed files | `phase4.sanity` (unchanged) | Structure already places the sanity emit AFTER the `summary is None → continue` skip, so it only fires per parsed file. No false-negative on all-skipped runs. Verified by code inspection. |
| Docstring — cite ak-19d | `phase4_preview` header | Marks parser expansion as separately-scoped so future readers know the SKIP path is intentional + tracked. |

---

## Copy-paste blocks (infra fires each, pastes stdout back verbatim)

Every block emits one-line JSON events prefixed `ak32o|`. Backend parses those into the
`[STATUS ak-32o]` shape for Lead.

### Working directory + env

```bash
cd /home/opc/data/Desktop/Akkountant/Akkountant-v2   # (or wherever prod app is deployed)
export USER_ID='<Vidish-user-id>'                    # infra fills in
```

### Step 0 — Confirm branch state (10s)

```bash
git log --oneline -3
# Expected: 7d35075 (ak-2r8) or newer on Personal, or the agent/ak32o-mp-fire branch
# tip that Lead handed off.
```

**Success signal**: agent/ak32o-mp-fire branch head, or Personal with ak-32o scaffold cherry-picked.

### Step 1 — Pause scheduler (avoid concurrent HDFC writes)

```bash
# Stop the background scheduler so live CheckStatement / reprocess_pdf runs
# don't race the wipe. Method depends on deploy env — one of:
sudo systemctl stop akkountant-scheduler.service   # if systemd-managed
# OR
kill $(pgrep -f 'python.*scheduler')               # if bare process
```

**Success signal**: `ps -ef | grep -i scheduler` shows nothing.

### Step 2 — Enumerate the 12 canonical HDFC files (10s, read-only)

```bash
python3 scripts/ak32o_hdfc_mp_fire.py --user-id "$USER_ID" --phase enumerate
```

**Success signal**: `phase1.done` event with `file_count=12` (SOFT WARN if not — Overseer's
Q3 will decide whether to abort or proceed). Every file emits a `phase1.manifest` event with
resolved PDF path + `pdf_present=true`. Any `phase1.warn` for unresolved paths → PAUSE and
report to Lead; do not proceed to Phase 2.

### Step 3 — Full dry-run (60s, read-only)

```bash
python3 scripts/ak32o_hdfc_mp_fire.py --user-id "$USER_ID" --dry-run
```

**Success signal**: emits `phase0.dry` (live snapshot count), `phase2.dry` (projected DELETE
count), `phase3.dry` (per-file), `phase5.summary`. **Paste this whole block back to backend.**
Backend parses into `[STATUS ak-32o dry-run]` for Lead + Overseer review.

**Halt condition**: any unresolved PDF path (`phase1.warn` with unresolved_ids) or `phase0.error`
→ do NOT run Step 4/5/6. Report to Lead.

### Step 4 — Snapshot (5-30s, WRITES backup table only, no txn changes)

```bash
python3 scripts/ak32o_hdfc_mp_fire.py --user-id "$USER_ID" --phase snapshot
```

**Success signal**: `phase0.done` with `snapshot_count > 0` and `snapshot_count == live_count`.
Backup table name = `transactions_ak32o_reparse_bak_<YYYYMMDD>`. Retry-safe (INSERT IGNORE).

**Verify manually (M3 fix: SCOPED to the 12 fileIDs, not all-HDFC_DEBIT):**

First capture the fileID list from `phase1.manifest` events (Step 2) — paste them
into `$FIDS` as a comma-separated quoted list, e.g. `'fid_a','fid_b',...`:

```bash
FIDS="'<fid-1>','<fid-2>','<fid-3>','<fid-4>','<fid-5>','<fid-6>','<fid-7>','<fid-8>','<fid-9>','<fid-10>','<fid-11>','<fid-12>'"

mysql -e "SELECT COUNT(*) AS bak FROM akkountant.transactions_ak32o_reparse_bak_$(date +%Y%m%d);"
mysql -e "SELECT COUNT(*) AS live_scoped FROM akkountant.transactions
          WHERE user='$USER_ID' AND bank='HDFC_DEBIT' AND fileID IN ($FIDS);"
# Two counts should match. UNSCOPED live count would false-alarm if Vidish has
# HDFC_DEBIT rows outside the 12 (M3 reviewer catch — always scope to fileIDs).
```

### Step 5 — Wipe (5-15s, DESTRUCTIVE — REQUIRES --assume-yes)

**Get Lead + Overseer greenlight based on Step 3 dry-run output BEFORE this step.**

```bash
python3 scripts/ak32o_hdfc_mp_fire.py --user-id "$USER_ID" --phase wipe --assume-yes
```

**Success signal**: `phase2.done` with `deleted == expected`. If `phase2.error` fires with
`snapshot drift` → re-run Step 4 to refresh snapshot, then retry Step 5. Any other error →
STOP + report + run rollback (Step 8).

### Step 6 — Chronological re-invoke (10-40 min depending on file sizes)

```bash
python3 scripts/ak32o_hdfc_mp_fire.py --user-id "$USER_ID" --phase reinvoke --assume-yes
```

**Success signal**: `phase3.start` → `phase3.done` for each of the 12 files (oldest first).
`phase3.done.inserted > 0` per file. Emits `phase4.preview` for each file with
stated_opening / stated_closing / reconstructed_closing / delta_per_file / delta_inter_file.
Final `phase5.summary` with fleet totals.

**Halt condition**: `phase3.error` on ANY file → stop, capture the error, report to Lead. Do
NOT auto-continue to next file. The wipe is complete but re-invoke is partial; rollback is
still clean (Step 8 restores from Step 4's snapshot).

### Step 7 — Verify (30s, read-only)

Manual sanity checks against the summary:

```bash
mysql -e "SELECT COUNT(*) AS live FROM akkountant.transactions
          WHERE user='$USER_ID' AND bank='HDFC_DEBIT';"
mysql -e "SELECT fileID, COUNT(*) AS row_count
          FROM akkountant.transactions
          WHERE user='$USER_ID' AND bank='HDFC_DEBIT'
          GROUP BY fileID ORDER BY MIN(date) ASC;"
```

Compare to the `phase5.summary.total_transactions_inserted`. Compare per-file counts to
`phase3.done.inserted`. Compare **live totals** to **snapshot totals** (Step 4's backup) —
delta = net change from re-parse.

### Step 8 — Rollback (only if Step 5, 6, or 7 goes bad)

**M2 fix: rollback DELETE is SCOPED to the 12 fileIDs.** Same scope as the wipe
(Step 5). An unscoped DELETE would nuke any HDFC_DEBIT rows Vidish has outside
the 12 (contains bug if there are any) before the INSERT SELECT restores from
snapshot.

Reuse the `$FIDS` list captured in Step 4's manual verify:

```bash
BACKUP=transactions_ak32o_reparse_bak_$(date +%Y%m%d)   # or your snapshot's suffix
# $FIDS from Step 4 (comma-separated quoted fileIDs)

mysql -e "
  START TRANSACTION;
  DELETE FROM akkountant.transactions
   WHERE user='$USER_ID' AND bank='HDFC_DEBIT' AND fileID IN ($FIDS);
  INSERT INTO akkountant.transactions
    SELECT * FROM akkountant.$BACKUP;
  COMMIT;
  SELECT COUNT(*) AS restored FROM akkountant.transactions
    WHERE user='$USER_ID' AND bank='HDFC_DEBIT' AND fileID IN ($FIDS);
"
```

**Success signal**: `restored` count matches the pre-wipe live count from Step 4.

### Step 9 — Resume scheduler

```bash
sudo systemctl start akkountant-scheduler.service   # or restart the bare process
```

**Success signal**: `ps -ef | grep -i scheduler` shows the process again; jobs table shows
CheckMail / CheckStatement moving through statuses at the next tick.

---

## HELD: Phase 4 (OPENING_BALANCE synthetic insert) — awaiting Overseer Q2

Phase 4 is scaffolded but **refuses to run** until Overseer picks per-file (a) vs
inter-file (b) semantics for the delta. The Step 6 output includes `phase4.preview` per file
with BOTH deltas so Overseer can pick from real numbers.

When Overseer answers, backend updates the `_PHASE4_AWAITING_Q2_GO` guard in the orchestrator
+ implements the chosen insert path + reviewer + mayor + you re-fire Step 6 with
`--enable-phase4-per-file` OR `--enable-phase4-inter-file`.

## HELD: Phase 1 hard-assert count==12 — awaiting Overseer Q3

Phase 1 currently emits a SOFT `phase1.warn` if the fileDetails query returns != 12 files.
When Overseer answers Q3, backend updates the expected count or the pinned-fileID list.

## Rollback readiness at every step

| After step | State | Rollback |
|-----------|-------|----------|
| Step 4    | Snapshot exists, no txn changes | Drop backup table if unwanted: `DROP TABLE akkountant.transactions_ak32o_reparse_bak_<yyyymmdd>;` |
| Step 5    | Wipe done, no re-invoke | Step 8 restores snapshot cleanly |
| Step 6    | Wipe + partial/full re-invoke | Step 8 restores snapshot cleanly (re-parsed rows get deleted, snapshot rows restored) |
| Step 7    | Verify caught divergence | Step 8 restores snapshot cleanly |

The snapshot table is the source of truth for rollback. Do NOT delete it until Overseer
signs off on the run.

## Report-back format for Lead

Backend parses infra's pasted-back stdout into `[STATUS ak-32o]`:

```
[STATUS ak-32o] v3 mailProcessor re-parse
  step: <enumerate|dry-run|snapshot|wipe|reinvoke|verify|rollback|resume>
  files_enumerated: N (expected 12)
  snapshot_count: N
  wipe_deleted: N
  reinvoke_ok: N/12  errors: N  skipped: N
  txns_inserted: N   snapshot_delta: +/- N
  phase4_preview: <count of files with delta_per_file != None>
  status: <OK|WARN|ERROR>
  notes: <...>
```
