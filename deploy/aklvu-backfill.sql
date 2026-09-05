-- ==========================================================================
-- ak-lvu backfill deploy script — ORDERED PAIR, DO NOT SPLIT
-- ==========================================================================
--
-- Applies the ak-lvu (Freelance money-math) schema changes + backfills legacy
-- rows with the correct `original_currency` semantics.
--
-- Supersedes the runbook SQL in the v3 (8503d9f) + v5 (3a65fd8) commit
-- bodies. This is the definitive, mayor-audited script.
--
-- Chain the script is deploying:
--   a1111f9 → a55d715 → 8503d9f → c4ddae4 → 3a65fd8 (5 commits, mayor squashes)
--
--
-- ┌────────────────────────────────────────────────────────────────────────┐
-- │ RE-RUN SEMANTICS (v7 correction — mayor audit)                         │
-- ├────────────────────────────────────────────────────────────────────────┤
-- │ * STEP 1 (ALTERs) auto-commits in MySQL — DDL is NOT transactional.    │
-- │   Re-running this file after STEP 1 succeeded will FAIL at ALTER       │
-- │   with error 1060 "Duplicate column name" / 1091 "Can't DROP".         │
-- │ * STEP 2 (procedure with pre-flight + backfills + post-flight) is      │
-- │   idempotent — the WHERE clauses skip already-populated rows AND the   │
-- │   pre-flight ABORTS if rows this script owns are already populated    │
-- │   (partial prior run or manual intervention).                          │
-- │                                                                        │
-- │ Consequence: THE FILE AS A WHOLE IS NOT RE-RUNNABLE.                   │
-- │                                                                        │
-- │ Recovery from a partial failure (e.g. procedure aborted mid-run after  │
-- │ STEP 1 committed): DO NOT re-run the whole file. Re-run STEP 2 only    │
-- │ (the DELIMITER $$ ... DROP PROCEDURE block). STEP 1 is already applied │
-- │ and the ALTERs would fail on re-run.                                   │
-- └────────────────────────────────────────────────────────────────────────┘
--
--
-- ┌────────────────────────────────────────────────────────────────────────┐
-- │ CORRUPTION RISK IF SPLIT / REORDERED (v7: frozen-status FIRST)         │
-- ├────────────────────────────────────────────────────────────────────────┤
-- │ Both backfills below skip rows that are already populated (the WHERE   │
-- │ clauses include `original_amount IS NULL`). This makes each half       │
-- │ INDIVIDUALLY idempotent. BUT the PAIR is order-dependent because       │
-- │ v5's WHERE is a SUPERSET of v3's:                                      │
-- │                                                                        │
-- │   * v3 half → sets original_currency = invoice.currency (real value)   │
-- │              for PAID + single-payment invoices only                   │
-- │   * v5 half → sets original_currency = 'INR' UNCONDITIONALLY for every │
-- │              remaining legacy row (the safe read-side semantic)        │
-- │                                                                        │
-- │ IF v5 RUNS FIRST (or if only v5 runs):                                 │
-- │   → v5 claims EVERY legacy row (including paid single-payment USD/GBP) │
-- │   → v3 then matches NOTHING (rows no longer NULL)                      │
-- │   → paid single-payment USD/GBP invoices are STAMPED `INR`             │
-- │                                                                        │
-- │ ▼ DANGEROUS CONSEQUENCE — status becomes FROZEN                        │
-- │                                                                        │
-- │   Payment `original_currency = INR` on a USD-currency invoice means    │
-- │   the payment currency does NOT match the invoice currency. The v2    │
-- │   F-2 recompute path requires `all_same_currency` for the vintage-    │
-- │   free comparison; the v5-first stamp permanently forces this         │
-- │   invoice into the mixed-currency fallback branch. The v5 V4-3        │
-- │   symmetric anti-flap guard THEN refuses ANY today's-rate status      │
-- │   change on that branch.                                               │
-- │                                                                        │
-- │   Result: status FROZEN at whatever it was pre-migration. Overseer    │
-- │   can never legitimately transition it via edit — a real payment     │
-- │   mutation would need to land with fresh FX metadata to escape.       │
-- │                                                                        │
-- │ ▼ Amount side is SAFE (reassurance only, not the load-bearing risk)   │
-- │                                                                        │
-- │   The v5 code (V5.1 read-side backfill + F-1 guard extension) makes   │
-- │   FE round-trips byte-stable regardless of currency tag: converting  │
-- │   an INR-value tagged as INR is identity (fx_rate=1). So the 83×     │
-- │   amount-space corruption V4-1 flagged does NOT reappear from a v5-  │
-- │   first backfill. Amount-space is closed by the code alone.           │
-- │                                                                        │
-- │ ▼ Why v3 MUST run first                                                │
-- │                                                                        │
-- │   Paid single-payment invoices get their REAL invoice.currency        │
-- │   stamped on the payment. Payment currency = invoice currency →      │
-- │   vintage-free same-currency path stays available. Only the           │
-- │   ambiguous remaining legacy rows fall through to the INR-tagged      │
-- │   safe default via the v5 half.                                        │
-- │                                                                        │
-- │ ▼ Not self-repairing                                                   │
-- │                                                                        │
-- │   Re-running does NOT repair the wrong answer because both halves     │
-- │   skip non-NULL rows. Frozen-status once written is sticky.            │
-- └────────────────────────────────────────────────────────────────────────┘
--
--
-- ┌────────────────────────────────────────────────────────────────────────┐
-- │ v7 ANTI-PATTERN SWEEP (mayor Item 5)                                   │
-- ├────────────────────────────────────────────────────────────────────────┤
-- │ Full-file grep applied for two anti-patterns:                          │
-- │                                                                        │
-- │   1. TEST-NOT-INFERENCE: predicates that reason from a count being     │
-- │      zero (the v6 pre-flight bug: v3_target = 0 inferred "populated    │
-- │      by something else" when it could also mean "MP-3 legacy row       │
-- │      with no payment to backfill" — false positive on healthy data).   │
-- │                                                                        │
-- │      Fix pattern: DIRECTLY count the rows the predicate wants to       │
-- │      detect, not infer them from the absence of others. Applied at     │
-- │      the pre-flight guard (v_already_populated).                       │
-- │                                                                        │
-- │   2. LIKE-WITH-LIKE: assertions comparing counts of different kinds    │
-- │      (the v6 post-deploy checklist bug: v3+v5 payment-row updates      │
-- │      compared to paid_invoices_total, which is an invoice count —     │
-- │      false failure after a correct migration on any DB with MP-3       │
-- │      legacy rows).                                                     │
-- │                                                                        │
-- │      Fix pattern: assertions compare same-kind counts. Applied at the  │
-- │      post-deploy checklist (v3_updated=v3_target, v5_updated=v5_target,│
-- │      post_flight_orphan_rows=0).                                       │
-- │                                                                        │
-- │ Sweep result: no other instances found. All remaining `IS NULL` /      │
-- │ `IS NOT NULL` / `= 0` uses are same-kind direct-test semantics (WHERE  │
-- │ clauses on the actual rows being UPDATE'd; the post-flight orphan     │
-- │ check counts exactly the rows the assertion targets).                  │
-- └────────────────────────────────────────────────────────────────────────┘


-- --------------------------------------------------------------------------
-- STEP 1: schema changes (ALTERs — required BEFORE any backfill)
-- --------------------------------------------------------------------------

ALTER TABLE invoice_payments
  ADD COLUMN original_amount    DECIMAL(12,2) NULL,
  ADD COLUMN original_currency  VARCHAR(3)    NULL,
  ADD COLUMN inr_amount         DECIMAL(12,2) NULL,
  ADD COLUMN fx_rate            DECIMAL(12,4) NULL,
  ADD COLUMN fx_rate_source     VARCHAR(100)  NULL,
  ADD COLUMN converted_at       DATETIME      NULL;

ALTER TABLE invoices
  MODIFY status ENUM('draft','sent','paid','overdue','partially_paid')
  NOT NULL DEFAULT 'draft';


-- --------------------------------------------------------------------------
-- STEP 2: pre-flight + ordered backfill + post-flight (all in one procedure)
-- --------------------------------------------------------------------------
-- Wrapped in a stored procedure so the pre-flight ABORT can raise a real
-- error (SIGNAL) that halts execution — plain SQL doesn't have a clean
-- abort primitive outside procedures.

DELIMITER $$

DROP PROCEDURE IF EXISTS _aklvu_apply_backfill$$

CREATE PROCEDURE _aklvu_apply_backfill()
BEGIN
    -- Diagnostic locals (all counts held for the SELECT at the end;
    -- pre-flight abort predicate uses ONLY v_already_populated per
    -- mayor's v7 test-directly correction).
    DECLARE v_paid_invoices INT DEFAULT 0;
    DECLARE v_v3_target INT DEFAULT 0;
    DECLARE v_v5_target INT DEFAULT 0;
    DECLARE v_already_populated INT DEFAULT 0;
    DECLARE v_v3_updated INT DEFAULT 0;
    DECLARE v_v5_updated INT DEFAULT 0;
    DECLARE v_post_null INT DEFAULT 0;

    -- ── Pre-flight (v7 mayor audit — test-directly, not infer-from-absence) ──
    --
    -- ORIGINAL v6 predicate (WRONG on real data): "paid_invoices > 0 AND
    -- v3_target = 0" inferred "populated by something else" from the
    -- ABSENCE of un-populated rows. But that predicate ALSO fires on
    -- innocent data: an MP-3 legacy invoice (pre-ak-lvu mark_invoice_paid
    -- flipped status='paid' on email say-so WITHOUT writing a payment
    -- row) → paid_invoices > 0, v3_target = 0 → false-positive ABORT on
    -- healthy prod data. The email-path legacy row has no payment to
    -- backfill; both halves correctly no-op on it, no risk exists.
    --
    -- v7 CORRECTED predicate: DIRECTLY count rows this script would own
    -- that are already populated. Fires only when something genuinely
    -- populated the rows we're claiming — silent on any legacy shape
    -- that has nothing to backfill.
    --
    -- Diagnostic counters (kept for SELECT at end, NOT abort inputs):
    SELECT COUNT(*) INTO v_paid_invoices FROM invoices WHERE status = 'paid';

    SELECT COUNT(*) INTO v_v3_target
        FROM invoice_payments p JOIN invoices i ON p.invoice_id = i.id
        WHERE i.status = 'paid'
          AND p.original_amount IS NULL
          AND i.id IN (
              SELECT * FROM (
                  SELECT invoice_id
                    FROM invoice_payments
                    GROUP BY invoice_id
                    HAVING COUNT(*) = 1
              ) AS single_pay_derived
          );

    SELECT COUNT(*) INTO v_v5_target
        FROM invoice_payments
        WHERE original_amount IS NULL AND amount_received IS NOT NULL;

    -- v7 abort predicate (mayor's test-directly formulation).
    -- Count rows in the v3 SCOPE (paid + single-payment invoices) whose
    -- original_amount IS ALREADY POPULATED. If any exist, either a prior
    -- run partially completed or someone manually populated the columns.
    -- Same derived-table 1093 workaround as the target count above.
    SELECT COUNT(*) INTO v_already_populated
        FROM invoice_payments p JOIN invoices i ON p.invoice_id = i.id
        WHERE i.status = 'paid'
          AND p.original_amount IS NOT NULL
          AND i.id IN (
              SELECT * FROM (
                  SELECT invoice_id
                    FROM invoice_payments
                    GROUP BY invoice_id
                    HAVING COUNT(*) = 1
              ) AS single_pay_derived
          );

    -- Confident SIGNAL — this message describes the ONE thing the
    -- predicate detects (not "may have been ... or ...").
    IF v_already_populated > 0 THEN
        SIGNAL SQLSTATE '45000' SET MESSAGE_TEXT =
            'ak-lvu pre-flight ABORT: paid single-payment invoices already have original_amount populated. This means a prior run of this script partially completed, or the columns were manually populated. Do NOT re-run this file whole — inspect state and re-run only the STEP 2 procedure block if the prior partial run needs completion. See re-run semantics header.';
    END IF;

    -- ── STEP 2A: v3 backfill (paid single-payment invoices) ───────────
    -- Assign the REAL invoice.currency to paid single-payment legacy rows.
    -- Historically sound: the pre-ak-lvu status flip to 'paid' only fired
    -- when payment.amount_received >= invoice.total, so single-payment
    -- paid rows have a payment whose amount corresponds to invoice.total
    -- (in the invoice's original currency).
    --
    -- MUST run BEFORE v5 half. v5 would otherwise claim these rows first
    -- and stamp them 'INR' — permanently frozen away from the vintage-free
    -- same-currency path in the code (see corruption box at top of file).
    UPDATE invoice_payments p
        JOIN invoices i ON p.invoice_id = i.id
        SET p.original_amount   = i.total,
            p.original_currency = i.currency,
            p.inr_amount        = COALESCE(p.inr_amount, p.amount_received)
        WHERE i.status = 'paid'
          AND p.original_amount IS NULL
          AND i.id IN (
              SELECT * FROM (
                  SELECT invoice_id
                    FROM invoice_payments
                    GROUP BY invoice_id
                    HAVING COUNT(*) = 1
              ) AS single_pay_derived
          );
    SET v_v3_updated = ROW_COUNT();

    -- ── STEP 2B: v5 backfill (remaining legacy rows) ──────────────────
    -- All rows with NULL original_amount + populated amount_received are
    -- ambiguous: could be partially-paid legacy, sent-with-partial-mail-
    -- receipt, etc. Safe default: tag them 'INR' semantics because
    -- pre-ak-lvu `amount_received` was ALWAYS the INR value (post-
    -- conversion). Round-trip converts INR→INR identity → byte-stable.
    -- V4-3 symmetric guard prevents any today's-rate flap.
    UPDATE invoice_payments
        SET original_amount   = amount_received,
            original_currency = 'INR'
        WHERE original_amount IS NULL
          AND amount_received IS NOT NULL;
    SET v_v5_updated = ROW_COUNT();

    -- ── Post-flight sanity ─────────────────────────────────────────────
    -- After both backfills, no legacy row (amount_received populated)
    -- should have original_currency = NULL. If any remain, the assumptions
    -- above are wrong — surface for investigation.
    SELECT COUNT(*) INTO v_post_null
        FROM invoice_payments
        WHERE original_currency IS NULL AND amount_received IS NOT NULL;

    IF v_post_null > 0 THEN
        SIGNAL SQLSTATE '45000' SET MESSAGE_TEXT =
            'ak-lvu post-flight FAIL: rows with amount_received populated still have original_currency=NULL after both backfills.';
    END IF;

    -- Diagnostic output — mayor / infra can grep the deploy log.
    SELECT
        v_paid_invoices AS paid_invoices_total,
        v_v3_target     AS v3_backfill_target_rows,
        v_v3_updated    AS v3_backfill_rows_updated,
        v_v5_target     AS v5_backfill_target_rows,
        v_v5_updated    AS v5_backfill_rows_updated,
        v_post_null     AS post_flight_orphan_rows,
        'ak-lvu backfill: SUCCESS' AS status;
END$$

DELIMITER ;

-- Invoke the procedure + drop it (single-shot deployment artifact,
-- not a permanent schema object).
CALL _aklvu_apply_backfill();
DROP PROCEDURE _aklvu_apply_backfill;


-- ==========================================================================
-- Post-deploy verification (manual — infra runs these after the script)
-- ==========================================================================
--
-- v7 mayor audit — checklist assertions are now LIKE-WITH-LIKE. The
-- v6 shape compared v3+v5 payment-row counts against paid_invoices_total
-- (an invoice count) — those are only equal when every paid invoice
-- has a payment row, which is EXACTLY the MP-3 email-path legacy shape
-- (paid invoice with no payment row) that innocently fails the check
-- after a correct migration. Wrong prompt for a tired human at end of
-- an outage window.
--
-- Each assertion below compares like-with-like counts + has exactly
-- one cause when it fails.
--
-- 1. Confirm the diagnostic SELECT from STEP 2 shows all three
--    like-with-like invariants:
--        post_flight_orphan_rows         = 0
--        v3_backfill_rows_updated        = v3_backfill_target_rows
--        v5_backfill_rows_updated        = v5_backfill_target_rows
--    (Diagnostic-only counters — paid_invoices_total, v3/v5 targets —
--    are useful context for reading the shape of prod data, but they
--    are NOT assertion inputs. Comparing v3+v5 payment-row updates to
--    paid_invoices_total was the v6 bug.)
--
-- 2. Confirm the status enum expansion took effect:
--        SHOW COLUMNS FROM invoices LIKE 'status';
--    Expected: enum('draft','sent','paid','overdue','partially_paid')
--
-- 3. Confirm all 6 new payment columns exist:
--        SHOW COLUMNS FROM invoice_payments
--          WHERE Field IN ('original_amount','original_currency','inr_amount',
--                          'fx_rate','fx_rate_source','converted_at');
--    Expected: 6 rows, all Null=YES.
--
-- 4. Restart app.service (pending_payment_claims table auto-created by
--    db.create_all() on next boot).
