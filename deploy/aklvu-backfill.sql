-- ==========================================================================
-- ak-lvu backfill deploy script — ORDERED PAIR, DO NOT SPLIT
-- ==========================================================================
--
-- Applies the ak-lvu (Freelance money-math) schema changes + backfills legacy
-- rows with the correct `original_currency` semantics. Run atomically. The
-- backfills BELOW ARE ORDER-DEPENDENT and NON-REPAIRABLE if executed out of
-- order — see rationale under "CORRUPTION RISK IF SPLIT / REORDERED" below.
--
-- Supersedes the runbook SQL in the v3 (8503d9f) + v5 (3a65fd8) commit
-- bodies. This is the definitive, mayor-audited script.
--
-- Chain the script is deploying:
--   a1111f9 → a55d715 → 8503d9f → c4ddae4 → 3a65fd8 (5 commits, mayor squashes)
--
--
-- ┌────────────────────────────────────────────────────────────────────────┐
-- │ CORRUPTION RISK IF SPLIT / REORDERED                                   │
-- ├────────────────────────────────────────────────────────────────────────┤
-- │ Both backfills below skip rows that are already populated (the WHERE   │
-- │ clauses include `original_amount IS NULL`). This makes each half       │
-- │ INDIVIDUALLY idempotent — safe to re-run.                              │
-- │                                                                        │
-- │ BUT the PAIR is order-dependent. v5's WHERE is a SUPERSET of v3's:     │
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
-- │   → next FE touch: `_replace_payment` sees originalCurrency='INR' →    │
-- │     converts 8325 INR-value as INR (identity) — WAIT, actually with    │
-- │     the v5 code (V5.1 read-side backfill) this is now safe on that     │
-- │     side too because both read and write agree on INR semantics.       │
-- │                                                                        │
-- │   HOWEVER: the invoice.currency remains USD/GBP. The `paid_in_currency │
-- │   vs invoice.total` compare in `_recompute_invoice_status` requires    │
-- │   `all_same_currency` — payment.original_currency must equal           │
-- │   invoice.currency for the vintage-free path. If v5-first stamps INR   │
-- │   onto a USD invoice's payment, we lose the vintage-free path and      │
-- │   fall into the mixed-currency branch. The v5 V4-3 symmetric guard     │
-- │   then refuses status changes → status FROZEN at whatever it was       │
-- │   pre-migration → Overseer can never legitimately transition it.       │
-- │                                                                        │
-- │ This is why v3 MUST run first: paid single-payment invoices get their  │
-- │ real invoice.currency stamped on the payment, matching the invoice —   │
-- │ vintage-free path stays available. Only the ambiguous remaining        │
-- │ legacy rows fall through to the INR-tagged safe default.               │
-- │                                                                        │
-- │ Re-running does NOT repair the wrong answer because both halves skip   │
-- │ non-NULL rows.                                                         │
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
    -- Diagnostic locals.
    DECLARE v_paid_invoices INT DEFAULT 0;
    DECLARE v_v3_target INT DEFAULT 0;
    DECLARE v_v5_target INT DEFAULT 0;
    DECLARE v_v3_updated INT DEFAULT 0;
    DECLARE v_v5_updated INT DEFAULT 0;
    DECLARE v_post_null INT DEFAULT 0;

    -- ── Pre-flight ────────────────────────────────────────────────────
    SELECT COUNT(*) INTO v_paid_invoices FROM invoices WHERE status = 'paid';

    -- v3 target: paid + single-payment + still NULL original_amount.
    -- Derived-table wrapper on the inner subquery to sidestep MySQL error
    -- 1093 (ER_UPDATE_TABLE_USED) — "You can't specify target table for
    -- update in FROM clause". Applied here + at the UPDATE below.
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

    -- v5 target: any remaining row with NULL original_amount + populated
    -- amount_received (legacy pre-ak-lvu shape).
    SELECT COUNT(*) INTO v_v5_target
        FROM invoice_payments
        WHERE original_amount IS NULL AND amount_received IS NOT NULL;

    -- Pre-flight abort: if paid invoices exist but the v3 half has ZERO
    -- targets, the original_currency column has been populated by something
    -- OTHER than this script. Running the backfills now risks corruption.
    -- Halt with a clear error so a human can investigate before proceeding.
    IF v_paid_invoices > 0 AND v_v3_target = 0 THEN
        SIGNAL SQLSTATE '45000' SET MESSAGE_TEXT =
            'ak-lvu pre-flight ABORT: paid invoices exist but v3 backfill has no target rows. The original_amount column may have been populated by manual intervention or a partial prior run. Investigate before proceeding.';
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
-- 1. Confirm the diagnostic SELECT above showed sensible counts (v3+v5
--    updated ≥ paid_invoices_total, post_flight_orphan_rows = 0).
-- 2. Confirm the status enum expansion took effect:
--        SHOW COLUMNS FROM invoices LIKE 'status';
--    Expected: enum('draft','sent','paid','overdue','partially_paid')
-- 3. Confirm all 6 new payment columns exist:
--        SHOW COLUMNS FROM invoice_payments
--          WHERE Field IN ('original_amount','original_currency','inr_amount',
--                          'fx_rate','fx_rate_source','converted_at');
--    Expected: 6 rows, all Null=YES.
-- 4. Restart app.service (pending_payment_claims table auto-created by
--    db.create_all() on next boot).
