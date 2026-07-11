"""ak-8l5 review MAJOR: code-level zero-loss insert backstop.

Extracted as a framework-free module so the logic is unit-testable
without the flask/SQLAlchemy import chain that transactionsService
pulls in. The service layer calls into these helpers on IntegrityError
paths.

The design (verbatim from the reviewer proposal that Lead accepted):

  At INSERT time:
    1. Compute referenceID via ref-aware scheme (existing code).
    2. Attempt to insert.
    3. On IntegrityError (PK collision), query for existing row with
       the same referenceID.
    4. IF (existing.date, existing.amount, normalized(existing.desc))
       DIFFERS from the incoming row → suffix incoming.referenceID
       with a disambiguator ("-dupN") and retry insert. Neither row
       is lost.
    5. IF fields MATCH → silent drop (intended chunk-overlap dedup
       path — the whole reason the ref-based dedup was added).

Overseer's hard requirement: "no transactions whatsoever lost". LLM
extraction imperfection can't be guaranteed at the ref layer alone;
this is the storage-layer safety net.
"""

from __future__ import annotations

from typing import Callable, Optional


# ── Comparison helper ────────────────────────────────────────────────


def rows_content_equal(existing, incoming, normalize_desc: Callable[[str], str]) -> bool:
    """Return True iff (date, amount, normalized-description) match
    between the `existing` DB row and the `incoming` row about to be
    inserted.

    Both `existing` and `incoming` must expose `.date`, `.amount`,
    `.details` attributes (Transactions ORM shape).

    Rules:
      - date: prefer direct equality; fall back to str() equality so
        subclass mismatches (e.g. datetime vs date object) still
        compare.
      - amount: normalized to 2dp (float noise tolerance).
      - description: normalized via the passed `normalize_desc`
        callable — pass utils.reference_id.normalize_description_for_backstop
        so the same shape used by the fallback hash is applied here.
    """
    # Date compare with defensive str-fallback for cross-type quirks.
    if existing.date != incoming.date:
        try:
            if str(existing.date) != str(incoming.date):
                return False
        except Exception:
            return False

    try:
        e_amt = round(float(existing.amount or 0), 2)
        i_amt = round(float(incoming.amount or 0), 2)
    except (TypeError, ValueError):
        return False
    if e_amt != i_amt:
        return False

    if normalize_desc(existing.details) != normalize_desc(incoming.details):
        return False

    return True


# ── Disambiguation helper ────────────────────────────────────────────


def find_disambiguated_ref(
    session,
    model_class,
    base_ref: str,
    *,
    max_attempts: int = 999,
    ref_column_name: str = "referenceID",
) -> str:
    """Find an unused suffixed ref based on `base_ref`.

    Suffix scheme: `<truncated_base>-dupN` where N counts from 1
    upward, and `<truncated_base>` is `base_ref` sliced to keep the
    total candidate length ≤ 64 chars (Transactions.referenceID's
    VARCHAR(64) width).

    Raises RuntimeError if `max_attempts` is exhausted — that's a
    system-level anomaly (>999 distinct colliding rows on the same
    ref).
    """
    for i in range(1, max_attempts + 1):
        suffix = f"-dup{i}"
        truncated = base_ref[:64 - len(suffix)]
        candidate = f"{truncated}{suffix}"

        # Cheap PK-only existence check.
        try:
            filter_kwargs = {ref_column_name: candidate}
            row = session.query(model_class).filter_by(**filter_kwargs).first()
        except Exception as err:
            raise RuntimeError(
                f"disambiguator lookup failed at attempt {i}: {err}"
            )
        if not row:
            return candidate

    raise RuntimeError(
        f"exhausted {max_attempts} disambiguation attempts for "
        f"base_ref={base_ref}"
    )


# ── Orchestration ────────────────────────────────────────────────────


def apply_backstop_on_collision(
    *,
    session,
    model_class,
    incoming,
    normalize_desc: Callable[[str], str],
    logger,
    add_fn: Optional[Callable] = None,
    commit_fn: Optional[Callable] = None,
    rollback_fn: Optional[Callable] = None,
) -> str:
    """Handle a PK collision that just occurred on `incoming`.

    Assumes the caller has already:
      - Attempted the insert.
      - Rolled back the failed insert.

    This function then decides between the "chunk-overlap silent drop"
    path and the "differing-content suffix + retry" path.

    Returns one of:
      "dropped_matching"  — existing row had matching content; the
                            silent-drop path (chunk-overlap dedup).
      "disambiguated"     — suffixed the incoming ref and inserted.
                            `incoming.referenceID` is mutated in place.
      "dropped_missing"   — the collision was reported by the DB but
                            the existing row can't be found (race
                            with concurrent insert; extremely rare).
      "retry_failed"      — the suffixed insert itself failed — log
                            already emitted; caller should count this
                            as an integrity error.

    `add_fn`, `commit_fn`, `rollback_fn` default to the session's own
    methods; the parameters exist so the service layer can adapt for
    the `isinstance(db, dict)` code path.
    """
    if add_fn is None:
        add_fn = session.add
    if commit_fn is None:
        commit_fn = session.commit
    if rollback_fn is None:
        rollback_fn = session.rollback

    try:
        existing = session.query(model_class).filter_by(
            referenceID=incoming.referenceID
        ).first()
    except Exception as query_err:  # pragma: no cover — defensive
        logger.error(
            f"ak-8l5 backstop: existing-row query failed for "
            f"ref={incoming.referenceID}: {query_err}"
        )
        rollback_fn()
        return "dropped_missing"

    if existing is None:
        logger.debug(
            f"Skipping duplicate transaction: {incoming.referenceID}"
        )
        return "dropped_missing"

    if rows_content_equal(existing, incoming, normalize_desc):
        # Chunk-overlap dedup — intended silent drop.
        logger.debug(
            f"Skipping duplicate transaction: {incoming.referenceID}"
        )
        return "dropped_matching"

    # DIFFERING content → suffix + retry (the zero-loss guarantee).
    base_ref = incoming.referenceID
    try:
        disambiguated_ref = find_disambiguated_ref(
            session, model_class, base_ref,
        )
    except RuntimeError as e:
        logger.error(
            f"ak-8l5 backstop: could not disambiguate ref {base_ref}: "
            f"{e}. Dropping row to avoid crash."
        )
        return "retry_failed"

    logger.warning(
        "ak-8l5 backstop: PK collision with DIFFERING content; "
        "suffixing to preserve tx. bank=%s user=%s "
        "orig_ref=%s -> new_ref=%s "
        "existing=(date=%s,amount=%s,desc=%.80s) "
        "incoming=(date=%s,amount=%s,desc=%.80s)",
        getattr(incoming, "bank", "?"),
        getattr(incoming, "user", "?"),
        base_ref, disambiguated_ref,
        existing.date, existing.amount,
        (existing.details or "")[:80],
        incoming.date, incoming.amount,
        (incoming.details or "")[:80],
    )
    incoming.referenceID = disambiguated_ref

    try:
        add_fn(incoming)
        commit_fn()
        return "disambiguated"
    except Exception as retry_err:
        logger.error(
            f"ak-8l5 backstop: suffixed insert ALSO failed "
            f"({disambiguated_ref}): {retry_err}"
        )
        rollback_fn()
        return "retry_failed"
