"""ak-ep4i (2026-11-24): user annotations on bank transactions, for the
transaction drawer (ak-0dgh): category (Keep / Change…), an optional
merchant rule, and a free-text note.

Contract (agreed with frontend, see bead ak-ep4i):
  PUT /transactions/<ref>/category  {category: str|null, rule?: {pattern, apply_to_existing}}
  PUT /transactions/<ref>/note      {note: str|null}
  GET /transactions/categories
Errors are AnnotationError(code, message, status) -> {"error", "code"}.

Storage (no ALTER needed):
  * category      -> transactions.tag ("" = uncategorised)
  * merchant rule -> savedTags (details = pattern, tag = category)
  * note          -> transactionNotes (models/transactionNote.py)

Merchant rules: a case-insensitive SUBSTRING of the bank memo
(transactions.details). At import, an UNTAGGED row whose memo contains a
rule's pattern gets that rule's category; the longest matching pattern
wins. Rules only ever set ``tag``; amounts, dates and ids are never
touched. Everything is scoped to the caller's uid.

Leaf module: functions take a SQLAlchemy session so they are testable
on in-memory SQLite without the service boot chain. Memo / note text is
personal financial data and is never logged here.
"""
from __future__ import annotations

import re

from sqlalchemy import func, or_

CATEGORY_MAX_LEN = 100
PATTERN_MIN_LEN = 3
PATTERN_MAX_LEN = 100
NOTE_MAX_LEN = 2000
_REF_RE = re.compile(r"^[A-Za-z0-9._:-]{1,64}$")


class AnnotationError(Exception):
    def __init__(self, code, message, status=400):
        super().__init__(message)
        self.code = code
        self.message = message
        self.status = status

    def to_response(self):
        return {"error": self.message, "code": self.code}, self.status


# ── validation ──────────────────────────────────────────────────────

def validate_reference(reference_id):
    if not isinstance(reference_id, str) or not _REF_RE.match(reference_id) \
            or reference_id.endswith("\n"):
        raise AnnotationError("invalid_reference", "Invalid transaction reference")
    return reference_id


def normalize_category(category):
    """None / "" / whitespace -> None (clear). Else trimmed 1..100 chars."""
    if category is None:
        return None
    if not isinstance(category, str):
        raise AnnotationError("invalid_category", "category must be a string or null")
    category = category.strip()
    if not category:
        return None
    if len(category) > CATEGORY_MAX_LEN:
        raise AnnotationError(
            "invalid_category", f"category must be at most {CATEGORY_MAX_LEN} characters",
        )
    return category


def normalize_rule(rule, category, details):
    """Returns None (no rule) or (pattern, apply_to_existing)."""
    if rule is None:
        return None
    if not isinstance(rule, dict):
        raise AnnotationError("invalid_rule", "rule must be an object")
    if category is None:
        raise AnnotationError("invalid_rule", "a rule needs a category (cannot clear with a rule)")
    pattern = rule.get("pattern")
    if not isinstance(pattern, str):
        raise AnnotationError("invalid_rule", "rule.pattern must be a string")
    pattern = pattern.strip()
    if not (PATTERN_MIN_LEN <= len(pattern) <= PATTERN_MAX_LEN):
        raise AnnotationError(
            "invalid_rule",
            f"rule.pattern must be {PATTERN_MIN_LEN}-{PATTERN_MAX_LEN} characters",
        )
    if pattern.lower() not in (details or "").lower():
        raise AnnotationError("invalid_rule", "rule.pattern must appear in this transaction's memo")
    apply_existing = rule.get("apply_to_existing", False)
    if apply_existing is None:
        apply_existing = False
    if not isinstance(apply_existing, bool):
        raise AnnotationError("invalid_rule", "rule.apply_to_existing must be a boolean")
    return pattern, apply_existing


def normalize_note(note):
    """None / "" / whitespace -> None (delete). Else trimmed, max 2000."""
    if note is None:
        return None
    if not isinstance(note, str):
        raise AnnotationError("invalid_note", "note must be a string or null")
    note = note.strip()
    if not note:
        return None
    if len(note) > NOTE_MAX_LEN:
        raise AnnotationError("note_too_long", f"note must be at most {NOTE_MAX_LEN} characters")
    return note


# ── rules (pure + loader) ───────────────────────────────────────────

def rule_tag_for(rules, details):
    """Category of the longest rule pattern contained (case-insensitive)
    in ``details``, or None. ``rules`` = iterable of (pattern, tag)."""
    memo = (details or "").lower()
    best = None
    for pattern, tag in rules:
        if not pattern or not tag:
            continue
        if pattern.lower() in memo and (best is None or len(pattern) > len(best[0])):
            best = (pattern, tag)
    return best[1] if best else None


def load_tag_rules(session, user_id):
    from models.savedTags import SavedTags
    rows = session.query(SavedTags.details, SavedTags.tag).filter(
        SavedTags.user == user_id,
    ).all()
    return [(r[0], r[1]) for r in rows]


# ── operations ──────────────────────────────────────────────────────

def _own_transaction(session, user_id, reference_id):
    from models.transactions import Transactions
    txn = session.query(Transactions).filter(
        Transactions.referenceID == reference_id,
        Transactions.user == user_id,
    ).first()
    if txn is None:
        raise AnnotationError("not_found", "Transaction not found", 404)
    return txn


def _require_user(user_id):
    if not user_id:
        raise AnnotationError("unauthorized", "User not identified", 401)


def set_category(session, user_id, reference_id, category, rule=None):
    from models.savedTags import SavedTags
    from models.transactions import Transactions

    _require_user(user_id)
    validate_reference(reference_id)
    category = normalize_category(category)
    txn = _own_transaction(session, user_id, reference_id)
    parsed_rule = normalize_rule(rule, category, txn.details)

    try:
        txn.tag = category or ""
        rule_out = None
        if parsed_rule:
            pattern, apply_existing = parsed_rule
            existing = session.query(SavedTags).filter(
                SavedTags.user == user_id,
                func.lower(SavedTags.details) == pattern.lower(),
            ).first()
            if existing is None:
                existing = SavedTags(user=user_id, details=pattern, tag=category)
                session.add(existing)
            else:
                existing.details = pattern
                existing.tag = category
            session.flush()
            applied = 0
            if apply_existing:
                applied = session.query(Transactions).filter(
                    Transactions.user == user_id,
                    or_(Transactions.tag == "", Transactions.tag.is_(None)),
                    func.lower(Transactions.details).contains(
                        pattern.lower(), autoescape=True,
                    ),
                ).update({Transactions.tag: category}, synchronize_session=False)
            rule_out = {
                "id": existing.id, "pattern": pattern, "category": category,
                "applied_to_existing": int(applied or 0),
            }
        session.commit()
    except Exception:
        session.rollback()
        raise AnnotationError("save_failed", "Could not save the category", 500)

    return {"reference_id": reference_id, "category": category, "rule": rule_out}


def set_note(session, user_id, reference_id, note):
    from models.transactionNote import TransactionNote

    _require_user(user_id)
    validate_reference(reference_id)
    note = normalize_note(note)
    _own_transaction(session, user_id, reference_id)
    try:
        row = session.query(TransactionNote).filter(
            TransactionNote.user == user_id,
            TransactionNote.referenceID == reference_id,
        ).first()
        if note is None:
            if row is not None:
                session.delete(row)
        elif row is None:
            session.add(TransactionNote(user=user_id, referenceID=reference_id, note=note))
        else:
            row.note = note
        session.commit()
    except Exception:
        session.rollback()
        raise AnnotationError("save_failed", "Could not save the note", 500)
    return {"reference_id": reference_id, "note": note}


def list_categories(session, user_id):
    from models.savedTags import SavedTags
    from models.transactions import Transactions

    _require_user(user_id)
    used = session.query(Transactions.tag).filter(
        Transactions.user == user_id,
        Transactions.tag.isnot(None),
        Transactions.tag != "",
    ).distinct().all()
    ruled = session.query(SavedTags.tag).filter(SavedTags.user == user_id).distinct().all()
    names = {(r[0] or "").strip() for r in list(used) + list(ruled)}
    names.discard("")
    return {"categories": sorted(names, key=lambda s: (s.lower(), s))}


def notes_for(session, user_id, reference_ids):
    """{referenceID: note} for the given ids (only the user's own)."""
    from models.transactionNote import TransactionNote

    ids = [r for r in set(reference_ids or []) if r]
    if not user_id or not ids:
        return {}
    rows = session.query(TransactionNote.referenceID, TransactionNote.note).filter(
        TransactionNote.user == user_id,
        TransactionNote.referenceID.in_(ids),
    ).all()
    return {r[0]: r[1] for r in rows}
