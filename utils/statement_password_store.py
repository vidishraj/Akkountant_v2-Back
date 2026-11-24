"""ak-3rwh: the storage seam for statement-PDF passwords.

The /statementPasswords endpoints read and write ONLY through this
module, so the at-rest encryption swap (ak-3rwh option A: Fernet +
AK_STATEMENT_PW_KEY + backfill, held for the Overseer) changes this file
and nothing in the endpoints.

Storage today: `statementPasswords.password_hash`. Despite the name it
holds the PLAINTEXT password (PDF unlock needs the raw value), exactly as
the legacy `setOptedBanks` writes it and `_get_statement_password` reads
it. This module does not change that.

Rules:
  * No function returns a stored password. Status queries select the
    bank column only, so the value is never loaded.
  * Never log or format a password (or its length) anywhere here.
"""
from __future__ import annotations

from models import StatementPasswords

MAX_PASSWORD_LEN = 128


class InvalidStatementPassword(ValueError):
    """The submitted password fails the shape check. The message is
    fixed and never contains the value."""


def validate_password(password) -> str:
    """Return the password unchanged (no trim: stored exactly as typed)
    or raise InvalidStatementPassword."""
    if not isinstance(password, str) or not (1 <= len(password) <= MAX_PASSWORD_LEN):
        raise InvalidStatementPassword(
            f"password must be a string of 1-{MAX_PASSWORD_LEN} characters"
        )
    if "\x00" in password:
        raise InvalidStatementPassword("password contains an invalid character")
    return password


def banks_with_password(session, user_id) -> set:
    """Banks that have a non-empty stored password for this user. Loads
    the bank column only."""
    rows = (
        session.query(StatementPasswords.bank)
        .filter(StatementPasswords.user == user_id)
        .filter(StatementPasswords.password_hash.isnot(None))
        .filter(StatementPasswords.password_hash != "")
        .all()
    )
    return {r[0] for r in rows}


def set_password(session, user_id, bank, password) -> None:
    """Create or replace the stored password for (user, bank) and commit.
    Raises InvalidStatementPassword on a bad value; on a DB error rolls
    back and re-raises (callers must not str() it: SQLAlchemy errors
    carry the bound params)."""
    password = validate_password(password)
    try:
        row = session.query(StatementPasswords).filter_by(user=user_id, bank=bank).first()
        if row is None:
            session.add(StatementPasswords(bank=bank, password_hash=password, user=user_id))
        else:
            row.password_hash = password
        session.commit()
    except Exception:
        session.rollback()
        raise


__all__ = [
    "MAX_PASSWORD_LEN",
    "InvalidStatementPassword",
    "validate_password",
    "banks_with_password",
    "set_password",
]
