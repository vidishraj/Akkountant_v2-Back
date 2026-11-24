"""ak-ep4i (2026-11-24): per-transaction free-text note (drawer "Note").

A separate table (not a ``transactions.note`` column) on purpose: a new
column on ``transactions`` needs a manual ALTER, and until it runs every
ORM query on transactions would fail. A new table is created by
``db.create_all()`` on next boot, no migration needed.

* PK (user, referenceID): one note per transaction, scoped to the user.
* No FK to ``transactions``: re-imports delete and re-insert rows with
  the same referenceID, and the note should survive that rather than
  cascade away. A note on a referenceID that no longer exists is inert.
* The note is an annotation only; the transaction's amount/date never
  change.
"""
from sqlalchemy import Column, DateTime, ForeignKey, String
from sqlalchemy.sql import func

from models.Base import Base

NOTE_MAX_LEN = 2000


class TransactionNote(Base):
    __tablename__ = "transactionNotes"

    user = Column(
        String(100), ForeignKey("users.userID", ondelete="CASCADE"),
        primary_key=True,
    )
    referenceID = Column(String(64), primary_key=True)
    note = Column(String(NOTE_MAX_LEN), nullable=False)
    updated_at = Column(
        DateTime, nullable=False, server_default=func.now(), onupdate=func.now(),
    )
