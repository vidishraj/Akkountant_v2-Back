"""ak-ex2-v2: snapshot-before-delete audit table.

Reviewer flagged (MAJOR) that ak-ex2 v1 hard-DELETEd rows classified
as non-savings with no restore path. Under the Overseer's zero-loss
constraint, any heuristic-driven delete needs a recovery mechanism.

This table is written to BEFORE the strip DELETE fires. The stripper
snapshots each candidate row's full column set (mirroring
`transactions` layout, minus PK / FK constraints so re-strip of the
same file doesn't collide) along with three audit-scope columns:

  - stripped_at       — server-side timestamp for retention sweeps.
  - stripped_reason   — short label; today always
                        'ak-ex2 non-savings'. Future strippers can
                        reuse this table with a different label.
  - stripped_file_id  — the fileID the row was stripped from. Used
                        to scope the recovery query.

Recovery is manual (per Lead's contract in the ak-ex2-v2 BOUNCE):

  INSERT INTO transactions (referenceID, date, details, amount, tag,
                            fileID, source, bank, user, processed_via,
                            gmail_message_id, transfer_group_id,
                            bank_reference_id)
    SELECT referenceID, date, details, amount, tag, fileID, source,
           bank, user, processed_via, gmail_message_id,
           transfer_group_id, bank_reference_id
      FROM stripped_transactions_audit
      WHERE stripped_file_id = ?;
  DELETE FROM stripped_transactions_audit
    WHERE stripped_file_id = ?;

Retention: 30 days by default (see follow-up cleanup bead). No
automatic purge yet — Lead / infra handle for now.

The synthetic `audit_id` auto-increment primary key means the same
(referenceID, fileID) can be snapshotted multiple times without
collision (e.g. if a file is stripped, restored, and re-stripped
after re-parse). The transactions PK on referenceID is NOT mirrored
here — audit rows are historical snapshots, not live data.

Framework-free ORM model so db.create_all() picks it up at boot
(matches the ak-ifc-v3 pattern where the new column landed via ORM
+ ALTER — this one is even simpler, being a fresh table).
"""

from sqlalchemy import (
    Column, String, DateTime, Integer, Enum, func,
)
from sqlalchemy.types import DECIMAL as Decimal

from models.Base import Base
from models.transactions import ProcessingMethod


class StrippedTransactionsAudit(Base):
    __tablename__ = 'stripped_transactions_audit'

    # Synthetic auto-increment PK — audit rows are historical
    # snapshots, so we don't reuse the transactions.referenceID as
    # PK (a file stripped twice would collide on re-snapshot).
    audit_id = Column(Integer, primary_key=True, autoincrement=True)

    # Mirror of the transactions columns. NO ForeignKey constraints
    # here — the referenced row is being DELETED right after we
    # snapshot it, so an FK would violate. The row is meant to
    # stand on its own.
    referenceID = Column(String(64), nullable=False, index=True)
    date = Column(DateTime, nullable=False)
    details = Column(String(500), nullable=False)
    amount = Column(Decimal(10, 2), nullable=False)
    tag = Column(String(100))
    fileID = Column(String(100), nullable=True)
    source = Column(String(10), nullable=False)
    bank = Column(String(25), nullable=False)
    user = Column(String(100), nullable=False, index=True)
    processed_via = Column(
        Enum(ProcessingMethod), default=ProcessingMethod.PATTERN_MATCH,
    )
    gmail_message_id = Column(String(500), nullable=True)
    transfer_group_id = Column(String(64), nullable=True)
    bank_reference_id = Column(String(128), nullable=True)

    # Audit-scope metadata.
    stripped_at = Column(
        DateTime, nullable=False,
        server_default=func.current_timestamp(),
    )
    stripped_reason = Column(String(64), nullable=True)
    stripped_file_id = Column(String(100), nullable=False, index=True)

    @classmethod
    def from_transaction(cls, txn, *, reason: str, file_id: str):
        """Build a snapshot row from a live Transactions ORM object.
        Copies the mirrored columns verbatim; the audit-scope
        columns are set from the strip context.

        Caller commits after populating the batch.
        """
        return cls(
            referenceID=txn.referenceID,
            date=txn.date,
            details=txn.details,
            amount=txn.amount,
            tag=txn.tag,
            fileID=txn.fileID,
            source=txn.source,
            bank=txn.bank,
            user=txn.user,
            processed_via=txn.processed_via,
            gmail_message_id=txn.gmail_message_id,
            transfer_group_id=txn.transfer_group_id,
            bank_reference_id=txn.bank_reference_id,
            stripped_reason=reason,
            stripped_file_id=file_id,
        )
