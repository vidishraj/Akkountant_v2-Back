from sqlalchemy import Column, DateTime, String
from models.Base import Base
from sqlalchemy.orm import relationship


# Not sure about this at the moment.

class User(Base):
    __tablename__ = 'users'

    userID = Column(String(100), primary_key=True)
    email = Column(String(100), nullable=True)
    optedBanks = Column(String(500), nullable=True)
    # Timestamp of the most recent WealthDigest page visit / explicit
    # Mark-Read for this user. NULL = never read (nav badge shows).
    # Cross-device sync: FE clears the badge optimistically on visit +
    # POST /wealth-digest/mark-read stamps this column so a second
    # device sees the badge cleared on next fetch.
    #
    # MIGRATION (HARD PREREQ for deploy — sequencing matters):
    #   ALTER TABLE users ADD COLUMN wealth_digest_last_read_at
    #     TIMESTAMP NULL DEFAULT NULL;
    #
    # Recommended sequencing: run the ALTER BEFORE mayor-merge, or
    # atomically with the GHA-triggered app.service restart. Not
    # "run it whenever" — see below for what breaks pre-migration.
    #
    # Pre-migration deploy window behavior (asymmetric):
    #
    #   * READ side (endpoints): `_get_user_read_at` explicitly SELECTs
    #     `wealth_digest_last_read_at` — this SELECT fails until the
    #     ALTER runs. Service catches the OperationalError/Programming-
    #     Error, returns None, and the digest text still surfaces
    #     (`read_at: null` — same shape as never-read). Endpoint UX
    #     remains usable during a brief migration lag; nav badge just
    #     doesn't clear cross-device until the ALTER runs.
    #
    #   * WRITE side (WealthDigestTask cycle): pre-ak-5vg-v2 was
    #     coupled to the full User model — `query(User).filter(...)`
    #     included this column and would fail pre-migration, halting
    #     new-digest generation. FIXED in v2 by narrowing that write-
    #     path check to `query(User.userID)` (see
    #     WealthDigestTask._user_exists). Post-fix, write-side is NOT
    #     halted by pre-migration state; new digests continue to
    #     generate normally.
    #
    # Same shape as the fileDetails.reconciliation_fallback deploy
    # pattern (ak-ifc): the model declares the column so ORM
    # reads/writes it; `db.create_all()` in app._setup_database only
    # creates missing TABLES, not missing COLUMNS on existing tables.
    #
    # Future User-model additions: reuse the narrow-column-query
    # pattern (`query(User.<column>)` not `query(User)`) for any
    # write-path lookup that doesn't NEED the new column — eliminates
    # the coupling class this section documents.
    wealth_digest_last_read_at = Column(DateTime, nullable=True)

    saved_tags = relationship('SavedTags', back_populates='user_relationship')
    statement_passwords = relationship('StatementPasswords', back_populates='user_relationship')
    transactions = relationship('Transactions', back_populates='user_relationship')
    file_details = relationship('FileDetails', back_populates='user_relationship')
    transaction_reviews = relationship('TransactionForReview', back_populates='user_relationship')
    user_tokens = relationship('UserToken', back_populates='user_relationship')
    purchased_securities = relationship('PurchasedSecurities', back_populates='user_relationship')
    deposit_relationship = relationship('DepositSecurities', back_populates='user_relationship')
    transaction_relationship = relationship('SecurityTransactions', back_populates='user_relationship')
    investment_history = relationship('InvestmentHistory', back_populates='user_relationship')
    investment_snapshots = relationship('InvestmentSnapshot', back_populates='user_relationship')
    fo_trades = relationship('FOTrade', back_populates='user_relationship')
    processed_emails = relationship('ProcessedEmails', back_populates='user_relationship')
    statement_periods = relationship('StatementPeriod', back_populates='user_relationship')
    transfer_links = relationship('TransferLink', back_populates='user_relationship')
