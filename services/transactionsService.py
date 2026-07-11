import os
import shutil

from flask_sqlalchemy.session import Session
from sqlalchemy.exc import IntegrityError, SQLAlchemyError
from sqlalchemy import func, case

from enums.ServiceTypeEnum import ServiceTypeEnum
from enums.TransactionTypeEnum import TransactionTypeEnum
from models import User, UserToken, Transactions, TransactionForReview, StatementPasswords, FileDetails
from models.transactions import ProcessingMethod
from services.Base_Service import BaseService
from utils.logger import Logger


class TransactionService(BaseService):
    _instance = None

    def __new__(cls):
        if cls._instance is None:
            cls._instance = super(TransactionService, cls).__new__(cls)
            cls.logger = Logger(__name__).get_logger()
        return cls._instance

    def __init__(self):
        super().__init__()

    def fetchTransactions(self, page: int, filters: dict, user_id: str = None, page_size: int = 100):
        query = self.db.session.query(Transactions)

        # Filter by user
        if user_id:
            query = query.filter(Transactions.user == user_id)

        # Determine if we should apply the default Claude filter
        apply_claude_default = True
        if filters and 'processed_via' in filters:
            processed_via = filters.get('processed_via')
            if processed_via == 'ALL':
                apply_claude_default = False
            elif processed_via == 'CLAUDE_CODE':
                query = query.filter(Transactions.processed_via == ProcessingMethod.CLAUDE_CODE)
                apply_claude_default = False
            elif processed_via == 'PATTERN_MATCH':
                query = query.filter(Transactions.processed_via == ProcessingMethod.PATTERN_MATCH)
                apply_claude_default = False
        
        # Apply default Claude filter if no explicit processing method filter
        if apply_claude_default:
            query = query.filter(Transactions.processed_via == ProcessingMethod.CLAUDE_CODE)

        # Apply other filters if they are provided
        if filters:
            if date_range := filters.get('dateRange'):
                date_from = date_range.get('dateFrom')
                date_to = date_range.get('dateTo')
                if date_from and date_to:
                    query = query.filter(Transactions.date.between(date_from, date_to))
            if details := filters.get('details'):
                query = query.filter(Transactions.details.ilike(f"%{details}%"))
            if tag := filters.get('tags'):
                query = query.filter(Transactions.tag.ilike(f"%{tag}%"))
            if bank := filters.get('bank'):
                query = query.filter(Transactions.bank == bank)
            if source := filters.get('source'):
                query = query.filter(Transactions.source == source)

        # Get the total count before pagination by creating a new count query
        total_count_query = self.db.session.query(func.count(Transactions.referenceID))

        # Apply user filter to count query
        if user_id:
            total_count_query = total_count_query.filter(Transactions.user == user_id)

        # Apply the same processing method filter to count query
        if apply_claude_default:
            total_count_query = total_count_query.filter(Transactions.processed_via == ProcessingMethod.CLAUDE_CODE)
        elif filters and 'processed_via' in filters:
            processed_via = filters.get('processed_via')
            if processed_via == 'CLAUDE_CODE':
                total_count_query = total_count_query.filter(Transactions.processed_via == ProcessingMethod.CLAUDE_CODE)
            elif processed_via == 'PATTERN_MATCH':
                total_count_query = total_count_query.filter(Transactions.processed_via == ProcessingMethod.PATTERN_MATCH)

        # Reapply the same filters for the count query
        if filters:
            if date_range := filters.get('dateRange'):
                date_from = date_range.get('dateFrom')
                date_to = date_range.get('dateTo')
                if date_from and date_to:
                    total_count_query = total_count_query.filter(Transactions.date.between(date_from, date_to))
            if details := filters.get('details'):
                total_count_query = total_count_query.filter(Transactions.details.ilike(f"%{details}%"))
            if tag := filters.get('tags'):
                total_count_query = total_count_query.filter(Transactions.tag.ilike(f"%{tag}%"))
            if bank := filters.get('bank'):
                total_count_query = total_count_query.filter(Transactions.bank == bank)
            if source := filters.get('source'):
                total_count_query = total_count_query.filter(Transactions.source == source)

        total_count = total_count_query.scalar()

        # Calculate sums for debit and credit amounts
        sum_query = self.db.session.query(
            func.sum(case((Transactions.amount < 0, Transactions.amount), else_=0)).label("credit_sum"),
            func.sum(case((Transactions.amount > 0, Transactions.amount), else_=0)).label("debit_sum"),
        )

        # Apply user filter to sum query
        if user_id:
            sum_query = sum_query.filter(Transactions.user == user_id)

        # Apply the same processing method filter to sum query
        if apply_claude_default:
            sum_query = sum_query.filter(Transactions.processed_via == ProcessingMethod.CLAUDE_CODE)
        elif filters and 'processed_via' in filters:
            processed_via = filters.get('processed_via')
            if processed_via == 'CLAUDE_CODE':
                sum_query = sum_query.filter(Transactions.processed_via == ProcessingMethod.CLAUDE_CODE)
            elif processed_via == 'PATTERN_MATCH':
                sum_query = sum_query.filter(Transactions.processed_via == ProcessingMethod.PATTERN_MATCH)

        # Reapply the same filters for the sum query
        if filters:
            if date_range := filters.get('dateRange'):
                date_from = date_range.get('dateFrom')
                date_to = date_range.get('dateTo')
                if date_from and date_to:
                    sum_query = sum_query.filter(Transactions.date.between(date_from, date_to))
            if details := filters.get('details'):
                sum_query = sum_query.filter(Transactions.details.ilike(f"%{details}%"))
            if tag := filters.get('tags'):
                sum_query = sum_query.filter(Transactions.tag.ilike(f"%{tag}%"))
            if bank := filters.get('bank'):
                sum_query = sum_query.filter(Transactions.bank == bank)
            if source := filters.get('source'):
                sum_query = sum_query.filter(Transactions.source == source)

        credit_sum, debit_sum = sum_query.first() or (0, 0)

        # Apply sorting if `sorted` is provided
        if sorted := filters.get('sorted'):
            column, order = sorted.get('column'), sorted.get('order', 'asc')
            if column and hasattr(Transactions, column):
                column_attr = getattr(Transactions, column)
                if order == 'desc':
                    query = query.order_by(column_attr.desc())
                else:
                    query = query.order_by(column_attr.asc())

        # Apply pagination to the main query
        paginated_query = query.offset((page - 1) * page_size).limit(filters.get('limit', page_size))

        # Execute and return the results
        paginated_results = paginated_query.all()

        return {
            "count": total_count,
            "results": paginated_results,
            "credit_sum": credit_sum,
            "debit_sum": debit_sum,
            "page": page
        }

    def fetchBanksOptedByUser(self, userID):
        from enums.BanksEnum import BankEnums
        return [bank.value for bank in BankEnums]

    def fetchTransactionDates(self, date_from: str, date_to: str, user_id: str = None):
        """
        Service to fetch transaction and statement dates within a given date range,
        plus statement period coverage for the calendar.
        """
        from models.statementPeriods import StatementPeriod

        transaction_query = (
            self.db.session.query(func.date(Transactions.date).label('txn_date'))
            .filter(Transactions.date.between(date_from, date_to))
        )
        if user_id:
            transaction_query = transaction_query.filter(Transactions.user == user_id)
        transaction_query = transaction_query.distinct()

        statement_query = (
            self.db.session.query(FileDetails.uploadDate)
            .filter(FileDetails.uploadDate.between(date_from, date_to))
            .filter(FileDetails.deleted == False)
        )
        if user_id:
            statement_query = statement_query.filter(FileDetails.user == user_id)
        statement_query = statement_query.distinct()

        # Statement periods that overlap with the requested date range
        period_query = self.db.session.query(StatementPeriod).filter(
            StatementPeriod.period_start <= date_to,
            StatementPeriod.period_end >= date_from,
        )
        if user_id:
            period_query = period_query.filter(StatementPeriod.user == user_id)
        period_query = period_query.all()

        transaction_dates = [t[0].strftime("%Y-%m-%d") if hasattr(t[0], 'strftime') else str(t[0]) for t in transaction_query]
        statement_dates = [s[0].strftime("%Y-%m-%d") for s in statement_query]
        covered_periods = [{
            "bank": p.bank,
            "period_start": p.period_start.isoformat(),
            "period_end": p.period_end.isoformat(),
        } for p in period_query]

        return {
            "transaction_dates": transaction_dates,
            "statement_dates": statement_dates,
            "covered_periods": covered_periods,
        }



    def insertTransactions(self, transactions, bank, userId, conflicts, source, fileId=None, source_emails=None):
        """OPTIMIZED: Batch insert transactions for better performance"""
        integrityErrors = 0
        
        if not transactions and not conflicts:
            return 0
            
        try:
            # OPTIMIZATION: Batch prepare all transaction objects
            transaction_objects = []
            
            for i, transaction in enumerate(transactions):
                date = self.dateTimeUtil.convert_to_sql_datetime(transaction['date'], bank)
                
                # Determine processing method
                processing_method = ProcessingMethod.CLAUDE_CODE if transaction.get('processed_via') == 'CLAUDE_CODE' else ProcessingMethod.PATTERN_MATCH
                
                # For email-based transactions, get the Gmail message ID
                gmail_message_id = None
                if source_emails and source == TransactionTypeEnum.Email.value:
                    # Use per-transaction email if available, otherwise fall back to first (batch from single email)
                    email_entry = source_emails[i] if i < len(source_emails) else source_emails[0]
                    gmail_message_id = email_entry.get('message_id')
                
                transaction_obj = Transactions(
                    referenceID=transaction['reference'],
                    date=date,
                    details=transaction['description'],
                    amount=transaction['amount'],
                    tag="",
                    fileID=fileId,
                    bank=bank,
                    source=source,
                    user=userId,
                    processed_via=processing_method,
                    gmail_message_id=gmail_message_id,
                    # ak-8l5: persist the LLM-extracted per-tx bank ref
                    # alongside the derived PK so downstream tools
                    # (audit, reprocess, inspection queries) can see
                    # the identifier that drove the dedup without
                    # re-reading the PDF.
                    bank_reference_id=transaction.get('bank_reference_id'),
                )
                transaction_objects.append(transaction_obj)
            
            # OPTIMIZATION: Batch insert all transactions in a single transaction
            if transaction_objects:
                try:
                    if isinstance(self.db, dict):
                        with self.db.session() as session:
                            session.add_all(transaction_objects)
                            session.commit()
                    else:
                        self.db.session.add_all(transaction_objects)
                        self.db.session.commit()
                    self.logger.info(f"Batch inserted {len(transaction_objects)} transactions")
                except IntegrityError as e:
                    self.logger.warning(f"Batch insert failed, falling back to individual inserts: {e}")
                    self.db.session.rollback()
                    # Fallback to individual inserts to handle duplicates
                    integrityErrors = self._insert_transactions_individually(transaction_objects)

            # OPTIMIZATION: Batch insert conflicts with duplicate checking
            if conflicts:
                # Check for existing conflicts to avoid duplicates
                existing_conflicts = set()
                if isinstance(self.db, dict):
                    with self.db.session() as session:
                        existing = session.query(TransactionForReview.conflict).filter(
                            TransactionForReview.user == userId
                        ).all()
                        existing_conflicts = {row.conflict for row in existing}
                else:
                    existing = self.db.session.query(TransactionForReview.conflict).filter(
                        TransactionForReview.user == userId
                    ).all()
                    existing_conflicts = {row.conflict for row in existing}
                
                # Filter out duplicate conflicts
                new_conflicts = [conflict for conflict in conflicts if conflict not in existing_conflicts]
                
                if new_conflicts:
                    conflict_objects = [
                        TransactionForReview(user=userId, conflict=conflict) 
                        for conflict in new_conflicts
                    ]
                    try:
                        if isinstance(self.db, dict):
                            with self.db.session() as session:
                                session.add_all(conflict_objects)
                                session.commit()
                        else:
                            self.db.session.add_all(conflict_objects)
                            self.db.session.commit()
                        self.logger.info(f"Batch inserted {len(conflict_objects)} new conflicts (skipped {len(conflicts) - len(new_conflicts)} duplicates)")
                    except IntegrityError as e:
                        self.logger.warning(f"Batch conflict insert failed: {e}")
                        self.db.session.rollback()
                else:
                    self.logger.info(f"Skipped {len(conflicts)} duplicate conflicts")
                    
        except Exception as e:
            self.logger.error(f"Error in batch insert: {str(e)}")
            self.db.session.rollback()
            
        return integrityErrors

    def _insert_transactions_individually(self, transaction_objects):
        """Fallback method for individual transaction inserts when batch fails.

        ak-8l5 review MAJOR — code-level zero-loss backstop:

        When an insert hits IntegrityError on the referenceID PK, we
        no longer silently drop the row. The `utils.insert_backstop`
        helper decides between:

          - Silent drop (chunk-overlap dedup path; same content) —
            the intended pre-ak-8l5 behavior.
          - Suffix + retry (differing content) — extractor mapped two
            distinct rows to the same ref; suffix disambiguator with
            "-dupN" and retry so neither row is lost.

        Overseer's hard requirement: "no transactions whatsoever lost".
        Extraction perfection can't be guaranteed at the LLM layer, so
        this is the safety net at the storage layer.

        The Gmail-side dedup path (unique gmail_message_id) is
        UNCHANGED — it existed pre-ak-8l5 and is not about ref
        collisions.
        """
        from utils.reference_id import normalize_description_for_backstop
        from utils.insert_backstop import apply_backstop_on_collision

        integrityErrors = 0
        disambiguated_count = 0

        for transaction_obj in transaction_objects:
            try:
                if isinstance(self.db, dict):
                    with self.db.session() as session:
                        session.add(transaction_obj)
                        session.commit()
                else:
                    self.db.session.add(transaction_obj)
                    self.db.session.commit()
                continue  # inserted cleanly, next row
            except IntegrityError as e:
                error_msg = str(e)
                # Gmail-side dedup (unique gmail_message_id) — not a
                # tx PK collision. Existing pre-ak-8l5 behavior.
                if 'gmail_message_id' in error_msg and transaction_obj.gmail_message_id:
                    self.logger.debug(
                        f"Skipping duplicate email transaction "
                        f"(Gmail ID: {transaction_obj.gmail_message_id})"
                    )
                    self.db.session.rollback()
                    integrityErrors += 1
                    continue
                # PK collision. Roll back, then invoke the ak-8l5
                # backstop.
                self.db.session.rollback()

            # ── ak-8l5 MAJOR backstop ──────────────────────────────
            outcome = apply_backstop_on_collision(
                session=self.db.session,
                model_class=Transactions,
                incoming=transaction_obj,
                normalize_desc=normalize_description_for_backstop,
                logger=self.logger,
            )
            if outcome == "disambiguated":
                disambiguated_count += 1
            elif outcome in ("dropped_matching", "dropped_missing", "retry_failed"):
                integrityErrors += 1

        if disambiguated_count > 0:
            self.logger.info(
                f"ak-8l5 backstop applied to {disambiguated_count} tx(s) "
                f"with colliding referenceID but differing content — "
                f"both original and incoming preserved via suffix"
            )

        return integrityErrors

    def fetchGmailTokenForUser(self, userID):
        userToken = self.db.session.query(UserToken).filter_by(user_id=userID) \
            .filter_by(service_type=ServiceTypeEnum.Gmail.value).first()
        return {
            'token': userToken.access_token,
            'refresh_token': userToken.refresh_token,
            'client_id': userToken.client_id,
            'client_secret': userToken.client_secret,
        }

    def fetchDriveTokenForUser(self, userID):
        userToken = self.db.session.query(UserToken).filter_by(user_id=userID). \
            filter_by(service_type=ServiceTypeEnum.Gdrive.value).first()
        return {
            'token': userToken.access_token,
            'refresh_token': userToken.refresh_token,
            'client_id': userToken.client_id,
            'client_secret': userToken.client_secret,
        }

    def insertFileDetails(self, fileId, fileName, statementCount,
                          bank, user, path, gmail_message_id=None):
        fileDetails = FileDetails(
            fileID=fileId,
            uploadDate=self.dateTimeUtil.getCurrentDatetimeSqlFormat(),
            fileName=fileName,
            fileSize=self.genericUtil.getFileSize(path),
            statementCount=statementCount,
            bank=bank,
            user=user,
            gmail_message_id=gmail_message_id
        )
        try:
            if isinstance(self.db, dict):
                with self.db.session() as session:
                    session.add(fileDetails)
                    session.commit()
            else:
                self.db.session.add(fileDetails)
                self.db.session.commit()
        except IntegrityError as e:
            self.logger.warning(f"Duplicate file details entry error occurred: {e.__cause__}")
            self.db.session.rollback()

    def deleteFileDetails(self, fileId, user_id=None):
        # Find the row by ID and delete it
        query = self.db.session.query(FileDetails).filter_by(fileID=fileId)
        if user_id:
            query = query.filter(FileDetails.user == user_id)
        row = query.first()
        if row:
            if isinstance(self.db, dict):
                with self.db.session() as session:
                    session.delete(row)
                    session.commit()
            else:
                self.db.session.delete(row)
                self.db.session.commit()

    def updateStatementCount(self, fileId, newStatementCount):
        # Find the row by ID to update
        row = self.db.session.query(FileDetails).filter_by(fileID=fileId).first()
        if row:
            setattr(row, 'statementCount', newStatementCount)
            if isinstance(self.db, dict):
                with self.db.session() as session:
                    session.commit()
            else:
                self.db.session.commit()

    def mark_reconciliation_fallback(self, fileId, user_id=None):
        """ak-ifc v3 MINOR 1: mark a fileDetails row as having
        triggered the savings-summary reconciliation fallback.

        The row is tagged so a follow-up sweep can flag the file for
        manual review of the acknowledged non-savings over-parse
        that unfiltered extraction introduces.

        Swallows ORM errors defensively so the fallback re-run still
        lands the data even if the column isn't present yet — infra
        needs to ALTER TABLE fileDetails ADD COLUMN
        reconciliation_fallback BOOLEAN NOT NULL DEFAULT 0 before
        this becomes observable.
        """
        try:
            query = self.db.session.query(FileDetails).filter_by(fileID=fileId)
            if user_id:
                query = query.filter(FileDetails.user == user_id)
            row = query.first()
            if not row:
                self.logger.warning(
                    f"ak-ifc reconciliation-fallback tag: no fileDetails "
                    f"row for fileID={fileId!r}"
                )
                return False
            setattr(row, "reconciliation_fallback", True)
            if isinstance(self.db, dict):
                with self.db.session() as session:
                    session.commit()
            else:
                self.db.session.commit()
            self.logger.info(
                f"ak-ifc reconciliation-fallback tag: marked fileID="
                f"{fileId!r} for follow-up manual review"
            )
            return True
        except Exception as e:
            self.logger.warning(
                f"ak-ifc reconciliation-fallback tag: could not mark "
                f"fileID={fileId!r}: {e}. Data lands regardless; run "
                f"the ALTER TABLE migration to enable this tag."
            )
            try:
                self.db.session.rollback()
            except Exception:
                pass
            return False

    def _resolve_fallback_pdf_path(self, fd, user_id):
        """ak-ex2-v2 MINOR 1: derive the PDF path server-side from
        (fileDetails, user_id) — client never supplies a filesystem
        string.

        Resolution order:
          1. processedEmails.pdf_filename joined with the standard
             claude_statements/<user>/ dir (matches how
             _save_pdf_to_persistent_storage stored it).
          2. Glob claude_statements/<user>/**/{gmail_id}_* as a
             fallback (matches reprocess_pdf's shape).

        Returns the resolved absolute path or None on failure.
        """
        import glob as _glob
        gmail_id = fd.gmail_message_id
        if not gmail_id:
            return None

        base = os.path.join(os.getcwd(), "claude_statements", user_id)

        # Preferred: use processedEmails.pdf_filename if we have it.
        try:
            from models.processedEmails import ProcessedEmails
            pe = self.db.session.query(ProcessedEmails).filter_by(
                gmail_id=gmail_id, user_id=user_id,
            ).first()
            if pe and pe.pdf_filename:
                cand = os.path.join(base, pe.pdf_filename)
                if os.path.exists(cand):
                    return os.path.realpath(cand)
        except Exception as e:
            self.logger.debug(
                f"ak-ex2 strip: processedEmails path lookup failed "
                f"for gmail_id={gmail_id!r}: {e}"
            )

        # Fallback: glob by gmail_id prefix.
        matches = _glob.glob(
            os.path.join(base, "**", f"{gmail_id}_*"), recursive=True,
        )
        if matches:
            return os.path.realpath(matches[0])
        return None

    def strip_non_savings_from_fallback_file(
        self, fileId, user_id, password=None,
    ):
        """ak-ex2-v2: for a file previously tagged
        reconciliation_fallback=True by the ak-ifc-v3 file-level
        reconciliation, snapshot then remove non-savings sub-account
        rows (credit card / fixed deposit / mutual fund / RD / PPF /
        current) that the unfiltered fallback re-extraction
        re-admitted into the savings fileID.

        v2 changes (per reviewer BOUNCE ak-ex2-v2):
          - MAJOR: snapshot every candidate row into
            stripped_transactions_audit BEFORE the DELETE fires.
            Manual restore path recovers via
              INSERT INTO transactions SELECT ... FROM
                stripped_transactions_audit WHERE stripped_file_id=?
            Under Overseer's zero-loss constraint, heuristic-driven
            deletes MUST have a restore path.
          - MINOR 1: pdf_path is derived server-side from
            fileDetails.gmail_message_id + processedEmails.pdf_filename
            (or a claude_statements glob fallback). Callers no
            longer supply a filesystem string.
          - MINOR 3: FileDetails query is user-scoped so callers
            can't probe another user's file state.

        Returns a dict with:
          - status: "stripped" | "skipped" | "error"
          - reason: short explanation
          - removed: count deleted
          - kept: count classified SAVINGS
          - unlocatable: count where we couldn't classify (skipped,
            never deleted — conservative)
          - total: total tx rows examined
          - snapshot_count: count copied into
            stripped_transactions_audit before delete (should equal
            `removed` on success; a mismatch indicates partial
            rollback and shows up in error logs)
          - pdf_path: the server-derived path used (for audit trail)
        """
        import fitz as _fitz
        from models.transactions import Transactions
        from models.strippedTransactionsAudit import (
            StrippedTransactionsAudit,
        )
        from utils.statement_sections import (
            SectionType,
            build_line_to_section_map,
            classify_row_section,
            detect_hdfc_sections,
        )

        def _err(reason, **extra):
            base = {"status": "error", "reason": reason,
                    "removed": 0, "kept": 0, "unlocatable": 0,
                    "total": 0, "snapshot_count": 0, "pdf_path": None}
            base.update(extra)
            return base

        def _skip(reason, **extra):
            base = {"status": "skipped", "reason": reason,
                    "removed": 0, "kept": 0, "unlocatable": 0,
                    "total": 0, "snapshot_count": 0, "pdf_path": None}
            base.update(extra)
            return base

        # MINOR 3: user-scoped FileDetails lookup.
        try:
            fd = self.db.session.query(FileDetails).filter_by(
                fileID=fileId, user=user_id,
            ).first()
        except Exception as e:
            self.logger.error(
                f"ak-ex2 strip: FileDetails lookup failed for "
                f"fileID={fileId!r} user={user_id!r}: {e}"
            )
            return _err(f"lookup failed: {e}")
        if not fd:
            return _skip("no fileDetails row for (fileID, user)")
        if not getattr(fd, "reconciliation_fallback", False):
            return _skip("reconciliation_fallback is not True")
        if fd.bank != "HDFC_DEBIT":
            # The current stripper is HDFC-specific — section
            # detection recognizes HDFC layouts only. BOI equivalent
            # deferred (per ak-ifc BOI follow-up TODO).
            return _skip(f"bank={fd.bank!r} not supported by strip")

        # MINOR 1: derive PDF path server-side.
        pdf_path = self._resolve_fallback_pdf_path(fd, user_id)
        if not pdf_path:
            return _err(
                "could not resolve pdf_path server-side; "
                "check processedEmails.pdf_filename + claude_statements/"
            )
        if not os.path.exists(pdf_path):
            return _err(f"resolved pdf_path missing on disk: {pdf_path}",
                        pdf_path=pdf_path)

        # Read the PDF text.
        try:
            doc = _fitz.open(pdf_path)
            if doc.needs_pass and password:
                doc.authenticate(password)
            raw_lines = []
            for pn in range(1, doc.page_count + 1):
                # Same page-marker shape as detect_hdfc_sections' input
                # so span indices align.
                raw_lines.append(f"\f<PAGE:{pn}>")
                for line in doc[pn - 1].get_text("text").split("\n"):
                    raw_lines.append(line)
            doc.close()
        except Exception as e:
            self.logger.error(
                f"ak-ex2 strip: could not read PDF at {pdf_path!r} "
                f"for fileID={fileId!r}: {e}"
            )
            return _err(f"PDF read failed: {e}", pdf_path=pdf_path)

        raw_text = "\n".join(raw_lines)
        spans = detect_hdfc_sections(raw_text)
        section_by_line = build_line_to_section_map(spans, len(raw_lines))

        # Load all tx rows for this file (user-scoped).
        try:
            rows = self.db.session.query(Transactions).filter(
                Transactions.fileID == fileId,
                Transactions.user == user_id,
            ).all()
        except Exception as e:
            self.logger.error(
                f"ak-ex2 strip: transaction query failed for "
                f"fileID={fileId!r}: {e}"
            )
            return _err(f"transaction query failed: {e}",
                        pdf_path=pdf_path)

        to_delete_rows = []  # keep the Transactions objects for snapshotting
        to_delete_refs = []  # referenceID list for the batch DELETE
        kept = 0
        unlocatable = 0
        for row in rows:
            try:
                amount = float(row.amount)
            except (TypeError, ValueError):
                unlocatable += 1
                continue
            section = classify_row_section(
                amount, row.details or "",
                raw_lines, section_by_line,
            )
            if section == SectionType.SAVINGS:
                kept += 1
            elif section == SectionType.UNKNOWN:
                # Ambiguous / not found → conservative KEEP
                unlocatable += 1
            else:
                to_delete_rows.append(row)
                to_delete_refs.append(row.referenceID)

        removed = 0
        snapshot_count = 0

        if to_delete_rows:
            # ── MAJOR: snapshot BEFORE delete ─────────────────────
            # Every row about to be deleted is copied into
            # stripped_transactions_audit so a manual restore path
            # exists. This is the whole point of ak-ex2-v2 — under
            # zero-loss, we can't hard-delete without a recovery
            # trail.
            try:
                audit_rows = [
                    StrippedTransactionsAudit.from_transaction(
                        row,
                        reason="ak-ex2 non-savings",
                        file_id=fileId,
                    )
                    for row in to_delete_rows
                ]
                self.db.session.add_all(audit_rows)
                # Flush (not commit) so the audit rows are visible
                # in this transaction before the DELETE — one atomic
                # commit at the end wraps snapshot + delete.
                self.db.session.flush()
                snapshot_count = len(audit_rows)
            except Exception as e:
                # Snapshot failure BLOCKS the delete. Better to leave
                # the over-parse in place (recoverable via a later
                # re-strip after the audit table is available) than
                # to delete without a snapshot (irreversible loss).
                self.logger.error(
                    f"ak-ex2 strip: snapshot INSERT failed for "
                    f"fileID={fileId!r}: {e}. ABORTING delete to "
                    f"preserve recovery contract."
                )
                try:
                    self.db.session.rollback()
                except Exception:
                    pass
                return _err(f"snapshot failed: {e}",
                            total=len(rows), pdf_path=pdf_path)

            # ── Then the DELETE ─────────────────────────────────
            try:
                removed = self.db.session.query(Transactions).filter(
                    Transactions.referenceID.in_(to_delete_refs),
                    Transactions.user == user_id,
                    Transactions.fileID == fileId,
                ).delete(synchronize_session='fetch')
                self.db.session.commit()

                if removed != snapshot_count:
                    # Log a warning — the snapshot succeeded but the
                    # DELETE removed a different count (race with a
                    # concurrent insert / delete). The extra snapshot
                    # rows are harmless (audit only); the missing
                    # delete rows survive live.
                    self.logger.warning(
                        f"ak-ex2 strip: snapshot/delete count mismatch "
                        f"for fileID={fileId!r}: snapshot={snapshot_count} "
                        f"removed={removed}. Investigate for a race."
                    )

                self.logger.warning(
                    f"ak-ex2 strip: snapshotted={snapshot_count}, "
                    f"removed={removed} non-savings row(s) from "
                    f"fileID={fileId!r} (bank={fd.bank}, "
                    f"user={user_id!r}). kept={kept} "
                    f"unlocatable={unlocatable} total={len(rows)}. "
                    f"Restore path: SELECT ... FROM "
                    f"stripped_transactions_audit WHERE "
                    f"stripped_file_id={fileId!r};"
                )
            except Exception as e:
                self.logger.error(
                    f"ak-ex2 strip: DELETE failed for fileID={fileId!r} "
                    f"AFTER successful snapshot (snapshot_count="
                    f"{snapshot_count}): {e}. Rolling back both."
                )
                try:
                    self.db.session.rollback()
                except Exception:
                    pass
                return _err(f"delete failed: {e}",
                            kept=kept, unlocatable=unlocatable,
                            total=len(rows), snapshot_count=0,
                            pdf_path=pdf_path)
        else:
            self.logger.info(
                f"ak-ex2 strip: no non-savings rows to remove from "
                f"fileID={fileId!r}. kept={kept} unlocatable={unlocatable} "
                f"total={len(rows)}"
            )

        return {
            "status": "stripped",
            "reason": "ok",
            "removed": removed,
            "kept": kept,
            "unlocatable": unlocatable,
            "total": len(rows),
            "snapshot_count": snapshot_count,
            "pdf_path": pdf_path,
        }

    def fetchFileDetails(self, page: int, filters: dict, user_id: str = None, page_size: int = 100):
        query = self.db.session.query(FileDetails).filter(FileDetails.deleted == False)

        # Filter by user
        if user_id:
            query = query.filter(FileDetails.user == user_id)

        # Apply filters if they are provided
        if filters:
            if upload_date_range := filters.get('dateRange'):
                date_from = upload_date_range.get('dateFrom')
                date_to = upload_date_range.get('dateTo')
                if date_from and date_to:
                    query = query.filter(FileDetails.uploadDate.between(date_from, date_to))
            if file_name := filters.get('fileName'):
                query = query.filter(FileDetails.fileName.ilike(f"%{file_name}%"))
            if bank := filters.get('bank'):
                query = query.filter(FileDetails.bank == bank)

        # Get the total count before pagination
        total_count = query.count()

        # Apply sorting if `sorted` is provided
        if sorted := filters.get('sorted'):
            column, order = sorted.get('column'), sorted.get('order', 'asc')
            if column and hasattr(FileDetails, column):
                column_attr = getattr(FileDetails, column)
                if order == 'desc':
                    query = query.order_by(column_attr.desc())
                else:
                    query = query.order_by(column_attr.asc())

        # Apply pagination to the main query
        paginated_query = query.offset((page - 1) * page_size).limit(filters.get('limit', page_size))

        # Execute and return the results
        paginated_results = paginated_query.all()

        return {
            "count": total_count,
            "results": paginated_results,
            "page": page
        }

    def updateTransaction(self, reference_id: int, updates: dict, user_id: str = None):
        # Fetch the transaction by referenceID
        query = self.db.session.query(Transactions).filter_by(referenceID=reference_id)
        if user_id:
            query = query.filter(Transactions.user == user_id)
        transaction = query.first()

        # If transaction is not found, return an error message
        if not transaction:
            return {"error": f"Transaction with referenceID {reference_id} not found"}

        # Update only the allowed fields if they are present in the updates dictionary
        if 'details' in updates:
            transaction.details = updates['details']
        if 'tag' in updates:
            transaction.tag = updates['tag']
        if 'amount' in updates:
            transaction.amount = updates['amount']

        # Commit the changes
        self.db.session.commit()
        return {"message": "Transaction updated successfully"}

    def addUser(self, user_data: dict):
        # Create a new User instance
        new_user = User(
            userID=user_data['userID'],
            email=user_data.get('email'),
            optedBanks=user_data.get('optedBanks')  # Can be None if not provided
        )

        # Add and commit the new user to the session
        self.db.session.add(new_user)
        self.db.session.commit()
        return {"message": "User added successfully"}

    def updateOptedBanks(self, user_id: str, opted_banks: str):
        # Fetch the user by userID
        user = self.db.session.query(User).filter_by(userID=user_id).first()

        # If user doesn't exist, return an error message
        if not user:
            return {"error": f"User with userID {user_id} not found"}

        # Update optedBanks, overwriting if it already has a value
        user.optedBanks = opted_banks
        self.db.session.commit()
        return {"message": "optedBanks updated successfully"}

    def addUpdateUserToken(self, token_data: dict):
        # Check if the token already exists for the user and service type
        user_token = self.db.session.query(UserToken).filter_by(
            user_id=token_data['user_id'],
            service_type=token_data['service_type']
        ).first()

        if user_token:
            # Update existing token
            user_token.access_token = token_data['access_token']
            user_token.refresh_token = token_data['refresh_token']
            user_token.client_id = token_data['client_id']
            user_token.client_secret = token_data['client_secret']
            user_token.expiry = token_data['expiry']
            message = "User token updated successfully."
        else:
            # Add new token
            user_token = UserToken(
                user_id=token_data['user_id'],
                access_token=token_data['access_token'],
                refresh_token=token_data['refresh_token'],
                client_id=token_data['client_id'],
                client_secret=token_data['client_secret'],
                expiry=token_data['expiry'],
                service_type=token_data['service_type']
            )
            self.db.session.add(user_token)
            message = "User token added successfully."

        # Commit changes to the database
        self.db.session.commit()
        return {"message": message}

    def deleteFile(self, user_id: str, fileId: str):
        session = self.db.session
        try:
            with session.begin():  # Start an outer transaction
                self.logger.info("Fetched drive token")
                driveToken = self.fetchDriveTokenForUser(user_id)
                self.deleteFileDetails(fileId, user_id=user_id)
                self.logger.info("Deleted file details")
                self.deleteTransactionsFromAFile(fileId, user_id=user_id)
                self.logger.info("Deleted transaction related to file")
                self.driveService.deleteFile(fileId, user_id, driveToken)
                self.logger.info("Deleted file on Google Drive")
        except SQLAlchemyError as e:
            session.rollback()
            self.logger.error(f"Error deleting file details. Error: {e}")
            raise
        except Exception as e:
            session.rollback()
            self.logger.error(f"Error deleting file. Error: {e}")
            raise
        return {"message": "File deleted successfully"}

    def deleteTransactionsFromAFile(self, fileID, user_id=None):
        query = self.db.session.query(Transactions).filter_by(fileID=fileID)
        if user_id:
            query = query.filter(Transactions.user == user_id)
        result = query.delete(synchronize_session='fetch')
        return result

    def renameFile(self, user_id: str, fileId: str, newName: str):
        driveToken = self.fetchDriveTokenForUser(user_id)
        self.driveService.renameFile(fileId, newName, user_id, driveToken)
        fileDetails = self.db.session.query(FileDetails).filter_by(fileID=fileId).first()
        fileDetails.fileName = newName
        # If transaction is not found, return an error message
        if not fileDetails:
            return {"error": f"File info with fileID {fileId} not found"}
        self.db.session.commit()
        return {"message": "File renamed successfully"}

    def downloadFile(self, user_id: str, fileId: str):
        driveToken = self.fetchDriveTokenForUser(user_id)
        return self.driveService.downloadFile(fileId, user_id, driveToken)

    def checkGoogleStatus(self, user_id: str, serviceType: ServiceTypeEnum):
        if serviceType == ServiceTypeEnum.Gdrive:
            scopes = self.driveService.googleService.getDriveScope()
            try:
                token = self.fetchDriveTokenForUser(user_id)
                serviceCheck = self.driveService.checkStatus(token)
                if not serviceCheck:
                    return self.driveService.googleService.start_fresh_auth_flow(scopes)
                return {"Message": "Successful"}
            except Exception:
                return self.driveService.googleService.start_fresh_auth_flow(scopes)
        elif serviceType == ServiceTypeEnum.Gmail:
            scopes = self.gmailService.googleService.getGmailScope()
            try:
                token = self.fetchGmailTokenForUser(user_id)
                serviceCheck = self.gmailService.checkStatus(token)
                if not serviceCheck:
                    return self.gmailService.googleService.start_fresh_auth_flow(scopes)
                return {"Message": "Successful"}
            except Exception:
                return self.gmailService.googleService.start_fresh_auth_flow(scopes)
        return {"Message": "Weird Failure"}

    def setOptedBanks(self, user_id: str, banks: dict):
        """
           Updates the optedBanks for a user in the database.

           Args:
               user_id (str): The ID of the user.
               banks (list[str]): The list of bank names to set.

           Returns:
               User: The updated user object.
           """
        # Validate banks
        passwords = list(banks.values())
        banks = list(banks.keys())

        session: Session = self.db.session  # Replace with your DB session management
        user = None
        try:
            # Fetch user from DB
            with session.begin():
                user = session.query(User).filter(User.userID == user_id).first()
                if not user:
                    return None
                # Update optedBanks
                user.optedBanks = ','.join(banks)
                for index, password in enumerate(passwords):
                    statementPassword = self.db.session.query(StatementPasswords).filter_by(user=user_id) \
                        .filter_by(bank=banks[index]).first()
                    if statementPassword is None:
                        statement_password = StatementPasswords(
                            bank=banks[index],
                            password_hash=password,
                            user=user_id
                        )
                        # Add and commit the record
                        session.add(statement_password)
                    else:
                        statementPassword.password_hash = password
            return user
        except Exception as e:
            session.rollback()
            raise e
        finally:
            session.close()

    def getProcessingStats(self, userId):
        try:
            total_count = self.db.session.query(func.count(Transactions.referenceID)).filter_by(user=userId).scalar()
            
            pattern_count = self.db.session.query(func.count(Transactions.referenceID)).filter(
                Transactions.user == userId,
                Transactions.processed_via == ProcessingMethod.PATTERN_MATCH
            ).scalar()
            
            claude_count = self.db.session.query(func.count(Transactions.referenceID)).filter(
                Transactions.user == userId,
                Transactions.processed_via == ProcessingMethod.CLAUDE_CODE
            ).scalar()

            return {
                "total_transactions": total_count or 0,
                "pattern_match_count": pattern_count or 0,
                "claude_code_count": claude_count or 0,
                "claude_success_rate": round((claude_count or 0) / max(total_count or 1, 1) * 100, 2)
            }
        except Exception as e:
            self.logger.error(f"Error fetching processing stats: {str(e)}")
            return {"error": str(e)}

    # NEW: Local Statement Management Methods
    def fetchLocalStatements(self, userId, bank=None):
        """Fetch list of locally stored Claude-analyzed statements"""
        try:
            statements_dir = os.path.join(os.getcwd(), "claude_statements", userId)
            
            if not os.path.exists(statements_dir):
                return {"statements": [], "total_count": 0}
            
            statements = []
            banks_to_check = [bank] if bank else os.listdir(statements_dir)
            
            for bank_name in banks_to_check:
                bank_dir = os.path.join(statements_dir, bank_name)
                if not os.path.isdir(bank_dir):
                    continue
                    
                for filename in os.listdir(bank_dir):
                    if filename.endswith('.pdf'):
                        file_path = os.path.join(bank_dir, filename)
                        file_stat = os.stat(file_path)
                        
                        statements.append({
                            "filename": filename,
                            "bank": bank_name,
                            "file_size": file_stat.st_size,
                            "created_date": file_stat.st_ctime,
                            "modified_date": file_stat.st_mtime,
                            "file_path": file_path,
                            "storage_type": "local_claude"
                        })
            
            # Sort by modified date (newest first)
            statements.sort(key=lambda x: x['modified_date'], reverse=True)
            
            return {
                "statements": statements,
                "total_count": len(statements)
            }
            
        except Exception as e:
            self.logger.error(f"Error fetching local statements: {str(e)}")
            return {"error": str(e)}

    def downloadLocalStatement(self, userId, bank, filename):
        """Download a locally stored statement"""
        try:
            # Sanitize to prevent path traversal
            safe_bank = os.path.basename(bank)
            safe_filename = os.path.basename(filename)
            base_dir = os.path.join(os.getcwd(), "claude_statements", userId)
            file_path = os.path.join(base_dir, safe_bank, safe_filename)

            # Verify the resolved path is within the expected directory
            if not os.path.realpath(file_path).startswith(os.path.realpath(base_dir)):
                return {"error": "Invalid file path"}

            if not os.path.exists(file_path):
                return {"error": "Statement not found"}
            
            # Return file path for download (Flask will handle the actual file serving)
            return {"file_path": file_path, "filename": filename}
            
        except Exception as e:
            self.logger.error(f"Error downloading local statement: {str(e)}")
            return {"error": str(e)}

    def deleteLocalStatement(self, userId, bank, filename):
        """Delete a locally stored statement"""
        try:
            # Sanitize to prevent path traversal
            safe_bank = os.path.basename(bank)
            safe_filename = os.path.basename(filename)
            base_dir = os.path.join(os.getcwd(), "claude_statements", userId)
            file_path = os.path.join(base_dir, safe_bank, safe_filename)

            # Verify the resolved path is within the expected directory
            if not os.path.realpath(file_path).startswith(os.path.realpath(base_dir)):
                return {"error": "Invalid file path"}

            if not os.path.exists(file_path):
                return {"error": "Statement not found"}

            os.remove(file_path)
            self.logger.info(f"Deleted local statement: {file_path}")
            
            # Check if bank directory is empty and remove if so
            bank_dir = os.path.dirname(file_path)
            if not os.listdir(bank_dir):
                os.rmdir(bank_dir)
                self.logger.info(f"Removed empty bank directory: {bank_dir}")
            
            return {"message": "Statement deleted successfully"}
            
        except Exception as e:
            self.logger.error(f"Error deleting local statement: {str(e)}")
            return {"error": str(e)}

    def fetchProcessedStatements(self, userId, page=1, limit=100, filters=None):
        """Fetch processed email records from processedEmails table for the statements view."""
        from models.processedEmails import ProcessedEmails
        from sqlalchemy import desc, asc

        filters = filters or {}
        query = self.db.session.query(ProcessedEmails).filter_by(user_id=userId)

        # Default to bank_statement category — only show statement files
        # unless a specific category filter is provided
        bank_filter = filters.get("bank")
        if bank_filter:
            query = query.filter(ProcessedEmails.category == bank_filter)
        else:
            query = query.filter(ProcessedEmails.category == 'bank_statement')

        # Filter by fileName (search in subject)
        file_name_filter = filters.get("fileName")
        if file_name_filter:
            query = query.filter(ProcessedEmails.subject.ilike(f"%{file_name_filter}%"))

        # Date range filter
        date_range = filters.get("dateRange")
        if date_range:
            if date_range.get("dateFrom"):
                query = query.filter(ProcessedEmails.email_date >= date_range["dateFrom"])
            if date_range.get("dateTo"):
                query = query.filter(ProcessedEmails.email_date <= date_range["dateTo"])

        # Sort
        sorted_config = filters.get("sorted", {})
        sort_col_name = sorted_config.get("column", "processed_at")
        sort_dir = sorted_config.get("order", "desc")

        col_map = {
            "uploadDate": ProcessedEmails.processed_at,
            "processed_at": ProcessedEmails.processed_at,
            "fileName": ProcessedEmails.subject,
            "bank": ProcessedEmails.category,
            "email_date": ProcessedEmails.email_date,
        }
        sort_col = col_map.get(sort_col_name, ProcessedEmails.processed_at)
        order_fn = desc if sort_dir == "desc" else asc
        query = query.order_by(order_fn(sort_col))

        total = query.count()
        offset = (page - 1) * limit
        rows = query.offset(offset).limit(limit).all()

        # Batch-load statement periods for all gmail_ids in this page
        from models.statementPeriods import StatementPeriod
        gmail_ids = [row.gmail_id for row in rows if row.gmail_id]
        period_map = {}
        if gmail_ids:
            periods = self.db.session.query(StatementPeriod).filter(
                StatementPeriod.gmail_message_id.in_(gmail_ids),
                StatementPeriod.user == userId,
            ).all()
            for p in periods:
                period_map[p.gmail_message_id] = p

        results = []
        for row in rows:
            has_pdf = False
            if row.pdf_filename:
                full_path = os.path.join(os.getcwd(), "claude_statements", userId, row.pdf_filename)
                has_pdf = os.path.exists(full_path)

            # Extract bank from extraction_summary if available
            summary = row.extraction_summary or {}
            bank = summary.get("bank", row.category or "Unknown")

            # Build a human-readable summary string
            summary_text = ""
            items = row.items_extracted or 0
            if items > 0:
                summary_text = f"{items} item(s) extracted"
                if summary.get("inserted"):
                    summary_text = f"{summary['inserted']} inserted"
                if summary.get("duplicates"):
                    summary_text += f", {summary['duplicates']} duplicates"
            elif row.status == "skipped":
                summary_text = row.error_message or "Skipped"
            elif row.status == "failed":
                summary_text = row.error_message or "Processing failed"
            else:
                summary_text = "No items extracted"

            # Look up statement period coverage
            period_record = period_map.get(row.gmail_id)

            results.append({
                "fileID": f"pe_{row.gmail_id}",
                "fileName": row.subject or row.pdf_filename or row.gmail_id,
                "bank": bank,
                "uploadDate": row.processed_at.isoformat() if row.processed_at else "",
                "email_date": row.email_date.isoformat() if row.email_date else "",
                "statementCount": items,
                "category": row.category,
                "status": row.status,
                "items_extracted": items,
                "extraction_summary": summary,
                "summary_text": summary_text,
                "sender": row.sender,
                "subject": row.subject,
                "has_pdf": has_pdf,
                "user": userId,
                "period_start": period_record.period_start.isoformat() if period_record and period_record.period_start else None,
                "period_end": period_record.period_end.isoformat() if period_record and period_record.period_end else None,
            })

        return {
            "total_count": total,
            "page": page,
            "page_size": limit,
            "results": results,
        }

    def getProcessedEmailByGmailId(self, userId, gmail_id):
        """Look up a processedEmails row by gmail_id and user_id."""
        from models.processedEmails import ProcessedEmails
        try:
            return self.db.session.query(ProcessedEmails).filter_by(
                gmail_id=gmail_id, user_id=userId
            ).first()
        except Exception as e:
            self.logger.error(f"Error fetching processed email {gmail_id}: {str(e)}")
            return None

    def delete_email_transactions_for_period(self, user_id, bank, period_start, period_end):
        """Delete email-sourced transactions for a bank+period. Returns count deleted."""
        try:
            from sqlalchemy import func as sa_func
            deleted = self.db.session.query(Transactions).filter(
                Transactions.user == user_id,
                Transactions.bank == bank,
                sa_func.lower(Transactions.source) == 'email',
                Transactions.date.between(period_start, period_end),
            ).delete(synchronize_session='fetch')
            self.db.session.commit()
            self.logger.info(
                f"Deleted {deleted} email transactions for {bank} "
                f"({period_start} to {period_end})"
            )
            return deleted
        except Exception as e:
            self.db.session.rollback()
            self.logger.error(f"Error deleting email transactions: {str(e)}")
            return 0

    def fetchAllStatements(self, userId, include_legacy=True, include_local=True):
        """Fetch both legacy (Google Drive) and local statements"""
        try:
            result = {
                "legacy_statements": [],
                "local_statements": [],
                "total_count": 0
            }
            
            # Fetch legacy Google Drive statements
            if include_legacy:
                legacy_files = self.fetchFileDetails(page=1, filters={}, user_id=userId)
                result["legacy_statements"] = [
                    {**{key: value for key, value in fd.__dict__.items() if key != '_sa_instance_state'}, 
                     "storage_type": "google_drive"}
                    for fd in legacy_files["results"]
                ]
            
            # Fetch local Claude statements  
            if include_local:
                local_statements = self.fetchLocalStatements(userId)
                result["local_statements"] = local_statements.get("statements", [])
            
            result["total_count"] = len(result["legacy_statements"]) + len(result["local_statements"])
            
            return result
            
        except Exception as e:
            self.logger.error(f"Error fetching all statements: {str(e)}")
            return {"error": str(e)}
