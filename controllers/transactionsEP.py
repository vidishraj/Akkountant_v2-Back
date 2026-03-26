import threading
import uuid
from datetime import datetime
from enums.BanksEnum import BankEnums
from enums.ServiceTypeEnum import ServiceTypeEnum
from services.transactionsService import TransactionService
from utils.logger import Logger
from flask import request, jsonify, current_app

from flask import g


class TransactionController:
    TransactionService: TransactionService

    # In-memory scan progress tracker: scan_id -> progress dict
    _scans = {}
    _scans_lock = threading.Lock()

    def __init__(self, transactionService, mail_processor=None, reconciliation_service=None):
        self.TransactionService = transactionService
        self.mail_processor = mail_processor
        self.reconciliation_service = reconciliation_service
        self.logger = Logger(__name__).get_logger()

    @Logger.standardLogger
    def fetchTransactions(self):
        data = request.get_json(force=True)
        page = data.get("Page", 1)
        filters = data.get("Filter", None)
        user_id = g.get('firebase_id')
        self.logger.info(f"Fetch {page} with filter {filters}")
        transactions = self.TransactionService.fetchTransactions(page=page, filters=filters, user_id=user_id)
        # Format the transactions for JSON response
        results = []
        for t in transactions["results"]:
            transaction_dict = {}
            for key, value in t.__dict__.items():
                if key != '_sa_instance_state':
                    # Convert enum objects to their string values for JSON serialization
                    if hasattr(value, 'value'):  # Check if it's an enum
                        transaction_dict[key] = value.value
                    else:
                        transaction_dict[key] = value
            results.append(transaction_dict)
        response = {
            "total_count": transactions["count"],
            "page": page,
            "credit_sum": transactions["credit_sum"],
            "debit_sum": transactions["debit_sum"],
            "page_size": len(results),
            "results": results,
        }

        # Return the response
        return jsonify(response), 200

    def fetchOptedBanks(self):
        """
                Endpoint to fetch transaction and statement dates for the calendar view.
                """

        userId = g.get('firebase_id')
        # Ensure both `userID` and `optedBanks` are provided
        if not userId:
            return jsonify({"error": "userID are required"}), 400

        # Call the update_opted_banks service function
        result = self.TransactionService.fetchBanksOptedByUser(userId)
        if "error" in result:
            return jsonify(result), 404  # Return 404 if user not found
        return jsonify(result), 200  # Return 200 OK on success

    @Logger.standardLogger
    def fetchCalendarTransactions(self):
        """
        Endpoint to fetch transaction and statement dates for the calendar view.
        """
        data = request.get_json(force=True)
        month_start = data.get("monthStart")  # Expected in "yyyy-mm-dd" format
        month_end = data.get("monthEnd")  # Expected in "yyyy-mm-dd" format

        if not month_start or not month_end:
            return jsonify({"error": "Invalid or missing date range"}), 400

        self.logger.info(f"Fetching transactions for range: {month_start} - {month_end}")

        # Fetch transactions and statements from the service
        user_id = g.get('firebase_id')
        transactions = self.TransactionService.fetchTransactionDates(
            date_from=month_start, date_to=month_end, user_id=user_id
        )

        # Format the response
        response = {
            "transaction_dates": transactions.get("transaction_dates", []),
            "statement_dates": transactions.get("statement_dates", []),
            "covered_periods": transactions.get("covered_periods", []),
        }

        return jsonify(response), 200

    @Logger.standardLogger
    def triggerEmailCheck(self):
        """Trigger async email scan — returns scan_id immediately."""
        userId = request.headers.get("X-Firebase-ID")
        dateTo = request.args.get('dateTo')
        dateFrom = request.args.get('dateFrom')
        processing_mode = request.args.get('processing_mode', 'image')
        self.logger.info(f"Triggering async email scan for user {userId} ({dateFrom} to {dateTo})")

        if not self.mail_processor:
            return jsonify({"error": "Mail processor not configured"}), 500

        scan_id = str(uuid.uuid4())[:8]
        progress = {
            "status": "started",
            "stage": "initializing",
            "total_emails_fetched": 0,
            "emails_classified": 0,
            "text_emails_processed": 0,
            "pdf_emails_processed": 0,
            "text_emails_total": 0,
            "pdf_emails_total": 0,
            "pre_skipped": 0,
            "errors": [],
            "result": None,
        }

        with self._scans_lock:
            self._scans[scan_id] = progress

        flask_app = current_app._get_current_object()

        def run_scan():
            try:
                with flask_app.app_context():
                    g.db = flask_app.extensions.get("sqlalchemy")
                    g.firebase_id = userId

                    def on_progress(update):
                        with self._scans_lock:
                            self._scans[scan_id].update(update)

                    result = self.mail_processor.process_emails(
                        userId, dateFrom, dateTo, processing_mode,
                        progress_callback=on_progress,
                    )
                    with self._scans_lock:
                        self._scans[scan_id].update({
                            "status": "completed",
                            "stage": "done",
                            "result": result,
                        })
            except Exception as e:
                self.logger.error(f"Async scan {scan_id} failed: {e}", exc_info=True)
                with self._scans_lock:
                    self._scans[scan_id].update({
                        "status": "failed",
                        "stage": "error",
                        "errors": [str(e)],
                    })

        thread = threading.Thread(target=run_scan, daemon=True)
        thread.start()

        return jsonify({"scan_id": scan_id, "status": "started"}), 202

    @Logger.standardLogger
    def getEmailScanStatus(self):
        """GET /readEmails/status?scan_id=xxx — Poll scan progress."""
        scan_id = request.args.get('scan_id')
        if not scan_id:
            return jsonify({"error": "scan_id is required"}), 400

        with self._scans_lock:
            progress = self._scans.get(scan_id)

        if not progress:
            return jsonify({"error": "Scan not found"}), 404

        return jsonify(progress), 200

    @Logger.standardLogger
    def triggerStatementCheck(self):
        """Legacy endpoint — now redirects to unified mail processing pipeline."""
        userId = request.headers.get("X-Firebase-ID")
        dateTo = request.args.get('dateTo')
        dateFrom = request.args.get('dateFrom')
        processing_mode = request.args.get('processing_mode', 'image')
        self.logger.info(f"Reading statements for user {userId} via unified pipeline ({processing_mode} mode)")
        if not self.mail_processor:
            return jsonify({"error": "Mail processor not configured"}), 500
        result = self.mail_processor.process_emails(userId, dateFrom, dateTo, processing_mode)
        return jsonify({"Message": result}), 200

    @Logger.standardLogger
    def updateTransaction(self):
        data = request.get_json()
        reference_id = data.get("referenceID")
        updates = data.get("updates", {})
        user_id = g.get('firebase_id')
        # Validate that referenceID is provided
        if not reference_id:
            return jsonify({"error": "referenceID is required"}), 400
        # Call the update service function
        result = self.TransactionService.updateTransaction(reference_id, updates, user_id=user_id)
        if "error" in result:
            return jsonify(result), 404  # Return 404 if transaction not found
        return jsonify(result), 200  # Return 200 if update is successful

    @Logger.standardLogger
    def addUser(self):
        data = request.get_json()

        # Ensure `userID` is provided in the request data
        if 'userID' not in data:
            return jsonify({"error": "userID is required"}), 400

        # Call the add_user service function
        result = self.TransactionService.addUser(data)
        return jsonify(result), 201  # Return 201 Created on success

    @Logger.standardLogger
    def updateOptedBanks(self):
        data = request.get_json()
        user_id = data.get("userID")
        opted_banks = data.get("optedBanks")

        # Ensure both `userID` and `optedBanks` are provided
        if not user_id or opted_banks is None:
            return jsonify({"error": "userID and optedBanks are required"}), 400

        # Call the update_opted_banks service function
        result = self.TransactionService.updateOptedBanks(user_id, opted_banks)
        if "error" in result:
            return jsonify(result), 404  # Return 404 if user not found
        return jsonify(result), 200  # Return 200 OK on success

    @Logger.standardLogger
    def addUpdateUserToken(self):
        data = request.get_json()
        userId = g.get('firebase_id')
        # Ensure all required fields are present
        required_fields = ['access_token', 'refresh_token', 'client_id', 'client_secret', 'expiry',
                           'service_type']
        missing_fields = [field for field in required_fields if field not in data]
        if missing_fields or not userId:
            return jsonify({"error": f"Missing required fields: {', '.join(missing_fields)}"}), 400
        # Call the service function to add or update the token
        data['user_id'] = userId
        result = self.TransactionService.addUpdateUserToken(data)
        return jsonify(result), 200

    @Logger.standardLogger
    def deleteFile(self):
        """Enhanced to handle both legacy Google Drive and local statements"""
        userId = g.get('firebase_id')
        fileId = request.args.get('fileId')
        
        # Ensure all required fields are present
        if userId is None or fileId is None:
            return jsonify({"error": "Missing required fields"}), 400
            
        # Check if this is a local statement (fileId format: local_BANK_filename)
        if fileId.startswith('local_'):
            try:
                # Parse local file ID: local_BANK_filename.pdf
                parts = fileId.split('_', 2)  # Split into max 3 parts
                if len(parts) >= 3:
                    bank = parts[1]
                    filename = parts[2]
                    result = self.TransactionService.deleteLocalStatement(userId, bank, filename)
                    if "error" in result:
                        return jsonify(result), 404
                    return jsonify(result), 200
                else:
                    return jsonify({"error": "Invalid local file ID format"}), 400
            except Exception as e:
                return jsonify({"error": f"Failed to delete local statement: {str(e)}"}), 500
        else:
            # Legacy Google Drive deletion
            result = self.TransactionService.deleteFile(userId, fileId)
            return jsonify(result), 200

    @Logger.standardLogger
    def renameFile(self):
        """Enhanced to handle both legacy Google Drive and local statements"""
        data = request.get_json()

        # Ensure all required fields are present
        required_fields = ['user_id', 'fileId', 'newName']
        missing_fields = [field for field in required_fields if field not in data]
        if missing_fields:
            return jsonify({"error": f"Missing required fields: {', '.join(missing_fields)}"}), 400
            
        fileId = data['fileId']
        userId = data['user_id']
        newName = data['newName']
        
        # Check if this is a local statement (fileId format: local_BANK_filename)
        if fileId.startswith('local_'):
            # Note: Local statements cannot be renamed as they are stored based on email identifiers
            # This could be implemented if needed by moving files, but for now return not supported
            return jsonify({"error": "Renaming local Claude statements is not currently supported"}), 400
        else:
            # Legacy Google Drive rename
            result = self.TransactionService.renameFile(userId, fileId, newName)
            return jsonify(result), 200

    @Logger.standardLogger
    def downloadFile(self):
        """Handle download for processed email PDFs, local statements, and legacy Google Drive"""
        from flask import send_file as flask_send_file
        userId = g.get('firebase_id')
        fileId = request.args.get('fileId')

        if userId is None or fileId is None:
            return jsonify({"error": "Missing required fields"}), 400

        # Processed email PDF (fileId format: pe_GMAILID)
        if fileId.startswith('pe_'):
            gmail_id = fileId[3:]
            row = self.TransactionService.getProcessedEmailByGmailId(userId, gmail_id)
            if not row or not row.pdf_filename:
                return jsonify({"error": "PDF not available for this statement"}), 404
            import os
            file_path = os.path.join(os.getcwd(), "claude_statements", userId, row.pdf_filename)
            if not os.path.exists(file_path):
                return jsonify({"error": "PDF file not found on disk"}), 404
            return flask_send_file(file_path, as_attachment=True, download_name=os.path.basename(row.pdf_filename))

        # Local statement (fileId format: local_BANK_filename)
        if fileId.startswith('local_'):
            try:
                parts = fileId.split('_', 2)
                if len(parts) >= 3:
                    bank = parts[1]
                    filename = parts[2]
                    return self.downloadLocalStatement_internal(userId, bank, filename)
                else:
                    return jsonify({"error": "Invalid local file ID format"}), 400
            except Exception as e:
                return jsonify({"error": f"Failed to download local statement: {str(e)}"}), 500

        # Legacy Google Drive download
        result = self.TransactionService.downloadFile(userId, fileId)
        return result

    def downloadLocalStatement_internal(self, userId, bank, filename):
        """Internal method for downloading local statements"""
        from flask import send_file
        result = self.TransactionService.downloadLocalStatement(userId, bank, filename)
        if "error" in result:
            return jsonify(result), 404
            
        try:
            return send_file(result["file_path"], as_attachment=True, download_name=result["filename"])
        except Exception as e:
            return jsonify({"error": f"File download failed: {str(e)}"}), 500

    @Logger.standardLogger
    def fetchFileDetails(self):
        """Redirects to enhanced version that handles both legacy and local statements"""
        return self.fetchFileDetailsEnhanced()

    @Logger.standardLogger
    def checkGoogleApiStatus(self):
        userId = g.get('firebase_id')
        service = request.args.get('serviceType')
        self.logger.info(f"userID: {userId}")
        # Call the service function to add or update the token
        result = self.TransactionService.checkGoogleStatus(userId, ServiceTypeEnum[service.capitalize()])
        return jsonify(result), 200

    @Logger.standardLogger
    def setOptedBanks(self):
        try:
            data = request.json
            userId = request.headers.get("X-Firebase-ID")
            banks = data.get('banks')
            validBanks = list(banks.keys())
            # Validate input
            if not userId or not banks:
                return jsonify({"error": "userID and banks are required"}), 400

            if not all(bank in BankEnums.__members__ for bank in validBanks):
                return jsonify({"error": "Invalid bank(s) provided"}), 400

            # Call the service method
            updated_user = self.TransactionService.setOptedBanks(userId, banks)
            if updated_user is None:
                return jsonify({"error": "User not found"}), 404

            return jsonify({"message": "Opted banks updated successfully"}), 200
        except Exception as e:
            return jsonify({"error": str(e)}), 500

    @Logger.standardLogger
    def getProcessingStats(self):
        userId = g.get('firebase_id')
        stats = self.TransactionService.getProcessingStats(userId)
        return jsonify(stats), 200


    @Logger.standardLogger
    def updatePersonalInfo(self):
        """POST /updatePersonalInfo — Save/update user personal info for PDF password unlocking."""
        from models.userPersonalInfo import UserPersonalInfo
        data = request.get_json(force=True)
        user_id = g.get("firebase_id")

        # Parse date_of_birth string (DD/MM/YYYY) to date object
        dob_str = data.get("date_of_birth")
        dob_date = None
        if dob_str:
            try:
                dob_date = datetime.strptime(dob_str, "%d/%m/%Y").date()
            except ValueError:
                dob_date = None

        try:
            existing = g.db.session.query(UserPersonalInfo).filter_by(user_id=user_id).first()
            if existing:
                for field in ["first_name", "last_name", "pan_number",
                              "phone_number", "phone_number_2", "uan_number", "customer_id_hdfc"]:
                    if field in data:
                        setattr(existing, field, data[field])
                if "date_of_birth" in data:
                    existing.date_of_birth = dob_date
            else:
                info = UserPersonalInfo(
                    user_id=user_id,
                    first_name=data.get("first_name"),
                    last_name=data.get("last_name"),
                    date_of_birth=dob_date,
                    pan_number=data.get("pan_number"),
                    phone_number=data.get("phone_number"),
                    phone_number_2=data.get("phone_number_2"),
                    uan_number=data.get("uan_number"),
                    customer_id_hdfc=data.get("customer_id_hdfc"),
                )
                g.db.session.add(info)
            g.db.session.commit()
            return jsonify({"message": "Personal info updated successfully"}), 200
        except Exception as e:
            g.db.session.rollback()
            return jsonify({"error": str(e)}), 500

    @Logger.standardLogger
    def getPersonalInfo(self):
        """GET /getPersonalInfo — Fetch stored personal info (masked for security)."""
        from models.userPersonalInfo import UserPersonalInfo
        user_id = g.get("firebase_id")

        info = g.db.session.query(UserPersonalInfo).filter_by(user_id=user_id).first()
        if not info:
            return jsonify({"exists": False}), 200

        # Return masked versions for display
        def mask(val, show=2):
            if not val:
                return None
            if len(val) <= show:
                return val
            return val[:show] + "*" * (len(val) - show)

        return jsonify({
            "exists": True,
            "first_name": info.first_name,
            "last_name": info.last_name,
            "date_of_birth": mask(info.date_of_birth.strftime("%d/%m/%Y"), 5) if info.date_of_birth else None,
            "pan_number": mask(info.pan_number, 4) if info.pan_number else None,
            "phone_number": mask(info.phone_number, 4) if info.phone_number else None,
            "phone_number_2": mask(info.phone_number_2, 4) if info.phone_number_2 else None,
            "uan_number": mask(info.uan_number, 4) if info.uan_number else None,
            "customer_id_hdfc": mask(info.customer_id_hdfc, 3) if info.customer_id_hdfc else None,
        }), 200

    @Logger.standardLogger
    def processMailPipeline(self):
        """POST /processMailPipeline — Trigger unified AI email processing pipeline."""
        data = request.get_json(force=True)
        date_from = data.get("date_from")
        date_to = data.get("date_to")
        processing_mode = data.get("processing_mode", "image")
        user_id = g.get("firebase_id")

        if not self.mail_processor:
            return jsonify({"error": "Mail processor not configured"}), 500

        result = self.mail_processor.process_emails(user_id, date_from, date_to, processing_mode)
        return jsonify(result), 200

    @Logger.standardLogger
    def reprocessPdf(self):
        """POST /reprocessPdf — Reprocess a saved PDF directly (bypasses email fetch).

        Body: {
            "gmail_id": "19cd534052ce39a8",
            "pdf_path": "/path/to/saved.pdf",   (optional — auto-resolved from claude_statements)
            "password": "...",                   (optional — auto-tried from personal info)
            "processing_mode": "text"            (optional — default "text")
        }
        """
        data = request.get_json(force=True)
        gmail_id = data.get("gmail_id")
        user_id = g.get("firebase_id")
        processing_mode = data.get("processing_mode", "text")
        password = data.get("password")
        pdf_path = data.get("pdf_path")
        bank = data.get("bank", "HDFC_DEBIT")
        only_chunks = data.get("only_chunks")  # e.g. [[31,32],[11,12]]

        if not gmail_id:
            return jsonify({"error": "gmail_id is required"}), 400

        if not self.mail_processor:
            return jsonify({"error": "Mail processor not configured"}), 500

        result = self.mail_processor.reprocess_pdf(
            user_id, gmail_id, pdf_path=pdf_path,
            password=password, processing_mode=processing_mode,
            bank=bank, only_chunks=only_chunks,
        )
        return jsonify(result), 200

    # ── Reconciliation Endpoints ─────────────────────────────────────

    @Logger.standardLogger
    def reconcile(self):
        """POST /reconcile — Trigger cross-instrument reconciliation for a period."""
        if not self.reconciliation_service:
            return jsonify({"error": "Reconciliation service not configured"}), 500

        data = request.get_json(force=True)
        period_start = data.get("period_start")
        period_end = data.get("period_end")
        user_id = g.get("firebase_id")

        if not period_start or not period_end:
            return jsonify({"error": "period_start and period_end are required"}), 400

        from datetime import datetime
        try:
            ps = datetime.strptime(period_start, "%Y-%m-%d").date()
            pe = datetime.strptime(period_end, "%Y-%m-%d").date()
        except ValueError:
            return jsonify({"error": "Dates must be in YYYY-MM-DD format"}), 400

        result = self.reconciliation_service.reconcile_transfers(user_id, ps, pe)
        return jsonify(result), 200

    @Logger.standardLogger
    def zeroSumReport(self):
        """POST /zeroSumReport — Compute and return zero-sum verification report."""
        if not self.reconciliation_service:
            return jsonify({"error": "Reconciliation service not configured"}), 500

        data = request.get_json(force=True)
        period_start = data.get("period_start")
        period_end = data.get("period_end")
        user_id = g.get("firebase_id")

        if not period_start or not period_end:
            return jsonify({"error": "period_start and period_end are required"}), 400

        from datetime import datetime
        try:
            ps = datetime.strptime(period_start, "%Y-%m-%d").date()
            pe = datetime.strptime(period_end, "%Y-%m-%d").date()
        except ValueError:
            return jsonify({"error": "Dates must be in YYYY-MM-DD format"}), 400

        result = self.reconciliation_service.compute_zero_sum_report(user_id, ps, pe)
        return jsonify(result), 200

    @Logger.standardLogger
    def statementPeriods(self):
        """GET /statementPeriods — List all tracked statement periods for user."""
        if not self.reconciliation_service:
            return jsonify({"error": "Reconciliation service not configured"}), 500

        user_id = g.get("firebase_id")
        bank = request.args.get("bank")

        periods = self.reconciliation_service.get_statement_periods(user_id, bank)
        return jsonify({"periods": periods, "count": len(periods)}), 200

    @Logger.standardLogger
    def reconciliationStatus(self):
        """GET /reconciliationStatus — Check coverage and reconciliation state for a period."""
        if not self.reconciliation_service:
            return jsonify({"error": "Reconciliation service not configured"}), 500

        user_id = g.get("firebase_id")
        period_start = request.args.get("period_start")
        period_end = request.args.get("period_end")

        if not period_start or not period_end:
            return jsonify({"error": "period_start and period_end query params are required"}), 400

        from datetime import datetime
        try:
            ps = datetime.strptime(period_start, "%Y-%m-%d").date()
            pe = datetime.strptime(period_end, "%Y-%m-%d").date()
        except ValueError:
            return jsonify({"error": "Dates must be in YYYY-MM-DD format"}), 400

        result = self.reconciliation_service.get_reconciliation_status(user_id, ps, pe)
        return jsonify(result), 200

    @Logger.standardLogger
    def fetchFileDetailsEnhanced(self):
        """Fetch processed email statements from DB with pagination and filters"""
        data = request.get_json(force=True)
        page = data.get("Page", 1)
        filters = data.get("Filter", {})
        userId = g.get('firebase_id')

        self.logger.info(f"Fetch FileDetails Page {page} with filter {filters}")

        limit = filters.get("limit", 100) if filters else 100
        result = self.TransactionService.fetchProcessedStatements(userId, page=page, limit=limit, filters=filters)
        return jsonify(result), 200
