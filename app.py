import datetime
import logging
import os

from flask import Flask, g, request, jsonify
from flask_sqlalchemy import SQLAlchemy
from dotenv import load_dotenv
from flask_sqlalchemy.session import Session
from sqlalchemy import inspect, text
from flask_cors import CORS
from sqlalchemy.exc import SQLAlchemyError

from controllers.investmentsEP import InvestmentController
from controllers.transactionsEP import TransactionController
from controllers.customerEP import CustomerController
from controllers.invoiceEP import InvoiceController
from controllers.dashboardEP import DashboardController
from controllers.pdfEP import PDFController
from controllers.templateEP import TemplateController
from controllers.signatureEP import SignatureController
from controllers.paymentEP import PaymentController
from controllers.customFieldEP import CustomFieldController
from controllers.jobsEP import JobsController
from controllers.agentEP import AgentController
from controllers.agentConversationsEP import AgentConversationsController  # ak-bq5
from controllers.customerEmailEP import CustomerEmailController
from controllers.fileStorageEP import FileStorageController
from controllers.jobEmailEP import JobEmailController
from controllers.portfolioVisitorEP import PortfolioVisitorController
from controllers.wealthDigestEP import WealthDigestController  # ak-5vg
from controllers.userFilesEP import UserFilesController  # ak-9dz
from controllers.reconciliationEP import ReconciliationController  # ak-cipj
from controllers.watchlistEP import WatchlistController  # ak-05af
from controllers.kiteLtpEP import KiteLtpController  # ak-vue6
from enums.TaskStatusEnum import JobStatus
from services.InvestmentService import InvestmentService
from services.tasks.scheduler import TaskScheduler
from services.transactionsService import TransactionService
from services.customerService import CustomerService
from services.invoiceService import InvoiceService
from services.dashboardService import DashboardService
from services.pdfService import PDFService
from services.templateService import TemplateService
from services.signatureService import SignatureService
from services.paymentService import PaymentService
from services.customFieldService import CustomFieldService
from services.customerEmailService import CustomerEmailService
from services.fileStorageService import FileStorageService
from services.jobEmailService import JobEmailService
from services.agentService import AgentService
from services.agentConversationService import AgentConversationService  # ak-bq5
from services.wealthDigestService import WealthDigestService  # ak-5vg
from services.agentFileAttachmentService import AgentFileAttachmentService  # ak-9dz
from services.cronAgent import CronAgent
from services.portfolioVisitorService import PortfolioVisitorService
from services.mailProcessorService import MailProcessorService
from services.reconciliationService import ReconciliationService
from services.watchlistService import WatchlistService  # ak-05af
from services.tasks.checkMailUnifiedTask import CheckMailUnifiedTask
from utils.logger import Logger
import models


class Akkountant(Flask):
    """Main application class for Akkountant."""
    db: SQLAlchemy
    logger: logging.Logger
    scheduler: TaskScheduler
    transactionEP: TransactionController
    transactionService: TransactionService

    def __init__(self, import_name: str, test_config: dict = None):
        load_dotenv()
        super().__init__(import_name)
        self.app = self.app_context().app

        # Initialize logger
        self.logger = Logger(__name__).get_logger()
        self.logger.info("Starting Akkountant")
        if test_config:
            self.config.update(test_config)

        # Set up application components
        self._setup_config()
        self._setup_database()
        self._setup_instances()
        self._setup_schedulers()

        # Update db from dump file
        filename = "akkountV2.sql"  # Update dump file name here
        folder_path = os.getcwd() + '/tmp/'  # put file in tmp folder
        # self.updateFromDump(filename, folder_path)

        # Run async methods in setup
        # self._setup_investments()  # Run async setup
        self._setup_routes()
        self._setup_hooks()

        CORS(self)
        self.logger.info("Akkountant initialization complete.")

    def _setup_config(self):
        """Set up configuration."""
        self.config['SQLALCHEMY_DATABASE_URI'] = os.getenv('DATABASE_URL')
        self.config['SQLALCHEMY_TRACK_MODIFICATIONS'] = False
        # ak-1x4 reviewer fix: do NOT set a global MAX_CONTENT_LENGTH —
        # it would also cap legitimate file-storage / signature / bank-
        # statement uploads on other endpoints which were previously
        # unlimited. The /agent/attach handler enforces its own
        # Content-Length check up-front and streams with a hard byte
        # ceiling in save_upload (see utils/agent_attachments.py),
        # so per-route enforcement is sufficient without dragging the
        # global cap along.

    def updateFromDump(self, file_name, folder_path):
        """
        Update the database using a dump file.

        :param file_name: Name of the dump file to process.
        :param folder_path: Path to the folder containing the dump files.
        """
        # Construct the full path to the dump file
        file_path = os.path.join(folder_path, file_name)

        if not os.path.exists(file_path):
            self.logger.error(f"The file '{file_name}' does not exist in '{folder_path}'.")
            return

        # Read the contents of the SQL dump file
        with open(file_path, 'r') as file:
            sql_commands = file.read()

        # Execute the SQL commands in the dump file
        try:
            with self.app_context():
                with Session(self.db) as session:
                    for command in sql_commands.split(';'):  # Split commands by ';'
                        if command.strip():  # Skip empty commands
                            session.execute(text(command.strip()))
                    session.commit()
                    self.logger.info(f"Database successfully updated using the dump file: {file_name}")
        except Exception as e:
            self.logger.error(f"An error occurred while updating the database: {e}")

    # ak-htnb (2026-11-23): file-lock guarding first-boot DDL from
    # the concurrent-gunicorn-worker race. See
    # ``_setup_database`` for the full root-cause write-up.
    #
    # Chosen BLOCKING (not LOCK_NB) so every worker waits for the
    # lock-holder — all workers finish with a consistent healthy DB
    # view. The scheduler lock at ``_setup_schedulers`` uses
    # LOCK_NB-with-skip (correct for daemon exclusivity); the DDL
    # guard here uses LOCK_EX-with-wait (correct for "all workers
    # must observe the created schema").
    _DB_INIT_LOCK_PATH = '/tmp/akkountant_db_init.lock'

    def _setup_database(self):
        """Set up database connection and create tables.

        ak-htnb (2026-11-23) — root cause analysis of the
        2026-11-22 P0 first-boot-after-DDL crash:

        gunicorn.conf.py runs ``workers = 2`` without
        ``preload_app = True``. Default gunicorn behavior is to
        fork each worker as its own process + each independently
        imports + boots the Flask app, meaning BOTH workers call
        ``_setup_database()`` → ``self.db.create_all()``
        concurrently on fresh starts.

        Pre-ak-htnb, ``create_all()`` had NO cross-worker
        serialization. With a fresh DDL batch (e.g. ak-kxdr +
        ak-l8a7 + ak-05af introducing new tables / constraints
        on 2026-11-22), the two workers race:

          * Worker A: SELECT information_schema (table X absent)
                      → BEGIN → CREATE TABLE X → COMMIT. OK.
          * Worker B (concurrent): SELECT shows absent → BEGIN
                      → CREATE TABLE X → MySQL raises "Table
                      exists" / metadata-lock contention → worker
                      exits code=3.

        systemd's ``Restart=on-failure`` fires → fresh master
        → sequential worker boot (the first worker completes
        the DDL before the second one checks) → observed
        "second-boot succeeded with no error logged". This
        matches the recorded journal signature verbatim.

        Secondary P0 impact: the unintentional restart lifted
        ffdae30 (ak-c8l Firebase middleware) into memory WITHOUT
        its accompanying env vars → 30-min silent 401 cascade
        until the Overseer noticed. Had the first boot not
        crashed, the restart wouldn't have happened, and the
        middleware wouldn't have activated prematurely.

        Fix: fcntl advisory lock around the DDL window. BLOCKING
        acquire so Worker B waits for A to finish rather than
        skipping — once released, B's create_all() is a clean
        no-op via SQLAlchemy's checkfirst=True reflection. Both
        workers finish with the same consistent schema view.

        Belt-and-brace (not either-or): the correct long-term
        fix is ``preload_app = True`` in gunicorn.conf.py (makes
        the master boot the app ONCE before forking). Filing
        that as a separate P2 bead so this hotfix is the
        minimal-LOC immediate protection. See ak-htnb bead
        comment for the preload-app validation plan.
        """
        self.db = SQLAlchemy(self, model_class=models.Base)
        lock_fd = None
        try:
            # Acquire the DDL lock BEFORE entering app_context so a
            # failure to open the lock file surfaces immediately (bad
            # perms on /tmp, disk full, etc) rather than after a
            # partial SQLAlchemy bind.
            import fcntl
            lock_fd = open(self._DB_INIT_LOCK_PATH, 'w')
            fcntl.flock(lock_fd, fcntl.LOCK_EX)  # BLOCKING — see docstring
            with self.app_context():
                self.logger.info(
                    "Creating database tables if not exist "
                    "(DDL lock held)."
                )
                inspector = inspect(self.db.engine)

                existing_tables_before = inspector.get_table_names()
                self.db.create_all()

                self._insert_initial_jobs("SetNPSRate", "Pending", "High", datetime.datetime.now())
                self._insert_initial_jobs("SetNPSDetails", "Pending", "High", datetime.datetime.now())
                self._insert_initial_jobs("SetStocksOldDetails", "Pending", "High", datetime.datetime.now())
                self._insert_initial_jobs("SetStocksDetails", "Pending", "High", datetime.datetime.now())
                self._insert_initial_jobs("SetMFRate", "Pending", "High", datetime.datetime.now())
                self._insert_initial_jobs("SetMFDetails", "Pending", "High", datetime.datetime.now())
                self._insert_initial_jobs("SetGoldRate", "Pending", "High", datetime.datetime.now())
                self._insert_initial_jobs("SetPPFRate", "Pending", "High", datetime.datetime.now())
                existing_tables_after = inspector.get_table_names()
                new_tables = set(existing_tables_after) - set(existing_tables_before)
                if new_tables:
                    self.logger.info(f"New tables created: {new_tables}")
                else:
                    self.logger.info("No new tables created.")
        finally:
            # Lock release is automatic on fd close; be explicit for
            # clarity. If acquire failed, lock_fd is None → safe skip.
            if lock_fd is not None:
                try:
                    import fcntl as _fcntl
                    _fcntl.flock(lock_fd, _fcntl.LOCK_UN)
                except (OSError, ValueError):
                    pass
                try:
                    lock_fd.close()
                except OSError:
                    pass

    def _setup_instances(self):
        """Initialize application instances."""
        self.transactionService = TransactionService()
        self.investmentService = InvestmentService()
        self.investmentEP = InvestmentController(self.investmentService)
        self.customerService = CustomerService()
        self.customerEP = CustomerController(self.customerService)
        self.invoiceService = InvoiceService()
        self.invoiceEP = InvoiceController(self.invoiceService)
        self.dashboardService = DashboardService()
        self.dashboardEP = DashboardController(self.dashboardService)
        self.pdfService = PDFService()
        self.pdfEP = PDFController(self.pdfService, self.invoiceService)
        self.templateService = TemplateService()
        self.templateEP = TemplateController(self.templateService)
        self.signatureService = SignatureService()
        self.signatureEP = SignatureController(self.signatureService)
        self.paymentService = PaymentService()
        self.paymentEP = PaymentController(self.paymentService)
        self.customFieldService = CustomFieldService()
        self.customFieldEP = CustomFieldController(self.customFieldService)
        self.customerEmailService = CustomerEmailService()
        self.customerEmailEP = CustomerEmailController(self.customerEmailService)
        self.fileStorageService = FileStorageService()
        self.fileStorageEP = FileStorageController(self.fileStorageService)
        self.jobsEP = JobsController(flask_app=self)
        self.jobEmailService = JobEmailService()
        self.jobEmailEP = JobEmailController(self.jobEmailService)
        self.reconciliationService = ReconciliationService()
        self.mailProcessor = MailProcessorService(
            flask_app=self,
            transaction_service=self.transactionService,
            investment_service=self.investmentService,
            invoice_service=self.invoiceService,
            reconciliation_service=self.reconciliationService,
        )
        self.transactionEP = TransactionController(
            self.transactionService,
            mail_processor=self.mailProcessor,
            reconciliation_service=self.reconciliationService,
        )
        # ak-bq5: cross-device chat persistence — instantiate the
        # conversation service BEFORE wiring it into AgentService so
        # set_services receives a ready instance.
        self.agentConversationService = AgentConversationService()
        # ak-9dz: file-download plumbing for agent-produced files (CSV
        # exports, PDFs, etc). Backs the shared `attach_file_to_chat`
        # MCP tool + GET /api/user-files/<uuid> endpoint. Instantiate
        # BEFORE AgentService.set_services so the wire-up sees a ready
        # instance.
        self.agentFileAttachmentService = AgentFileAttachmentService()
        self.agentService = AgentService()
        self.agentService.set_services(
            investment_service=self.investmentService,
            transaction_service=self.transactionService,
            invoice_service=self.invoiceService,
            customer_service=self.customerService,
            dashboard_service=self.dashboardService,
            mail_processor=self.mailProcessor,
            conversation_service=self.agentConversationService,  # ak-bq5
            file_attachment_service=self.agentFileAttachmentService,  # ak-9dz
        )
        self.agentEP = AgentController(self.agentService)
        self.agentConversationsEP = AgentConversationsController(  # ak-bq5
            self.agentConversationService,
        )
        self.userFilesEP = UserFilesController(  # ak-9dz
            self.agentFileAttachmentService,
        )
        # ak-5vg: read-side WealthDigest endpoints for the dedicated
        # page (ak-hqm frontend companion). Wires to the same
        # AgentConversation store the WealthDigestTask writes to.
        self.wealthDigestService = WealthDigestService()
        self.wealthDigestEP = WealthDigestController(self.wealthDigestService)
        self.portfolioVisitorService = PortfolioVisitorService()
        self.portfolioVisitorEP = PortfolioVisitorController(self.portfolioVisitorService)
        # ak-cipj: trade-reconciliation read + resolve surface. Wires
        # the existing ReconcileTradesTask (ak-zai2) write-only
        # pipeline through to Overseer. Reuses the already-instantiated
        # ReconciliationService above (L194); no duplicate instance.
        self.reconciliationEP = ReconciliationController(
            self.reconciliationService,
        )
        # ak-05af: persistent watchlist CRUD + LTP-batch surface.
        # Category C #16 of the Kite capability roadmap. Wires
        # the WatchlistService with KiteService (via StocksService)
        # for the LTP batch — ``/watchlist/ltp`` hits Kite on behalf
        # of the user; CRUD endpoints are pure DB. Reuses the
        # existing InvestmentService singleton for the Kite login
        # URL on auth-error reconnect payload.
        self.watchlistService = WatchlistService(
            kite_service=self.investmentService.StockService.kite_service,
        )
        self.watchlistEP = WatchlistController(
            self.watchlistService,
            investment_service=self.investmentService,
        )
        # ak-vue6 Phase 1 (2026-11-23): Kite LTP SSE + single-symbol
        # endpoints. Flask-side CLIENT of the standalone
        # ``ak-kite-ws.service`` daemon (UNIX-socket IPC at
        # /run/akkountant/kite-ltp.sock). The daemon owns the Kite
        # WebSocket; app.service only reads snapshots. Clean failure
        # when the daemon is down: 503 on single-symbol, SSE emits
        # ``reconnect_required`` + closes.
        self.kiteLtpEP = KiteLtpController()

    def _setup_schedulers(self):
        """Set up background daemon threads (CronAgent, mail, snapshots).
        TaskScheduler is run separately via schedular.service.
        Uses a file lock so only one gunicorn worker starts these threads.

        ak-htnb (2026-11-23): this method header was accidentally
        dropped during the ak-vue6 Edit that inserted
        `self.kiteLtpEP = KiteLtpController()` at the tail of
        ``_setup_instances``. ``__init__`` line 87 calls
        ``self._setup_schedulers()`` so the next app.service restart
        would AttributeError — a P0 boot failure. Restored here with
        the full prior method body unchanged. Belt-and-brace: this
        comment makes the regression + cause greppable so a reviewer
        running a future similar Edit catches it.

        Discovered during the ak-htnb investigation (same class of
        boot-time failure as the first-boot-DDL race that prompted
        the bead); folded into this hotfix per lead directive.
        """
        if os.getenv('ENV') == 'LOCAL':
            self.logger.info("Skipping schedulers in LOCAL environment.")
            return

        # ak-dzla (2026-11-23): standalone-unit early-exit. The
        # ak-kite-ws.service entrypoint imports this Akkountant
        # class to bootstrap Flask app context (DB session +
        # config), but MUST NOT spin up the scheduler daemons —
        # app.service already owns CronAgent / mail / snapshot.
        # The fcntl lock below is per-process-group scope so it
        # does NOT prevent cross-service duplication; without this
        # env gate ak-kite-ws spawns a SECOND set of daemons
        # (observed 2026-11-23 post-ak-uvjk install: journalctl
        # showed 2x snapshot thread → §2 accounting-correctness
        # risk of duplicated InvestmentSnapshot rows at 11PM IST).
        # Pair: deploy/ak-kite-ws.service sets AK_SKIP_SCHEDULERS=1
        # via Environment=; drift guard in test_ak_skip_
        # schedulers_env_gate.py pins both ends.
        if os.getenv('AK_SKIP_SCHEDULERS') == '1':
            self.logger.info(
                "Skipping schedulers: AK_SKIP_SCHEDULERS=1 "
                "(standalone unit — app.service owns schedulers)."
            )
            return

        lock_path = '/tmp/akkountant_scheduler.lock'
        try:
            self._scheduler_lock_fd = open(lock_path, 'w')
            import fcntl
            fcntl.flock(self._scheduler_lock_fd, fcntl.LOCK_EX | fcntl.LOCK_NB)
        except (IOError, OSError):
            self.logger.info("Another worker owns the scheduler lock, skipping daemon threads.")
            return

        with self.app_context():
            # 1. Start CronAgent (AI decides what to refresh)
            self.cron_agent = CronAgent(
                flask_app=self,
                investment_service=self.investmentService,
                interval_seconds=1800,
            )
            self.cron_agent.start()
            self.logger.info("CronAgent started (30-min interval).")

            # 2. Start CheckMailUnifiedTask (AI email processing)
            self.mail_cron = CheckMailUnifiedTask(
                flask_app=self,
                mail_processor=self.mailProcessor,
                interval_seconds=3600,
            )
            self.mail_cron.start()
            self.logger.info("CheckMailUnifiedTask started (1-hour interval).")

            # 3. Start InvestmentSnapshotTask (daily at 11PM IST)
            from services.tasks.investmentSnapshotTask import InvestmentSnapshotTask
            self.snapshot_task = InvestmentSnapshotTask(
                flask_app=self,
                investment_service=self.investmentService,
            )
            self.snapshot_task.start()
            self.logger.info("InvestmentSnapshotTask started (daily at 11PM IST).")

    def _insert_initial_jobs(self, title, status, priority, due_date, user_id=None):
        try:
            # Check if a job with the same title and status exists
            existing_job = self.db.session.query(models.Job).filter(models.Job.status.in_([JobStatus.OVERDUE.value,
                                                                                           JobStatus.PENDING.value])).filter_by(title=title).first()

            if existing_job:
                return False  # Job already exists

            # Insert a new job, clamped to the 1-7AM IST window
            from utils.DateTimeUtil import clamp_to_allowed_window
            new_job = models.Job(
                title=title,
                status=status,
                priority=priority,
                due_date=clamp_to_allowed_window(due_date),
                user_id=user_id,
                result=None,
            )
            self.db.session.add(new_job)
            self.db.session.commit()
            return True  # Job inserted successfully
        except SQLAlchemyError as e:
            self.db.session.rollback()
            self.logger.error(f"Error inserting job: {e}")
            return False

    @staticmethod
    def _setup_investments():
        """Read from the JSON file, call the JSON service individually for rates and lists."""
        pass

    def _setup_routes(self):
        """Define application routes."""
        transactionRoutes = [
            ('/fetchTransactions', 'POST', self.transactionEP.fetchTransactions),
            ('/fetchOptedBanks', 'GET', self.transactionEP.fetchOptedBanks),
            ('/calendarTransactions', 'POST', self.transactionEP.fetchCalendarTransactions),
            ('/readEmails', 'GET', self.transactionEP.triggerEmailCheck),
            ('/readEmails/status', 'GET', self.transactionEP.getEmailScanStatus),
            ('/readStatements', 'GET', self.transactionEP.triggerStatementCheck),
            ('/getFileDetails', 'POST', self.transactionEP.fetchFileDetails),
            ('/getGoogleStatus', 'GET', self.transactionEP.checkGoogleApiStatus),
            ('/updateGoogleTokens', 'POST', self.transactionEP.addUpdateUserToken),
            ('/setOptedBanks', 'POST', self.transactionEP.setOptedBanks),
            # ak-3rwh: statement-PDF passwords (status only / set one bank).
            ('/statementPasswords', 'GET', self.transactionEP.getStatementPasswords),
            ('/statementPasswords/<bank>', 'PUT', self.transactionEP.putStatementPassword),
            ('/downloadFile', 'GET', self.transactionEP.downloadFile),
            ('/deleteFile', 'GET', self.transactionEP.deleteFile),
            ('/processMailPipeline', 'POST', self.transactionEP.processMailPipeline),
            ('/reprocessPdf', 'POST', self.transactionEP.reprocessPdf),
            # ak-ep4i: transaction drawer — category (+ merchant rule),
            # note, and the category picker list. uid-scoped; 404 for
            # another user's / unknown referenceID.
            ('/transactions/categories', 'GET', self.transactionEP.listTransactionCategories),
            ('/transactions/<reference_id>/category', 'PUT', self.transactionEP.setTransactionCategory),
            ('/transactions/<reference_id>/note', 'PUT', self.transactionEP.setTransactionNote),
            ('/statementAttention', 'GET', self.transactionEP.statementAttention),
            ('/retryStatementEmail', 'POST', self.transactionEP.retryStatementEmail),
            ('/updatePersonalInfo', 'POST', self.transactionEP.updatePersonalInfo),
            ('/getPersonalInfo', 'GET', self.transactionEP.getPersonalInfo),
            ('/reconcile', 'POST', self.transactionEP.reconcile),
            ('/zeroSumReport', 'POST', self.transactionEP.zeroSumReport),
            ('/statementPeriods', 'GET', self.transactionEP.statementPeriods),
            ('/reconciliationStatus', 'GET', self.transactionEP.reconciliationStatus),
            # ak-cipj: trade-reconciliation read + resolve. Surfaces
            # ReconciliationIssue rows (written by ReconcileTradesTask
            # ak-zai2) to the Overseer so the Home-card badge + review
            # page have real data. List + resolve are user_id-scoped
            # via g.firebase_id; resolve is idempotent (double-resolve
            # = noop 200 with idempotent=True, first-set resolved_at
            # preserved).
            ('/reconciliation/issues', 'GET', self.reconciliationEP.listIssues),
            (
                '/reconciliation/issues/<issue_id>/resolve', 'POST',
                self.reconciliationEP.resolveIssue,
            ),
            # ak-ex2: post-fallback non-savings row stripper. Called by
            # infra during the ak-32o HDFC re-parse cycle for each file
            # tagged reconciliation_fallback=True by ak-ifc-v3.
            ('/admin/stripFallbackRows', 'POST', self.transactionEP.stripFallbackRows),
        ]

        for rule, method, view_func in transactionRoutes:
            self.add_url_rule(rule, methods=[method], view_func=view_func)

        investmentRoutes = [
            ('/fetchSecurityList', 'GET', self.investmentEP.fetchSecurityList),
            ('/fetchSecurityScheme', 'GET', self.investmentEP.fetchSecurityRate),
            ('/uploadSecuritiesFile', 'POST', self.investmentEP.process_file_upload),
            ('/fetchSummary', 'GET', self.investmentEP.fetchSummary),
            ('/fetchSecurityTransactions', 'GET', self.investmentEP.fetchSecurityTransactions),
            ('/fetchUserSecurities', 'GET', self.investmentEP.fetchUserSecurities),
            ('/insertSecurityTransaction', 'POST', self.investmentEP.insertSecurityTransaction),
            ('/fetchCompleteEPG', 'GET', self.investmentEP.fetchCompleteDataForEPG),
            ('/fetchRates', 'GET', self.investmentEP.fetchRateForEPG),
            ('/getsJobs', 'GET', self.investmentEP.getJobsTable),
            ('/startJob', 'GET', self.investmentEP.setJobs),
            ('/fetchTimeStamps', 'GET', self.investmentEP.fetchTimeStamps),
            ('/fetchRealizedPnL', 'GET', self.investmentEP.fetchRealizedPnL),
            ('/deleteSingleInvestment', 'GET', self.investmentEP.deleteSingleRecord),
            ('/fetchFOSummary', 'GET', self.investmentEP.fetchFOSummary),
            ('/fetchFOTrades', 'GET', self.investmentEP.fetchFOTrades),
            ('/deleteAllInvestments', 'DELETE', self.investmentEP.deleteAllInvestments),
            # Kite Connect API endpoints
            ('/fetchInvestmentEmails', 'GET', self.investmentEP.fetchInvestmentEmails),
            ('/fetchEmailBody', 'GET', self.investmentEP.fetchEmailBody),
            ('/fetchInvestmentSnapshots', 'GET', self.investmentEP.fetchInvestmentSnapshots),
            # ak-4o41 Bundle 1 (2026-11-12, supersedes ak-874): N-day
            # portfolio trend for WealthDigest PortfolioTrendChart,
            # aggregated from Kite historical-candle cache.
            ('/investment-snapshot/history', 'GET', self.investmentEP.fetchPortfolioHistory),
            # ak-ue7l (2026-11-22): per-stock OHLC candles for the
            # FE candlestick / line chart on MSNDetail (Stock view).
            # Serves from the local stockHistoricalCandles cache
            # populated daily by BackfillHistoricalCandlesTask
            # (ak-4o41) — ZERO Kite calls per chart render.
            # READ-ONLY.
            (
                '/investments/stock/<security_code>/candles', 'GET',
                self.investmentEP.fetchStockCandles,
            ),
            ('/kite/login-url', 'GET', self.investmentEP.getKiteLoginUrl),
            ('/kite/status', 'GET', self.investmentEP.fetchKiteTokenStatus),
            ('/kite/generate-session', 'POST', self.investmentEP.generateKiteSession),
            ('/kite/holdings', 'GET', self.investmentEP.fetchKiteHoldings),
            ('/kite/positions', 'GET', self.investmentEP.fetchKitePositions),
            ('/kite/sync-holdings', 'GET', self.investmentEP.syncKiteHoldings),
            # ak-v55t (2026-11-22): Kite instrument-master autocomplete
            # search. Serves DB-backed LIKE queries from the daily-
            # refreshed kite_instruments cache; no Kite broker call
            # per request. Prereq for watchlist + alert-creation.
            (
                '/kite/instruments/search', 'GET',
                self.investmentEP.searchKiteInstruments,
            ),
            # ak-zai2 Kite Bundle 2 (2026-11-12): available-funds
            # dashboard tile + today's-trades FE surface + daily
            # reconciliation cross-check (via ReconcileTradesTask).
            ('/kite/margins', 'GET', self.investmentEP.fetchKiteMargins),
            ('/kite/trades', 'GET', self.investmentEP.fetchKiteTrades),
            # ak-h1mq (2026-11-22): Mutual Funds detail-page endpoints
            # — 3 thin READ-ONLY wrappers over KiteService.get_mf_*.
            # Backs the FE MF-page holdings / orders / SIPs tabs
            # (Category B #10/#11/#12 of the Kite capability roadmap).
            ('/kite/mf/holdings', 'GET', self.investmentEP.fetchKiteMFHoldings),
            ('/kite/mf/orders', 'GET', self.investmentEP.fetchKiteMFOrders),
            ('/kite/mf/sips', 'GET', self.investmentEP.fetchKiteMFSips),
            # ak-zi54 (2026-11-22): today's full order book from Kite
            # — complements /kite/trades (filled only) with every
            # order placed today across all statuses (OPEN / COMPLETE
            # / CANCELLED / REJECTED / TRIGGER_PENDING). READ-ONLY.
            ('/kite/orders', 'GET', self.investmentEP.fetchKiteOrders),
            # ak-l8a7 (2026-11-22): per-order drill-down endpoints —
            # Kite's order_history + order_trades wrapped as READ-ONLY
            # endpoints behind the Category B #9 FE drill-down modal.
            # /history returns the status-transition timeline for a
            # single order; /trades returns the per-fill breakdown
            # (useful when a sliced order fills across multiple
            # executions).
            (
                '/kite/orders/<order_id>/history', 'GET',
                self.investmentEP.fetchKiteOrderHistory,
            ),
            (
                '/kite/orders/<order_id>/trades', 'GET',
                self.investmentEP.fetchKiteOrderTrades,
            ),
            # ak-dx98 Kite Bundle 3 (2026-11-12): monitoring surfaces —
            # active GTTs with LTP-distance overlay + price alerts +
            # per-alert trigger history drill-down.
            ('/kite/gtts', 'GET', self.investmentEP.fetchKiteGtts),
            ('/kite/alerts', 'GET', self.investmentEP.fetchKiteAlerts),
            (
                '/kite/alerts/<alert_uuid>/history',
                'GET',
                self.investmentEP.fetchKiteAlertHistory,
            ),
        ]

        for rule, method, view_func in investmentRoutes:
            self.add_url_rule(rule, methods=[method], view_func=view_func)

        # ak-05af (2026-11-22): Watchlist CRUD + LTP batch endpoints.
        # First Kite-roadmap bead with INTENTIONAL mutations (POST /
        # DELETE / PUT) — single-user, non-destructive own-data
        # shape per bead §23; single-delete gate NOT required
        # (would be for bulk-delete). ``/watchlist/ltp`` polls
        # Kite every 30s from the FE until the WebSocket bead
        # (ak-vue6) lands; KiteAuthError on /ltp maps to the
        # standard reconnect-required 401 payload.
        watchlistRoutes = [
            ('/watchlist', 'GET', self.watchlistEP.listItems),
            ('/watchlist', 'POST', self.watchlistEP.addItem),
            (
                '/watchlist/<item_id>', 'DELETE',
                self.watchlistEP.removeItem,
            ),
            (
                '/watchlist/<item_id>/note', 'PUT',
                self.watchlistEP.updateNote,
            ),
            ('/watchlist/ltp', 'GET', self.watchlistEP.fetchLtp),
        ]
        for rule, method, view_func in watchlistRoutes:
            self.add_url_rule(rule, methods=[method], view_func=view_func)

        # ak-vue6 Phase 1 (2026-11-23): Kite LTP SSE + snapshot
        # endpoints. Both READ-ONLY — single-symbol snapshot + SSE
        # delta stream. Backed by the standalone ak-kite-ws.service
        # daemon via UNIX-socket IPC. Overseer-approved spec:
        # /api/kite/* prefix (not /kite/*) so the FE companion can
        # cleanly route the live-data surface separately from the
        # REST Kite endpoints.
        kiteLtpRoutes = [
            ('/api/kite/ltp/<symbol>', 'GET', self.kiteLtpEP.getLtp),
            ('/api/kite/stream/ltp', 'GET', self.kiteLtpEP.streamLtp),
        ]
        for rule, method, view_func in kiteLtpRoutes:
            self.add_url_rule(rule, methods=[method], view_func=view_func)

        # Freelance Management API Routes
        
        # Dashboard endpoints
        dashboardRoutes = [
            ('/freelance/dashboard', 'GET', self.dashboardEP.get_dashboard_analytics),
            ('/freelance/earnings', 'GET', self.dashboardEP.get_earnings_by_date_range),
        ]

        # Invoice management endpoints
        invoiceRoutes = [
            ('/freelance/invoices', 'POST', self.invoiceEP.create_invoice),
            ('/freelance/invoices', 'GET', self.invoiceEP.get_invoices),
            ('/freelance/invoices/<invoice_number>', 'GET', self.invoiceEP.get_invoice_by_id),
            ('/freelance/invoices/<invoiceId>', 'PUT', self.invoiceEP.update_invoice),
            ('/freelance/invoices/<invoiceId>', 'DELETE', self.invoiceEP.delete_invoice),
        ]

        # PDF generation endpoints
        pdfRoutes = [
            ('/freelance/invoices/pdf', 'POST', self.pdfEP.generate_invoice_pdf),
            ('/freelance/invoices/<invoiceId>/sign', 'POST', self.pdfEP.sign_invoice_pdf),
        ]

        # Customer management endpoints
        customerRoutes = [
            ('/freelance/customers', 'GET', self.customerEP.get_customers),
            ('/freelance/customers', 'POST', self.customerEP.create_customer),
            ('/freelance/customers/<customerId>', 'GET', self.customerEP.get_customer),
            ('/freelance/customers/<customerId>', 'PUT', self.customerEP.update_customer),
            ('/freelance/customers/<customerId>', 'DELETE', self.customerEP.delete_customer),
            ('/freelance/customers/<customerId>/template', 'PUT', self.templateEP.update_customer_template),
        ]

        # Customer email linking endpoints
        customerEmailRoutes = [
            ('/freelance/customers/<customerId>/emails', 'GET', self.customerEmailEP.get_customer_emails),
            ('/freelance/customers/<customerId>/emails', 'POST', self.customerEmailEP.link_email),
            ('/freelance/customers/<customerId>/emails/<emailId>', 'DELETE', self.customerEmailEP.unlink_email),
            ('/freelance/emails/search', 'GET', self.customerEmailEP.search_emails),
            ('/freelance/emails/batch-relink', 'POST', self.customerEmailEP.batch_relink),
        ]

        # Template management endpoints
        templateRoutes = [
            ('/freelance/templates', 'GET', self.templateEP.get_templates),
            ('/freelance/templates', 'POST', self.templateEP.create_template),
            ('/freelance/templates/<templateId>', 'PUT', self.templateEP.update_template),
            ('/freelance/templates/<templateId>', 'DELETE', self.templateEP.delete_template),
        ]

        # Signature management endpoints
        signatureRoutes = [
            ('/freelance/signatures', 'GET', self.signatureEP.get_signatures),
            ('/freelance/signatures', 'POST', self.signatureEP.upload_signature),
            ('/freelance/signatures/<signatureId>', 'GET', self.signatureEP.get_signature_data),
            ('/freelance/signatures/<signatureId>/default', 'PUT', self.signatureEP.set_default_signature),
            ('/freelance/signatures/<signatureId>', 'DELETE', self.signatureEP.delete_signature),
        ]

        # Jobs management endpoints
        jobsRoutes = [
            ('/jobs/summary', 'GET', self.jobsEP.get_jobs_summary),
            ('/jobs/daily-history', 'GET', self.jobsEP.get_jobs_daily_history),
            ('/jobs/by-title-status', 'GET', self.jobsEP.get_jobs_by_title_status),
            ('/jobs/<job_id>/cancel', 'DELETE', self.jobsEP.cancel_job),
            ('/jobs/cancel-bulk', 'POST', self.jobsEP.cancel_jobs_bulk),
            ('/jobs/run-now', 'POST', self.jobsEP.run_job_now),
            ('/jobs/run-status', 'GET', self.jobsEP.get_run_status),
        ]

        # Temp Job Email management endpoints
        jobEmailRoutes = [
            ('/job-scanner/scan', 'POST', self.jobEmailEP.scan_emails),
            ('/job-scanner/emails', 'GET', self.jobEmailEP.get_job_emails),
            ('/job-scanner/emails/<email_id>', 'GET', self.jobEmailEP.get_job_email_details),
            ('/job-scanner/emails/<email_id>', 'PATCH', self.jobEmailEP.update_email_details),
            ('/job-scanner/emails/<email_id>', 'DELETE', self.jobEmailEP.delete_email),
            ('/job-scanner/emails/<email_id>/read', 'PATCH', self.jobEmailEP.mark_email_as_read),
            ('/job-scanner/stats', 'GET', self.jobEmailEP.get_email_stats),
            ('/job-scanner/gmail-status', 'GET', self.jobEmailEP.get_gmail_status),
            ('/job-scanner/gmail-refresh', 'POST', self.jobEmailEP.refresh_gmail_token),
        ]

        # File Storage (Document Vault) endpoints
        fileStorageRoutes = [
            ('/files/upload', 'POST', self.fileStorageEP.upload_file),
            ('/files/list', 'GET', self.fileStorageEP.list_files),
            ('/files/<fileId>/download', 'GET', self.fileStorageEP.download_file),
            ('/files/<fileId>/view', 'GET', self.fileStorageEP.view_file),
            # ak-kxdr (2026-11-22): DELETE /files/<fileId> is now a
            # SOFT-delete (behavior change — rows get deleted_at set,
            # disk file preserved for 30 days). Hard-delete requires
            # the DELETE /files/trash/<fileId> endpoint with the
            # confirm-id destructive gate.
            ('/files/<fileId>', 'DELETE', self.fileStorageEP.delete_file),
            ('/files/<fileId>/label', 'PUT', self.fileStorageEP.update_label),
            ('/files/<fileId>/move', 'PUT', self.fileStorageEP.move_file),
            # ak-kxdr: trash surface — list / restore / hard-delete.
            # Restore is user_id-scoped with the IDOR-leak 404-not-403
            # shape from ak-05af. Hard-delete gates on confirm_id ==
            # fileId (§23 #5 destructive-gate; operator must echo
            # the id to prove intent).
            ('/files/trash', 'GET', self.fileStorageEP.list_trash),
            (
                '/files/trash/<fileId>/restore', 'POST',
                self.fileStorageEP.restore_file,
            ),
            (
                '/files/trash/<fileId>', 'DELETE',
                self.fileStorageEP.hard_delete_file,
            ),
            ('/folders', 'POST', self.fileStorageEP.create_folder),
            ('/folders/<folderId>', 'PUT', self.fileStorageEP.rename_folder),
            ('/folders/<folderId>', 'DELETE', self.fileStorageEP.delete_folder),
        ]

        # Portfolio visitor tracking endpoints
        portfolioRoutes = [
            ('/portfolio/visitors', 'GET', self.portfolioVisitorEP.get_visitors),
            ('/portfolio/stats', 'GET', self.portfolioVisitorEP.get_stats),
        ]

        # Agent chat endpoint + ak-1x4 attachment upload + ak-bq5
        # cross-device conversation persistence
        agentRoutes = [
            ('/agent/chat', 'POST', self.agentEP.chat),
            ('/agent/attach', 'POST', self.agentEP.attach),
            # ak-bq5: cross-device chat persistence endpoints. All
            # endpoints filter by g.firebase_id (set by before_request
            # middleware). Soft delete via /agent/conversations/<id>.
            ('/agent/conversations', 'GET',
                self.agentConversationsEP.list),
            ('/agent/conversations', 'POST',
                self.agentConversationsEP.create),
            ('/agent/conversations/<conversation_id>', 'GET',
                self.agentConversationsEP.get),
            ('/agent/conversations/<conversation_id>', 'DELETE',
                self.agentConversationsEP.delete),
        ]

        # ak-5vg: WealthDigest page endpoints. Read-side only —
        # WealthDigestTask (services/tasks/WealthDigestTask.py) owns
        # the write path unchanged. Auth via before_request middleware
        # (g.firebase_id).
        wealthDigestRoutes = [
            ('/wealth-digest/latest', 'GET',
                self.wealthDigestEP.latest),
            ('/wealth-digest', 'GET',
                self.wealthDigestEP.by_date),
            ('/wealth-digest/mark-read', 'POST',
                self.wealthDigestEP.mark_read),
        ]

        # ak-9dz: user-scoped downloads for agent-produced files. Backs
        # the shared `attach_file_to_chat` MCP tool (files written by
        # Freelance / Investment agents mid-turn become downloadable
        # here). Auth via before_request middleware (g.firebase_id);
        # cross-user isolation enforced inside the controller/service.
        userFilesRoutes = [
            ('/api/user-files/<uuid>', 'GET',
                self.userFilesEP.download),
        ]

        # Register all routes
        all_routes = [
            *dashboardRoutes,
            *invoiceRoutes,
            *pdfRoutes,
            *customerRoutes,
            *customerEmailRoutes,
            *templateRoutes,
            *signatureRoutes,
            *jobsRoutes,
            *jobEmailRoutes,
            *portfolioRoutes,
            *agentRoutes,
            *wealthDigestRoutes,
            *userFilesRoutes,
            *fileStorageRoutes,
        ]

        for rule, method, view_func in all_routes:
            self.add_url_rule(rule, methods=[method], view_func=view_func)

        self.logger.info("Application routes initialized.")

    def _setup_hooks(self):
        """Set up request and teardown hooks."""

        @self.before_request
        def _set_db_on_request():
            """Attach the database session to the request context."""
            g.db = self.db

        @self.before_request
        def _request_interceptor():
            """Intercept incoming requests.

            ak-c8l (2026-11-22): replaced raw X-Firebase-ID header
            trust with firebase_admin JWT verification. See
            utils/firebase_auth.py for the full rollout-mode
            matrix. ``g.firebase_id`` now holds the VERIFIED uid
            (or, under rollout-cycle ``verify_with_raw_uid_
            fallback`` mode, the raw UID with a WARN log). All
            downstream code must read ``g.firebase_id`` — the raw
            header must NEVER be trusted outside this middleware.
            """
            if request.method == "OPTIONS":
                self.logger.info("OPTIONS preflight request received.")
                return
            from utils.firebase_auth import (
                authenticate_request, FirebaseAuthError,
            )
            header_value = request.headers.get("X-Firebase-ID")
            try:
                verified_uid, auth_mode = authenticate_request(
                    header_value,
                )
            except FirebaseAuthError as exc:
                self.logger.warning(
                    f"Firebase auth rejected request: {exc.reason}"
                )
                return jsonify({
                    "error": f"Unauthorized - {exc.reason}",
                }), 401
            # ak-et1b (2026-11-23): strict shape check on the
            # verified uid BEFORE it becomes g.firebase_id. The
            # downstream filesystem-path callsites
            # (transactionsService / fileStorageService /
            # signatureService / agentFileAttachmentService /
            # agent_attachments) use g.firebase_id as a path
            # component; a UID containing `/`, `..`, NUL, or
            # other filesystem-sensitive chars would escape the
            # sandbox dir. Firebase Auth UIDs are alphanumeric
            # (+ -_) by spec; anything else is a request to
            # reject at the chokepoint. Central defense; the 5
            # cited audit sites ALSO call
            # assert_valid_firebase_id_for_path as defense-in-
            # depth.
            from utils.firebase_id_validator import (
                is_valid_firebase_id,
            )
            if not is_valid_firebase_id(verified_uid):
                self.logger.warning(
                    "[ak-et1b] rejecting request: verified "
                    "firebase_id fails strict shape check "
                    "(auth_mode=%s)",
                    auth_mode,
                )
                return jsonify({
                    "error": (
                        "Bad request - firebase_id shape invalid"
                    ),
                }), 400
            g.firebase_id = verified_uid
            g.firebase_auth_mode = auth_mode

        # @self.teardown_appcontext
        # def _teardown_db():
        #     """Remove the database session at the end of the request."""
        #     g.pop('db', None)
        #     self.db.session.remove()

    def run_app(self, host='0.0.0.0', port=8080, debug=True):
        """Run the application."""
        self.logger.info(f"Running the app on {host}:{port} with debug={debug}.")
        self.run(host=host, port=port, debug=debug)

app = Akkountant(__name__)
flask_app = app.app

if __name__ == "__main__":
    app.run_app(debug=True)
