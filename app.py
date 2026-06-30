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
from services.cronAgent import CronAgent
from services.portfolioVisitorService import PortfolioVisitorService
from services.mailProcessorService import MailProcessorService
from services.reconciliationService import ReconciliationService
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

    def _setup_database(self):
        """Set up database connection and create tables."""
        self.db = SQLAlchemy(self, model_class=models.Base)
        with self.app_context():
            self.logger.info("Creating database tables if not exist.")
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
        self.agentService = AgentService()
        self.agentService.set_services(
            investment_service=self.investmentService,
            transaction_service=self.transactionService,
            invoice_service=self.invoiceService,
            customer_service=self.customerService,
            dashboard_service=self.dashboardService,
            mail_processor=self.mailProcessor,
            conversation_service=self.agentConversationService,  # ak-bq5
        )
        self.agentEP = AgentController(self.agentService)
        self.agentConversationsEP = AgentConversationsController(  # ak-bq5
            self.agentConversationService,
        )
        self.portfolioVisitorService = PortfolioVisitorService()
        self.portfolioVisitorEP = PortfolioVisitorController(self.portfolioVisitorService)

    def _setup_schedulers(self):
        """Set up background daemon threads (CronAgent, mail, snapshots).
        TaskScheduler is run separately via schedular.service.
        Uses a file lock so only one gunicorn worker starts these threads."""
        if os.getenv('ENV') == 'LOCAL':
            self.logger.info("Skipping schedulers in LOCAL environment.")
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
            ('/downloadFile', 'GET', self.transactionEP.downloadFile),
            ('/deleteFile', 'GET', self.transactionEP.deleteFile),
            ('/processMailPipeline', 'POST', self.transactionEP.processMailPipeline),
            ('/reprocessPdf', 'POST', self.transactionEP.reprocessPdf),
            ('/updatePersonalInfo', 'POST', self.transactionEP.updatePersonalInfo),
            ('/getPersonalInfo', 'GET', self.transactionEP.getPersonalInfo),
            ('/reconcile', 'POST', self.transactionEP.reconcile),
            ('/zeroSumReport', 'POST', self.transactionEP.zeroSumReport),
            ('/statementPeriods', 'GET', self.transactionEP.statementPeriods),
            ('/reconciliationStatus', 'GET', self.transactionEP.reconciliationStatus),
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
            ('/kite/login-url', 'GET', self.investmentEP.getKiteLoginUrl),
            ('/kite/generate-session', 'POST', self.investmentEP.generateKiteSession),
            ('/kite/holdings', 'GET', self.investmentEP.fetchKiteHoldings),
            ('/kite/positions', 'GET', self.investmentEP.fetchKitePositions),
            ('/kite/sync-holdings', 'GET', self.investmentEP.syncKiteHoldings),
        ]

        for rule, method, view_func in investmentRoutes:
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
            ('/files/<fileId>', 'DELETE', self.fileStorageEP.delete_file),
            ('/files/<fileId>/label', 'PUT', self.fileStorageEP.update_label),
            ('/files/<fileId>/move', 'PUT', self.fileStorageEP.move_file),
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
            """Intercept incoming requests."""
            if request.method == "OPTIONS":
                self.logger.info("OPTIONS preflight request received.")
                return
            firebase_id = request.headers.get("X-Firebase-ID")
            if not firebase_id:
                self.logger.warning("Request missing Firebase ID.")
                return jsonify({"error": "Unauthorized - Firebase ID is required"}), 401
            g.firebase_id = firebase_id

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
