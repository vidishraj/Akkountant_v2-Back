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
        self.transactionEP = TransactionController(self.transactionService)
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
        self.jobsEP = JobsController()

    def _setup_schedulers(self):
        """Set up background tasks."""
        pass
        # with self.app_context():
        #     self.scheduler = TaskScheduler(self.db, self)
        #     self.scheduler.start_scheduler()
        #     self.logger.info("Background schedulers initialized.")

    def _insert_initial_jobs(self, title, status, priority, due_date, user_id=None):
        try:
            # Check if a job with the same title and status exists
            existing_job = self.db.session.query(models.Job).filter(models.Job.status.in_([JobStatus.OVERDUE.value,
                                                                                           JobStatus.PENDING.value])).filter_by(title=title).first()

            if existing_job:
                return False  # Job already exists

            # Insert a new job
            new_job = models.Job(
                title=title,
                status=status,
                priority=priority,
                due_date=due_date,
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
            ('/readStatements', 'GET', self.transactionEP.triggerStatementCheck),
            ('/getFileDetails', 'POST', self.transactionEP.fetchFileDetails),
            ('/getGoogleStatus', 'GET', self.transactionEP.checkGoogleApiStatus),
            ('/updateGoogleTokens', 'POST', self.transactionEP.addUpdateUserToken),
            ('/setOptedBanks', 'POST', self.transactionEP.setOptedBanks),
            ('/downloadFile', 'GET', self.transactionEP.downloadFile),
            ('/deleteFile', 'GET', self.transactionEP.deleteFile),
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
            ('/deleteSingleInvestment', 'GET', self.investmentEP.deleteSingleRecord),
            ('/deleteAllInvestments', 'GET', self.investmentEP.deleteAllInvestments),
            # Kite Connect API endpoints
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
            ('/freelance/customers/<customerId>', 'PUT', self.customerEP.update_customer),
            ('/freelance/customers/<customerId>', 'DELETE', self.customerEP.delete_customer),
            ('/freelance/customers/<customerId>/template', 'PUT', self.templateEP.update_customer_template),
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
            ('/freelance/signatures/<signatureId>', 'DELETE', self.signatureEP.delete_signature),
        ]

        # Jobs management endpoints
        jobsRoutes = [
            ('/jobs/summary', 'GET', self.jobsEP.get_jobs_summary),
            ('/jobs/by-title-status', 'GET', self.jobsEP.get_jobs_by_title_status),
            ('/jobs/<job_id>/cancel', 'DELETE', self.jobsEP.cancel_job),
            ('/jobs/cancel-bulk', 'POST', self.jobsEP.cancel_jobs_bulk),
        ]

        # Register all routes
        all_routes = [
            *dashboardRoutes,
            *invoiceRoutes, 
            *pdfRoutes,
            *customerRoutes,
            *templateRoutes,
            *signatureRoutes,
            *jobsRoutes,
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
