import logging
import os

from flask import Flask, g, request, jsonify
from flask_sqlalchemy import SQLAlchemy
from dotenv import load_dotenv
from flask_sqlalchemy.session import Session
from sqlalchemy import inspect, text
from flask_cors import CORS

from controllers.investmentsEP import InvestmentController
from controllers.transactionsEP import TransactionController
from services.InvestmentService import InvestmentService
from services.JsonDownloadService import JSONDownloadService
from services.tasks.scheduler import TaskScheduler
from services.transactionsService import TransactionService
from utils.logger import Logger
from models import *


class Akkountant(Flask):
    """Main application class for Akkountant."""
    db: SQLAlchemy
    logger: logging.Logger
    scheduler: TaskScheduler
    transactionEP: TransactionController
    transactionService: TransactionService

    def __init__(self, import_name: str):
        load_dotenv()
        super().__init__(import_name)
        self.app = self.app_context().app

        # Initialize logger
        self.logger = Logger(__name__).get_logger()
        self.logger.info("Starting Akkountant")

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
        self._setup_investments()  # Run async setup
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
        self.db = SQLAlchemy(self, model_class=Base)
        with self.app_context():
            self.logger.info("Creating database tables if not exist.")
            inspector = inspect(self.db.engine)

            existing_tables_before = inspector.get_table_names()
            self.db.create_all()

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

    def _setup_schedulers(self):
        """Set up background tasks."""
        self.scheduler = TaskScheduler()
        self.logger.info("Background schedulers initialized.")

    @staticmethod
    def _setup_investments():
        """Read from the JSON file, call the JSON service individually for rates and lists."""
        json_service = JSONDownloadService(save_directory=f"{os.getcwd()}/services/assets/")
        json_service.handle_stocks()
        json_service.handle_nps()
        json_service.handle_gold()
        json_service.handle_mf()
        json_service.handle_ppf()

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
            ('/deleteSingleInvestment', 'GET', self.investmentEP.deleteSingleRecord),
            ('/deleteAllInvestments', 'GET', self.investmentEP.deleteAllInvestments),
        ]

        for rule, method, view_func in investmentRoutes:
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

    def run_app(self, host='0.0.0.0', port=8000, debug=True):
        """Run the application."""
        self.logger.info(f"Running the app on {host}:{port} with debug={debug}.")
        self.run(host=host, port=port, debug=debug)


app = Akkountant(__name__)
flask_app = app.app

if __name__ == "__main__":
    app.run_app()
