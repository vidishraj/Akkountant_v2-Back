import logging
import os
from flask import Flask
from flask_sqlalchemy import SQLAlchemy
from dotenv import load_dotenv
from sqlalchemy import inspect
from utils.logger import Logger
from models import *


class Akkountant(Flask):
    """Main application class for Akkountant."""
    db: SQLAlchemy
    logger: logging.Logger

    def __init__(self, import_name: str):
        load_dotenv()
        super().__init__(import_name)

        self.logger = Logger(__name__).get_logger()

        self.logger.info("Starting Akkountant")

        self.setUpDatabase()
        self.logger.info("Finished setting up db connection and creating tables.")

        self.init_routes()
        self.logger.info("Finished setting up routes.")

    def setUpDatabase(self):
        """Sets up the database configuration and initializes the connection using SQLAlchemy."""

        self.config['SQLALCHEMY_DATABASE_URI'] = os.getenv('DATABASE_URL')
        # self.config['SQLALCHEMY_ECHO'] = True  # For detailed logs
        self.config['SQLALCHEMY_TRACK_MODIFICATIONS'] = False  # Performance overhead
        self.db = SQLAlchemy(self, model_class=Base)

        # Creating tables if they do not exist
        with self.app_context():
            self.logger.info("Creating database tables")
            inspector = inspect(self.db.engine)
            existing_tables_before = inspector.get_table_names()

            self.db.create_all()
            self.logger.info("Database tables created successfully.")

            inspector = inspect(self.db.engine)
            existing_tables_after = inspector.get_table_names()
            self.logger.info(f"Existing tables after creation: {existing_tables_after}")

            if len(existing_tables_after) > len(existing_tables_before):
                new_tables = list(set(existing_tables_after) - set(existing_tables_before))
                self.logger.info(f"New tables created: {new_tables}")
            else:
                self.logger.info("No new tables were created.")

    def init_routes(self):
        """App routes."""
        pass

    def run_app(self, host='0.0.0.0', port=5500, debug=False):
        """Akkountant Runner."""
        self.logger.info(f"Running the app on {host}:{port} with debug={debug}.")
        self.run(host=host, port=port, debug=debug)


if __name__ == "__main__":
    app = Akkountant(__name__)
    app.run_app()
