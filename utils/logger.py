import logging
import os
from functools import wraps
from logging.handlers import TimedRotatingFileHandler
from flask import jsonify


class Logger:
    """Logger class to configure logging for Akkountant."""

    def __init__(self, name: str):
        """Initialize the logger with the given name."""
        self._logger = logging.getLogger(name)
        self._logger.setLevel(logging.DEBUG)  # Setting the logging level

        # Creating the logs directory if it doesn't exist
        # log_dir = 'logs'
        # if not os.path.exists(log_dir):
        #     os.makedirs(log_dir)

        # Creating handlers for different log levels
        self._setup_handlers("")

    def _setup_handlers(self, log_dir: str):
        """Set up file handlers for different log levels."""
        # Creating console handler
        console_handler = logging.StreamHandler()
        console_handler.setLevel(logging.DEBUG)

        # Creating formatter
        formatter = logging.Formatter('%(asctime)s - %(name)s - %(levelname)s - %(message)s')
        console_handler.setFormatter(formatter)
        self._logger.addHandler(console_handler)

        # File handler for pipeline debugging — captures all INFO+ logs
        log_dir_path = os.path.join(os.path.dirname(os.path.dirname(os.path.abspath(__file__))), 'logs')
        if not os.path.exists(log_dir_path):
            os.makedirs(log_dir_path)
        file_handler = logging.FileHandler(
            os.path.join(log_dir_path, 'pipeline_debug.log'),
            mode='a',
        )
        file_handler.setLevel(logging.INFO)
        file_handler.setFormatter(formatter)
        self._logger.addHandler(file_handler)

    @staticmethod
    def standardLogger(func):
        """Static method to use as a decorator for logging and error handling."""

        @wraps(func)
        def wrapper(*args, **kwargs):
            instance = args[0]  # The instance of the class where the method is called
            logger = instance.logger if hasattr(instance, 'logger') else logging.getLogger(func.__name__)

            logger.info(f"Starting {func.__name__}")
            try:
                result = func(*args, **kwargs)
                logger.info(f"Completed {func.__name__} successfully")
                return result
            except Exception as e:
                logger.error(f"An error occurred in {func.__name__}: {e}")
                return jsonify({"Error": e.__str__()}), 500

        return wrapper

    def get_logger(self):
        """Return the configured logger."""
        return self._logger
