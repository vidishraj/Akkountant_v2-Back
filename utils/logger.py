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
        log_dir = 'logs'
        if not os.path.exists(log_dir):
            os.makedirs(log_dir)

        # Creating handlers for different log levels
        self._setup_handlers(log_dir)

    def _setup_handlers(self, log_dir: str):
        """Set up file handlers for different log levels."""
        levels = {
            'DEBUG': logging.DEBUG,
            'INFO': logging.INFO,
            'WARNING': logging.WARNING,
            'ERROR': logging.ERROR,
            'CRITICAL': logging.CRITICAL,
        }

        # Creating console handler
        console_handler = logging.StreamHandler()
        console_handler.setLevel(logging.DEBUG)

        # Creating formatter
        formatter = logging.Formatter('%(asctime)s - %(name)s - %(levelname)s - %(message)s')
        console_handler.setFormatter(formatter)
        self._logger.addHandler(console_handler)

        # Creating file handlers for different log levels
        for level_name, level_value in levels.items():
            level_dir = os.path.join(log_dir, level_name.lower())
            if not os.path.exists(level_dir):
                os.makedirs(level_dir)

            # Creating a timed rotating file handler
            file_handler = TimedRotatingFileHandler(
                os.path.join(level_dir, f'{level_name.lower()}_log.log'),
                when='midnight',
                interval=1,
                backupCount=7  # Keep the last 7 log files
            )
            file_handler.setLevel(level_value)
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
