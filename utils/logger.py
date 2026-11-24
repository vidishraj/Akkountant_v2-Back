import logging
import traceback
from functools import wraps
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
        """Set up handlers for logging."""
        if self._logger.handlers:
            return

        console_handler = logging.StreamHandler()
        console_handler.setLevel(logging.DEBUG)
        formatter = logging.Formatter('%(asctime)s - %(name)s - %(levelname)s - %(message)s')
        console_handler.setFormatter(formatter)
        self._logger.addHandler(console_handler)

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
                # ak-ouu2: never str(e) to the client or the log — SQLAlchemy
                # errors carry bound params (passwords, PAN, receipt data).
                # Log the type + traceback FRAMES only (no exception message).
                frames = "".join(traceback.format_tb(e.__traceback__))
                logger.error(
                    f"An error occurred in {func.__name__} ({type(e).__name__})\n{frames}"
                )
                return jsonify({"Error": "Internal server error"}), 500

        return wrapper

    def get_logger(self):
        """Return the configured logger."""
        return self._logger
