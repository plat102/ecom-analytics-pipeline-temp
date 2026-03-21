"""
Provides consistent logging across the application
"""

import logging
from config import settings


def get_logger(name, log_file=None):
    """
    Get a configured logger

    Args:
        name: Logger name (usually __name__)
        log_file: Optional log file name (defaults to module name)

    Returns:
        logging.Logger
    """
    logger = logging.getLogger(name)

    # Avoid adding handlers multiple times
    if logger.handlers:
        return logger

    logger.setLevel(settings.LOG_LEVEL)

    # File handler
    if log_file is None:
        # Convert module name to filename
        log_file = f"{name.replace('.', '_')}.log"

    file_path = settings.LOGS_DIR / log_file
    fh = logging.FileHandler(file_path)
    fh.setLevel(settings.LOG_LEVEL)

    # Console handler
    ch = logging.StreamHandler()
    ch.setLevel(settings.LOG_LEVEL)

    # Formatter
    formatter = logging.Formatter(settings.LOG_FORMAT)
    fh.setFormatter(formatter)
    ch.setFormatter(formatter)

    # Add handlers
    logger.addHandler(fh)
    logger.addHandler(ch)

    return logger
