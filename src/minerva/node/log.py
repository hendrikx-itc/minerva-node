import os
import logging
from logging.handlers import RotatingFileHandler

from minerva.util.config import parse_size


level_map = {
    "DEBUG": logging.DEBUG,
    "INFO": logging.INFO,
    "WARNING": logging.WARNING,
    "ERROR": logging.ERROR,
    "CRITICAL": logging.CRITICAL}


def setup_logging(directory, filename, rotation_size, level):
    """
    Setup rotating file logging.
    """
    max_log_size = parse_size(rotation_size)

    filepath = os.path.join(directory, filename)
    handler = RotatingFileHandler(filepath, maxBytes=max_log_size, backupCount=3)
    handler.setLevel(level_map[level])

    formatter = logging.Formatter("%(asctime)s %(levelname)s %(message)s")
    handler.setFormatter(formatter)

    rootlogger = logging.getLogger("")
    rootlogger.setLevel(level_map[level])
    rootlogger.addHandler(handler)
