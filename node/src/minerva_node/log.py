import logging


level_map = {
    "DEBUG": logging.DEBUG,
    "INFO": logging.INFO,
    "WARNING": logging.WARNING,
    "ERROR": logging.ERROR,
    "CRITICAL": logging.CRITICAL
}


def setup_logging(level):
    root_logger = logging.getLogger("")
    root_logger.setLevel(level_map[level])
