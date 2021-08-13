"""
Minerva Node command line interface
"""
import os
import yaml
import argparse
import logging
import signal
from time import sleep
import threading

import psycopg2

from minerva_node.node import Node

package_name = "minerva_node"
script_name = os.path.basename(__file__)
config_file_name = "node.yml"

SIGNAL_MAP = {
    signal.SIGHUP: "SIGHUP",
    signal.SIGKILL: "SIGKILL",
    signal.SIGTERM: "SIGTERM",
    signal.SIGINT: "SIGINT",
    signal.SIGUSR1: "SIGUSR1"
}

NO_JOB_TIMEOUT = 1


class StartupError(Exception):
    pass


LOG_LEVEL_MAP = {
    'info': logging.INFO,
    'debug': logging.DEBUG,
    'error': logging.ERROR,
    'warn': logging.WARN
}


def main():
    """
    Script entry point
    """
    parser = argparse.ArgumentParser()

    parser.add_argument(
        "-v", "--version", action="version",
        version="%(prog)s {}".format("5.0.0")
    )

    parser.add_argument(
        "-c", "--config-file",
        default=os.path.join("/etc/minerva/", config_file_name),
        help="path of configuration file"
    )

    parser.add_argument(
        '--log-level', default='error', choices=list(LOG_LEVEL_MAP.keys()),
        help='Set log level')

    args = parser.parse_args()

    log_level = LOG_LEVEL_MAP.get(args.log_level.lower(), logging.DEBUG)

    setup_logging(log_level)

    stop_event = threading.Event()

    stop_chain = [
        stop_event.set
    ]

    def stop_node(signum, _frame):
        logging.info(
            "received {0!s} signal".format(SIGNAL_MAP.get(signum, signum))
        )

        for c in stop_chain:
            c()

    signal.signal(signal.SIGTERM, stop_node)
    signal.signal(signal.SIGINT, stop_node)
    signal.signal(signal.SIGHUP, stop_node)

    config = load_config(args.config_file)

    node = Node(connect, stop_event, config['rabbitmq'])

    stop_chain.append(node.stop)
    logging.info("Starting consumer")

    node.run()

    while not stop_event.is_set():
        sleep(1)

    logging.info("Stopped")


def connect():
    return psycopg2.connect('')


def load_config(file_path):
    with open(file_path) as config_file:
        return yaml.load(config_file, Loader=yaml.SafeLoader)


def setup_logging(log_level):
    log_handler = logging.StreamHandler()
    formatter = logging.Formatter("%(levelname)s %(message)s")
    log_handler.setFormatter(formatter)

    root_logger = logging.getLogger("")
    root_logger.setLevel(log_level)
    root_logger.addHandler(log_handler)


if __name__ == '__main__':
    main()
