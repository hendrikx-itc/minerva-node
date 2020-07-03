# -*- coding: utf-8 -*-
import logging
import json
import traceback

import psycopg2
import yaml
import signal
import threading
from time import sleep
from operator import not_

from minerva.util import compose, retry_while
from minerva.db import connect

from minerva_node.pika_consumer import Consumer
from minerva_node.harvest_job import HarvestJob
from minerva_node.error import JobError, JobDescriptionError
from minerva_node.log import level_map


class Node(Consumer):
    SIGNAL_MAP = {
        signal.SIGHUP: "SIGHUP",
        signal.SIGKILL: "SIGKILL",
        signal.SIGTERM: "SIGTERM",
        signal.SIGINT: "SIGINT",
        signal.SIGUSR1: "SIGUSR1"
        }

    def __init__(self, parser, url, queue, key="minerva", logger=None, log_level="WARNING"):

        Consumer.__init__(
            self, url, queue, key, logger
        )
        self.parser = parser
        log_level = level_map.get(log_level)
        self.setup_logging(log_level)
        self.stop_event = threading.Event()

    def start_node(self):
        signal.signal(signal.SIGTERM, self.stop_node)
        signal.signal(signal.SIGINT, self.stop_node)
        signal.signal(signal.SIGHUP,self.stop_node)

        handler_map = {
            psycopg2.OperationalError: lambda exc: logging.error(
                "could not connect to database ({}), waiting".format(exc)
            )
        }

        retry_condition = compose(not_, self.stop_event.is_set)

        self.dbconnection = retry_while(
            connect, handler_map, retry_condition
        )

        if self.dbconnection:
            logging.info("Starting consumer")

            self.run()

            while self.is_alive() and not self.stop_event.is_set():
                sleep(1)

        logging.info("Stopped")

    def on_reception(self, body):
        job = self.create_job(body)

        try:
            job.execute()
        except JobError:
            err_msg = traceback.format_exc()

            logging.error(
                'Error executing job: {}'.format(err_msg)
            )

        logging.info("Finished job {}".format(job))

    def create_job(self, job_description):
        """
        A job description is a dictionary in the following form:

            {
                "data_type": "pm_3gpp",
                "on_failure": [
                    "move_to", "/data/failed/"
                ],
                "on_success": [
                    "remove"
                ],
                "parser_config": {},
                "uri": "/data/new/some_file.xml",
                "data_source": "pm-system-1"
            }
        """
        try:
            job_description = json.loads(job_description)
            if 'description' in job_description.keys():
                job_description.update(job_description['description'])
        except ValueError as e:
            raise JobDescriptionError("Invalid job description") from e

        job = HarvestJob(
            self.parser, self.dbconnection, job_description
        )

        return job

    def load_config(self, file_path):
        with open(file_path) as config_file:
            return yaml.load(config_file, Loader=yaml.SafeLoader)

    def setup_logging(self, log_level):
        log_handler = logging.StreamHandler()
        formatter = logging.Formatter("%(levelname)s %(message)s")
        log_handler.setFormatter(formatter)

        root_logger = logging.getLogger("")
        root_logger.setLevel(log_level)
        root_logger.addHandler(log_handler)

    def stop_node(self, signum, _frame):
        logging.info(
            "received {0!s} signal".format(self.SIGNAL_MAP.get(signum, signum))
        )

        self.stop_event.set()
        self.stop()
