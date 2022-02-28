# -*- coding: utf-8 -*-
import logging
import json
from time import sleep

import psycopg2

from minerva.harvest.plugins import load_plugins

from minerva_node.pika_consumer import Consumer
from minerva_node.harvest_job import HarvestJob
from minerva_node.error import JobError, JobDescriptionError


MAX_TIMEOUT = 60  # Maximum timeout between retries
TIMEOUT_STEP = 1.0  # Step size in seconds for the timeout between retries


class Node(Consumer):
    config: dict

    def __init__(self, connect_fn, stop_event, config: dict):
        """Initialize a new instance of the Node class."""
        Consumer.__init__(self, config["url"], config["queue"])
        self.config = config
        self.connect_fn = connect_fn
        self.stop_event = stop_event
        self.plugins = load_plugins()

    def on_reception(self, body):
        job = self.create_job(body)

        retry = True
        attempt = 1

        while retry and not self.stop_event.is_set():
            try:
                conn = self.connect_fn()

                job.execute(conn)
            except psycopg2.OperationalError as e:
                logging.error("Error executing job: {}".format(e))

                # Database not ready yet, so retry
                retry = True

                timeout = min(attempt * TIMEOUT_STEP, MAX_TIMEOUT)

                sleep(timeout)
            except JobError as exc:
                logging.error("Error executing job: {}".format(exc))

                # We don't know if it was a recoverable error so don't retry
                retry = False
            else:
                # Success, so no retry is needed
                retry = False

            attempt += 1

        logging.info("Finished job {}".format(job))

    def create_job(self, job_description):
        """
        Create a new HarvestJob from a job description.

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
        except ValueError as e:
            raise JobDescriptionError("Invalid job description") from e

        return HarvestJob(self.plugins, job_description)
