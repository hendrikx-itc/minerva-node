# -*- coding: utf-8 -*-
import logging
import json
import traceback

from minerva.harvest.plugins import load_plugins

from minerva_node.pika_consumer import Consumer
from minerva_node.harvest_job import HarvestJob
from minerva_node.error import JobError, JobDescriptionError


class Node(Consumer):
    def __init__(self, conn, config):
        Consumer.__init__(
            self, config['url'], config['queue'], config['logger']
        )
        self.config = config
        self.conn = conn
        self.plugins = load_plugins()

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
        except ValueError as e:
            raise JobDescriptionError("Invalid job description") from e

        return HarvestJob(
            self.plugins, self.conn, job_description
        )
