# -*- coding: utf-8 -*-
import logging
import json

from minerva_node.pika_consumer import Consumer
from minerva_node.node_harvest import HarvestPlugin
from minerva_node.error import JobError


class Node(Consumer):
    def __init__(self, conn, config):
        Consumer.__init__(
            self, config['url'], config['queue'], config['logger']
        )
        self.config = config
        self.conn = conn
        self.harvest_plugin = HarvestPlugin(conn)

    def on_reception(self, body):
        job = self.create_job(body)

        try:
            job.execute()
        except JobError as exc:
            logging.error('Failed to complete job {}\nError message: {}'.format(body, exc.message))

    def create_job(self, job_description):
        try:
            job_description = json.loads(job_description)
        except ValueError:
            logging.error("invalid job description")
            raise

        return self.harvest_plugin.create_job(job_description, self.config)
