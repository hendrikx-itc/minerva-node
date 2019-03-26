# -*- coding: utf-8 -*-
import logging
import json

from minerva_node.config import consumer_settings as config
from minerva_node.pika_consumer import Consumer
from minerva_node.node_harvest import HarvestPlugin


class Node(Consumer):
    def __init__(self, conn, queue=None):
        Consumer.__init__(
            self, config['url'], queue or config['queue'], config['logger']
        )
        self.conn = conn
        self.harvest_plugin = HarvestPlugin(conn)

    def on_reception(self, body):
        job = self.create_job(body)

        job.execute()

    def create_job(self, job_description):
        try:
            job_description = json.loads(job_description)
        except ValueError:
            logging.error("invalid job description")
            raise

        return self.harvest_plugin.create_job(job_description, config)
