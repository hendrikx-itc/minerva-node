# -*- coding: utf-8 -*-
import logging
import json

from minerva_node import Job
from minerva_node.config import consumer_settings as config
from minerva_node.pika_consumer import Consumer

from contextlib import closing


class Node(Consumer):
    def __init__(self, queue=None):
        Consumer.__init__(self, config['url'], queue or config['queue'], config['logger'])
        self.plugin = None

    def on_reception(self, body):
        job = self.create_job(body)
        self.process(job)

    def create_job(self, job_description):
        job_type = self.queue
        job_id = 0

        try:
            job_description = json.loads(job_description)

        except ValueError:
            raise
            logging.error("invalid job description")
            return Job(job_type, 0, job_description)

        if self.plugin is None:
            logging.error("no plugin defined")
            return Job(job_type, job_id, job_description)
        else:
            return self.plugin.create_job(job_id, job_description, config)

    def process(self, job):
        job.execute()


class MultiNode(object):
    def __init__(self):
        self.nodes = []

    def add_node(self, node):
        self.nodes.append(node)

    def run(self):
        for node in self.nodes:
            node.run()

    def is_alive(self):
        for node in self.nodes:
            if node.is_alive():
                return True
        else:
            return False
