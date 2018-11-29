# -*- coding: utf-8 -*-
import logging
import json
import psycopg2
import traceback

from minerva_node import Job
from minerva_node.config import consumer_settings as config
from minerva_node.pika_consumer import Consumer
from minerva_node.error import NodeError, JobError

from contextlib import closing


class Node(Consumer):
    def __init__(self, conn, queue=None):
        Consumer.__init__(self, config['url'], queue or config['queue'], config['logger'])
        self.plugin = None
        self.connection = conn

    def on_reception(self, body):
        job = self.create_job(body)
        self.process(job)

    def create_job(self, job_description):
        job_type = self.queue
        job_id = 0

        try:
            job_description = json.loads(job_description)
        except ValueError:
            logging.error("invalid job description")
            return Job(job_type, 0, job_description)

        if self.plugin is None:
            logging.error("no plugin defined")
            return Job(job_type, job_id, job_description)
        else:
            return self.plugin.create_job(job_id, job_description, config)

    def process(self, job):
        try:
            job.execute()
        except JobError as exc:
            self.safe_rollback()
            logging.error(exc)
            message = str(exc)
            self.exec_commit(job.fail(message))
        except NodeError as exc:
            self.safe_rollback()
            logging.error(exc)
            message = str(exc)
            self.exec_commit(job.fail(message))
        except Exception:
            self.safe_rollback()
            message = traceback.format_exc()
            logging.error(message)
            self.exec_commit(job.fail(message))
        else:
            logging.info("finished job {} {}".format(job.id, job))
            self.exec_commit(job.finish)

    def safe_rollback(self):
        try:
            self.connection.rollback()
        except (psycopg2.InterfaceError, psycopg2.OperationalError) as exc:
            logging.info(str(exc))

    def safe_commit(self):
        try:
            self.connection.commit()
        except (psycopg2.InterfaceError, psycopg2.OperationalError) as exc:
            logging.info(str(exc))

    def exec_commit(self, cmd):
        try:
            with closing(self.connection.cursor()) as cursor:
                cmd(cursor)
            self.connection.commit()
        except Exception as exc:
            logging.error(exc)


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
