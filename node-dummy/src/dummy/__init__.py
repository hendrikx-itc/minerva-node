# -*- coding: utf-8 -*-
import logging

from minerva.node import Job


class DummyPlugin(object):
    name = "dummy"
    description = "dummy plugin for testing purposes"

    def __init__(self, conn):
        self.conn = conn

    def create_job(self, id, description, config):
        return DummyJob(id, description)


class DummyJob(Job):
    def __init__(self, id, description):
        Job.__init__(self, "dummy", id, description)
        self.id = id
        self.description = description

    def execute(self):
        logging.info("execute {0.id}, '{0.description}'".format(self))

