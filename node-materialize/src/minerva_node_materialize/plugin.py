# -*- coding: utf-8 -*-
from minerva_node_materialize.job import MaterializeJob


class MaterializePlugin(object):
    name = "materialize"
    description = "a materialization plugin"

    def __init__(self, conn):
        self.conn = conn

    def create_job(self, id, description, config):
        return MaterializeJob(self.conn, id, description)
