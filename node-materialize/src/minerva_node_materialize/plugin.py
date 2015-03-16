# -*- coding: utf-8 -*-
__docformat__ = "restructuredtext en"

__copyright__ = """
Copyright (C) 2013 Hendrikx-ITC B.V.

Distributed under the terms of the GNU General Public License as published by
the Free Software Foundation; either version 3, or (at your option) any later
version.  The full license is in the file COPYING, distributed as part of
this software.
"""
from minerva_node_materialize.job import MaterializeJob


class MaterializePlugin(object):
    name = "materialize"
    description = "a materialization plugin"

    def __init__(self, conn):
        self.conn = conn

    def create_job(self, id, description, config):
        return MaterializeJob(self.conn, id, description)
