# -*- coding: utf-8 -*-
__docformat__ = "restructuredtext en"

__copyright__ = """
Copyright (C) 2012 Hendrikx-ITC B.V.

Distributed under the terms of the GNU General Public License as published by
the Free Software Foundation; either version 3, or (at your option) any later
version.  The full license is in the file COPYING, distributed as part of
this software.
"""
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

