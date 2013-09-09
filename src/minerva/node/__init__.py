# -*- coding: utf-8 -*-
__docformat__ = "restructuredtext en"

__copyright__ = """
Copyright (C) 2008-2012 Hendrikx-ITC B.V.

Distributed under the terms of the GNU General Public License as published by
the Free Software Foundation; either version 3, or (at your option) any later
version.  The full license is in the file COPYING, distributed as part of
this software.
"""
import logging

from minerva import storage


class Job(object):
    """
    The Node job base class.
    """
    def __init__(self, type, id, description):
        self.type = type
        self.id = id
        self.description = description

    def execute(self):
        """
        Execute the actual job logic. This method should be overridden in child
        classes that implement the job specifics.
        """
        logging.info(self)

    def __str__(self):
        return "{0.type} {0.id}, '{0.description}'".format(self)


class MinervaContext(object):
    """
    Maintains a Minerva context, including things like a connection to the
    writable Minerva database, a connection to the readable Minerva database and
    current Minerva process Id.
    """
    def __init__(self, writer_conn, reader_conn):
        self.writer_conn = writer_conn
        self.reader_conn = reader_conn
        self.storage_providers = dict([(name, p(writer_conn, api_version=4))
                for name, p in storage.load_plugins().iteritems()])

        for name in self.storage_providers.keys():
            logging.info("loaded storage plugin '{}'".format(name))
