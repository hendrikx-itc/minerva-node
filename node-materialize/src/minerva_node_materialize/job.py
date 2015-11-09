# -*- coding: utf-8 -*-
__docformat__ = "restructuredtext en"

__copyright__ = """
Copyright (C) 2013 Hendrikx-ITC B.V.

Distributed under the terms of the GNU General Public License as published by
the Free Software Foundation; either version 3, or (at your option) any later
version.  The full license is in the file COPYING, distributed as part of
this software.
"""
from datetime import datetime
import logging
from contextlib import closing

import pytz
import psycopg2
from dateutil import parser as datetime_parser

from minerva.storage import get_plugin

from minerva_node.error import JobError, JobDescriptionError, JobExecutionError
from minerva_materialize.types import Materialization


class MaterializeJob(object):
    def __init__(self, minerva_context, id, description):
        self.minerva_context = minerva_context
        self.id = id
        self.description = description

        try:
            self.type_id = self.description["type_id"]
        except KeyError:
            raise JobDescriptionError("'type_id' not set in description")

        try:
            timestamp_str = self.description["timestamp"]
        except KeyError:
            raise JobDescriptionError("'timestamp' not set in description")

        self.timestamp = datetime_parser.parse(timestamp_str)

    def __str__(self):
        return "materialization {} for timestamp {}".format(
                self.type_id, self.timestamp)

    def execute(self):
        materialization = Materialization(self.type_id)

        chunk = materialization.chunk(self.timestamp)

        with closing(self.minerva_context.writer_conn.cursor()) as cursor:
            row_count = chunk.execute(cursor)

        msg_template = "{0} materialized {1} records for timestamp {2}"

        logging.info(
            msg_template.format(
                materialization.id, row_count, self.timestamp.isoformat()
            )
        )

        self.minerva_context.writer_conn.commit()

