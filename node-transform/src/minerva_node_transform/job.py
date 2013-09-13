# -*- coding: utf-8 -*-
__docformat__ = "restructuredtext en"

__copyright__ = """
Copyright (C) 2012-2013 Hendrikx-ITC B.V.

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

from minerva.node.error import JobError, JobDescriptionError, JobExecutionError
from minerva.transform.helpers import get_function_set, NoSuchFunctionSetError
from minerva.transform.types import Transformation


class TransformJob(object):
	def __init__(self, minerva_context, id, description):
		self.minerva_context = minerva_context
		self.id = id
		self.description = description

		try:
			self.function_set_id = self.description["function_set_id"]
		except KeyError:
			raise JobDescriptionError("'function_set_id' not set in description")

		try:
			dest_timestamp_str = self.description["dest_timestamp"]
		except KeyError:
			raise JobDescriptionError("'dest_timestamp' not set in description")

		self.dest_timestamp = datetime_parser.parse(dest_timestamp_str)

	def __str__(self):
		return "transform function_set {} for timestamp {}".format(
				self.function_set_id, self.dest_timestamp)

	def execute(self):
		with closing(self.minerva_context.reader_conn.cursor()) as cursor:
			try:
				function_set = get_function_set(cursor, self.function_set_id)
			except NoSuchFunctionSetError:
				raise JobExecutionError("no function set with id {}".format(
						self.function_set_id))

		transformation = Transformation(function_set, self.dest_timestamp)
		transformation.execute(self.minerva_context)
