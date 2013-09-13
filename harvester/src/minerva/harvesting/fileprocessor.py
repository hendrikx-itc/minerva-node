#!/usr/bin/env python
"""
Provides the function process_file for processing a single file.
"""
# -*- coding: utf-8 -*-

__docformat__ = "restructuredtext en"

__copyright__ = """
Copyright (C) 2008-2011 Hendrikx-ITC B.V.

Distributed under the terms of the GNU General Public License as published by
the Free Software Foundation; either version 3, or (at your option) any later
version.  The full license is in the file COPYING, distributed as part of
this software.
"""
import os
import threading
import time
import traceback
from operator import not_

import progressbar

from minerva.util import compose

from minerva.harvesting.error import DataError


class ParseError(Exception):
	pass


def process_file(filepath, plugin, parser_config, handle_package, \
		show_progress=False):
	"""
	Process a single file with specified plugin.
	"""
	if not os.path.exists(filepath):
		raise Exception("Could not find file '{0}'".format(filepath))

	_directory, filename = os.path.split(filepath)

	with open(filepath) as data_file:
		stop_event = threading.Event()
		condition = compose(not_, stop_event.is_set)

		if show_progress:
			start_progress_reporter(data_file, condition)

		parser = plugin.create_parser(handle_package, parser_config)

		try:
			parser.parse(data_file, filename)
		except DataError as exc:
			raise ParseError("{0!s} at position {1:d}".format(exc, data_file.tell()))
		except Exception:
			stack_trace = traceback.format_exc()
			position = data_file.tell()
			message = "{0} at position {1:d}".format(stack_trace, position)
			raise Exception(message)
		finally:
			stop_event.set()


def start_progress_reporter(data_file, condition):
	"""
	Start a daemon thread that reports about the progress (position in data_file).
	"""
	data_file.seek(0, 2)
	size = data_file.tell()
	data_file.seek(0, 0)

	widgets = [progressbar.Percentage(), " ", progressbar.Bar(), " ",
		progressbar.ETA()]

	progress_bar = progressbar.ProgressBar(maxval=size, widgets=widgets).start()

	def progress_reporter():
		"""
		Show progress in the file on the console using a progress bar.
		"""
		while condition():
			progress_bar.update(data_file.tell())
			time.sleep(1.0)

	thread = threading.Thread(target=progress_reporter)
	thread.daemon = True
	thread.start()
	return thread
