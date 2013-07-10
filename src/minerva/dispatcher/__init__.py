# -*- coding: utf-8 -*-
__docformat__ = "restructuredtext en"

__copyright__ = """
Copyright (C) 2008-2010 Hendrikx-ITC B.V.

Distributed under the terms of the GNU General Public License as published by
the Free Software Foundation; either version 3, or (at your option) any later
version.  The full license is in the file COPYING, distributed as part of
this software.
"""
import os
import logging
import re
import threading
import Queue
from functools import partial
import json

from version import __version__

import pyinotify

from minerva.util import compose

JOB_TYPE = "harvest"


def get_job_sources(cursor, job_type):
	query = (
		"SELECT id, name, job_type, config "
		"FROM system.job_source "
		"WHERE job_type = %s")

	args = (job_type, )

	cursor.execute(query, args)

	return cursor.fetchall()


def setup_notifier(job_sources, enqueue):
	watch_manager = pyinotify.WatchManager()

	notifier = pyinotify.ThreadedNotifier(watch_manager)

	mask = pyinotify.IN_MOVED_TO | pyinotify.IN_CLOSE_WRITE | pyinotify.IN_CREATE

	for job_source_id, name, job_type, config in job_sources:
		config_parsed = json.loads(config)

		match_pattern = config_parsed["match_pattern"]

		regex = re.compile(match_pattern)

		job_config = config_parsed["job_config"]

		make_job_description = job_description_creator(job_config)

		enqueue_for_job_source = partial(enqueue, job_source_id)

		enqueue_action = compose(enqueue_for_job_source, make_job_description)

		handler_map = {
			"IN_CLOSE_WRITE": partial(handle_event, regex.match, enqueue_action),
			"IN_MOVED_TO": partial(handle_event, regex.match, enqueue_action)}

		uri = config_parsed["uri"]
		recursive = config_parsed["recursive"]

		proc_fun = partial(event_handler, handler_map)

		watch_manager.add_watch(uri, mask, proc_fun=proc_fun, rec=recursive,
				auto_add=True)

		logging.info("watching {} with filter {}".format(uri, match_pattern))

	return notifier


def job_description_creator(job_config):
	def fn(path):
		description = {"uri": path}
		description.update(job_config)
		return description

	return fn


def handle_event(is_match, enqueue, event):
	if not event.dir and is_match(event.name):
		enqueue(os.path.join(event.path, event.name))


def event_handler(handler_map, event):
	handler = handler_map.get(event.maskname)

	if handler:
		handler(event)
