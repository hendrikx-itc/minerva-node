# -*- coding: utf-8 -*-
"""Provides the JobCollector class."""
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
import queue

import pyinotify

from minerva.util import expand_args, iter_queue, no_op

from minerva_dispatcher.harvestjobsource import HarvestJobSource, JOB_TYPE
from minerva_dispatcher.error import ConfigError

EVENT_MASK = (
    pyinotify.IN_MOVED_TO |
    pyinotify.IN_CLOSE_WRITE |
    pyinotify.IN_CREATE  # Needed for auto-watching created directories
)

TIMEOUT = 1.0


class JobCollector(object):

    """
    Collects jobs for specified job_sources.

    Each harvest jobsource points to a directory and the JobCollector monitors
    the filesystem for new files using inotify.

    """

    def __init__(self, job_sources, stop_event):
        self.job_sources = job_sources
        self.stop_event = stop_event
        self.notifier = None
        self.queue = queue.Queue()
        self.notifier = setup_notifier(self.job_sources, self.queue.put)

    def start(self):
        """Start the job collection."""
        self.notifier.start()

    def stop(self):
        """Stop the job collection."""
        self.notifier.stop()

    def join(self):
        """Wait for termination of the notifier thread."""
        self.notifier.join()

    def iter_jobs(self):
        """Return iterator over the job queue."""
        return iter_queue(self.stop_event, self.queue.get_nowait, queue.Empty,
                          TIMEOUT)


def get_job_sources(cursor):
    """Return list of HarvestJobSource instances."""
    query = (
        "SELECT id, name, job_type, config "
        "FROM system.job_source "
        "WHERE job_type = %s")

    args = (JOB_TYPE, )

    cursor.execute(query, args)

    return map(expand_args(HarvestJobSource), cursor.fetchall())


def setup_notifier(job_sources, enqueue):
    """Setup and return ThreadedNotifier watching the job sources."""
    watch_manager = pyinotify.WatchManager()

    for job_source in job_sources:
        try:
            watch_source(watch_manager, enqueue, job_source)
        except ConfigError as exc:
            logging.error(exc)

    return pyinotify.ThreadedNotifier(watch_manager)


def watch_source(watch_manager, enqueue, job_source):
    """Add a watch for the job source to watch_manager and return None."""
    match_pattern = job_source.config["match_pattern"]

    try:
        regex = re.compile(match_pattern)
    except re.error as exc:
        raise ConfigError(
            "invalid match_expression '{}': {}".format(match_pattern, exc))

    uri = job_source.config["uri"]

    try:
        os.stat(uri)
    except OSError as exc:
        raise ConfigError("error watching directory {}: {}".format(uri, exc))

    name_matches = regex.match

    def event_matches(event):
        return not event.dir and name_matches(event.name)

    def handle_event(event):
        if event_matches(event):
            file_path = os.path.join(event.path, event.name)

            enqueue(
                job_source.create_job(file_path)
            )

    proc_fun = event_handler({
        "IN_CLOSE_WRITE": handle_event,
        "IN_MOVED_TO": handle_event
    })

    recursive = job_source.config["recursive"]

    watch_manager.add_watch(
        uri, EVENT_MASK, proc_fun=proc_fun, rec=recursive, auto_add=True)

    logging.info("watching {} with filter {}".format(uri, match_pattern))


def event_handler(handler_map, default_handler=no_op):
    """Return a function that handles events based on their type."""
    def f(event):
        handler_map.get(event.maskname, default_handler)(event)

    return f
