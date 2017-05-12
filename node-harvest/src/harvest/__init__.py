# -*- coding: utf-8 -*-
__docformat__ = "restructuredtext en"

__copyright__ = """
Copyright (C) 2012 Hendrikx-ITC B.V.

Distributed under the terms of the GNU General Public License as published by
the Free Software Foundation; either version 3, or (at your option) any later
version.  The full license is in the file COPYING, distributed as part of
this software.
"""
import os
import logging
from datetime import datetime
from functools import partial
import codecs
import traceback
import gzip

import psycopg2

from minerva.util import compose
from minerva.directory.helpers import get_datasource, NoSuchDataSourceError
from minerva.directory.distinguishedname import explode
from minerva.directory.existence import Existence

from minerva_node.error import JobError

from harvest.plugins import ENTRYPOINT, load_plugins


DEFAULT_ACTION = {"name": "remove", "args": []}


class HarvestError(JobError):
    pass


class HarvestPlugin(object):
    name = "harvest"
    description = "a harvesting plugin"

    def __init__(self, minerva_context):
        self.minerva_context = minerva_context
        self.plugins = load_plugins()
        self.existence = Existence(minerva_context.writer_conn)

    def create_job(self, id, description, config):
        """
        A job description is a dictionary in the following form:

            {
                "datatype": "pm_3gpp",
                "on_failure": {
                    "name": "move",
                    "args": ["/data/failed/"]
                },
                "parser_config": {},
                "uri": "/data/new/some_file.xml",
                "datasource": "pm-system-1"
            }
        """
        return HarvestJob(self.plugins, self.existence, self.minerva_context, id, description)


class HarvestJob(object):
    def __init__(self, plugins, existence, minerva_context, id, description):
        self.plugins = plugins
        self.existence = existence
        self.minerva_context = minerva_context
        self.id = id
        self.description = description

    def __str__(self):
        return "'{}'".format(self.description["uri"])

    def execute(self):
        datasource_name = self.description["datasource"]

        try:
            datasource = get_datasource(self.minerva_context.writer_conn, datasource_name)
        except NoSuchDataSourceError:
            raise HarvestError("no datasource with name '{}'".format(datasource_name))

        parser_config = self.description.get("parser_config", {})
        uri = self.description["uri"]

        update_existence = parser_config.get("update_existence", None)

        datatype = self.description["datatype"]

        try:
            plugin = self.plugins[datatype]
        except KeyError:
            raise HarvestError("could not load parser plugin '{}'".format(datatype))

        storagetype = plugin.storagetype()

        try:
            storage_provider = self.minerva_context.storage_providers[storagetype]
        except KeyError:
            raise HarvestError("could not load '{}' storage provider plugin".format(storagetype))

        dispatch_raw_datapackage = partial(storage_provider.store_raw, datasource)

        if update_existence:
            dispatch_raw_datapackage = partial(dispatch_raw_and_mark_existing,
                    dispatch_raw_datapackage, update_existence,
                    self.existence.mark_existing)

        dispatch_raw = compose(dispatch_raw_datapackage, storage_provider.RawDataPackage)

        parser = plugin.create_parser(dispatch_raw, parser_config)

        encoding = self.description.get("encoding", "utf-8")

        datastream = open_uri(uri, encoding)

        logging.debug("opened uri '{}'".format(uri))

        try:
            parser.parse(datastream, os.path.basename(uri))
        except psycopg2.InterfaceError as exc:
            stacktrace = traceback.format_exc()

            raise JobError(stacktrace)
        except Exception as exc:
            stacktrace = traceback.format_exc()

            execute_action(uri, self.description.get("on_failure", DEFAULT_ACTION))

            raise JobError(stacktrace)
        else:
            execute_action(uri, self.description.get("on_success", DEFAULT_ACTION))

        if update_existence:
            self.existence.flush(datetime.now())


def execute_action(uri, action):
    if action:
        action_fn = done_actions[action["name"]]
        action_args = action["args"]
        action_fn(uri, *action_args)


def remove(path):
    try:
        os.remove(path)
    except Exception as exc:
        logging.warn(str(exc))


def move(path, to):
    filepath, filename = os.path.split(path)
    new = os.path.join(to, filename)

    try:
        os.rename(path, new)
    except Exception as exc:
        logging.warn(str(exc))


def do_nothing(path):
    pass


done_actions = {
    "remove": remove,
    "move": move,
    "do_nothing": do_nothing
}


def dispatch_raw_and_mark_existing(store_raw, filter_types, mark_existing, raw_datapackage):
    """
    :param store_raw: a storage plugin 'store_raw' function
    :param filter_types: a list of entitytype names that will be filtered for
    existence marking
    :param mark_existing: a function that takes arguments (dns, timestamp)
    """
    dns = [dn for dn, _timestamp, _values in raw_datapackage.rows if entitytype_from_dn(dn) in
            filter_types]

    mark_existing(dns)

    store_raw(raw_datapackage)


def entitytype_from_dn(dn):
    """
    Return the entitytype name from a Distinguished Name
    """
    parts = explode(dn)

    return parts[-1][0]


def open_uri(uri, encoding):
    """
    Return a file object for the specified URI.
    """
    if uri.endswith(".gz"):
        open_action = partial(gzip.open, uri)
    else:
        if encoding == "binary":
            open_action = partial(open, uri, "rb")
        else:
            open_action = partial(codecs.open, uri, encoding=encoding)

    try:
        return open_action()
    except IOError as exc:
        raise HarvestError("Could not open {0}: {1!s}".format(uri, exc))
    except LookupError as exc:
        raise HarvestError(str(exc))
