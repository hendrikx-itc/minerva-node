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
        self.existence = Existence(minerva_context.conn)

    def create_job(self, description):
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
        return HarvestJob(
            self.plugins, self.existence, self.minerva_context, description
        )


class HarvestJob(object):
    def __init__(self, plugins, existence, minerva_context, description):
        self.plugins = plugins
        self.existence = existence
        self.minerva_context = minerva_context
        self.description = description

    def __str__(self):
        return "'{}'".format(self.description["uri"])

    def execute(self):
        datasource_name = self.description["data_source"]

        try:
            datasource = get_datasource(self.minerva_context.conn, datasource_name)
        except NoSuchDataSourceError:
            raise HarvestError("no datasource with name '{}'".format(datasource_name))

        parser_config = self.description.get("parser_config", {})
        uri = self.description["uri"]

        datatype = self.description["data_type"]

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

        dispatch_raw = compose(dispatch_raw_datapackage, storage_provider.RawDataPackage)

        parser = plugin.create_parser(dispatch_raw, parser_config)

        encoding = self.description.get("encoding", "utf-8")

        logging.debug("encoding: {}".format(encoding))

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
            logging.debug("opening text file using encoding {}".format(encoding))
            open_action = partial(codecs.open, uri, encoding=encoding)

    try:
        return open_action()
    except IOError as exc:
        raise HarvestError("Could not open {0}: {1!s}".format(uri, exc))
    except LookupError as exc:
        raise HarvestError(str(exc))
