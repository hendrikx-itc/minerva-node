# -*- coding: utf-8 -*-
__docformat__ = "restructuredtext en"

__copyright__ = """
Copyright (C) 2012-2015 Hendrikx-ITC B.V.

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
from contextlib import closing

from minerva.directory import DataSource
from minerva.directory.distinguishedname import entity_type_name_from_dn
from minerva.directory.existence import Existence

from minerva_harvesting.plugins import load_plugins

from minerva_node.error import JobError
from minerva_node import NodePlugin, Job
from minerva_node_harvest.done_actions import execute_action


DEFAULT_ACTION = ["remove"]


class HarvestError(JobError):
    pass


class HarvestPlugin(NodePlugin):
    name = "harvest"
    description = "a harvesting plugin"

    def __init__(self, minerva_context):
        self.minerva_context = minerva_context
        self.plugins = load_plugins()
        self.existence = Existence(minerva_context.writer_conn)

    def create_job(self, id_, description, config):
        """
        A job description is a dictionary in the following form:

            {
                "data_type": "pm_3gpp",
                "on_failure": [
                    "move_to", "/data/failed/"
                ],
                "on_success": [
                    "remove"
                ],
                "parser_config": {},
                "uri": "/data/new/some_file.xml",
                "data_source": "pm-system-1"
            }
        """
        return HarvestJob(
            self.plugins, self.existence, self.minerva_context, id_, description
        )


class HarvestJob(Job):
    def __init__(self, id_, plugins, existence, minerva_context, description):
        self.id = id_
        self.plugins = plugins
        self.existence = existence
        self.minerva_context = minerva_context
        self.description = description

    def __str__(self):
        return "'{}'".format(self.description["uri"])

    def execute(self):
        data_source_name = self.description["data_source"]

        with closing(self.minerva_context.writer_conn.cursor()) as cursor:
            data_source = DataSource.get_by_name(data_source_name)(cursor)

            if data_source is None:
                raise HarvestError(
                    "no data source with name '{}'".format(data_source_name)
                )

        parser_config = self.description.get("parser_config", {})
        uri = self.description["uri"]

        update_existence = parser_config.get("update_existence", None)

        data_type = self.description["data_type"]

        try:
            plugin = self.plugins[data_type]
        except KeyError:
            raise HarvestError(
                "could not load parser plugin '{}'".format(data_type)
            )

        storage_type = plugin.storage_type()

        try:
            storage_provider = self.minerva_context.storage_providers[storage_type]
        except KeyError:
            raise HarvestError(
                "could not load '{}' storage provider plugin".format(
                    storage_type
                )
            )

        dispatch_raw_data_package = partial(
            storage_provider.store_raw, data_source
        )

        if update_existence:
            dispatch_raw_data_package = partial(
                dispatch_raw_and_mark_existing,
                dispatch_raw_data_package,
                update_existence,
                self.existence.mark_existing
            )

        parser = plugin.create_parser(parser_config)

        encoding = self.description.get("encoding", "utf-8")

        data_stream = open_uri(uri, encoding)

        logging.debug("opened uri '{}'".format(uri))

        try:
            for package in parser.parse(data_stream, os.path.basename(uri)):
                dispatch_raw_data_package(package)
        except Exception:
            stack_trace = traceback.format_exc()

            execute_action(
                uri, self.description.get("on_failure", DEFAULT_ACTION)
            )

            raise JobError(stack_trace)
        else:
            execute_action(
                uri, self.description.get("on_success", DEFAULT_ACTION)
            )

        if update_existence:
            self.existence.flush(datetime.now())


def dispatch_raw_and_mark_existing(
        store_raw, filter_types, mark_existing, raw_datapackage):
    """
    :param store_raw: a storage plugin 'store_raw' function
    :param filter_types: a list of entity type names that will be filtered for
    existence marking
    :param mark_existing: a function that takes arguments (dns, timestamp)
    """
    dns = [
        dn
        for dn, _timestamp, _values in raw_datapackage.rows
        if entity_type_name_from_dn(dn) in filter_types
    ]

    mark_existing(dns)

    store_raw(raw_datapackage)


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
