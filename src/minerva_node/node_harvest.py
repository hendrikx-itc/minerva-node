# -*- coding: utf-8 -*-
import os
import logging
from datetime import datetime
from functools import partial
import codecs
import traceback
import gzip
from contextlib import closing

from minerva.directory import DataSource
from minerva.directory.existence import Existence
from minerva.harvest.plugins import load_plugins

from minerva_node.error import JobError
from minerva_node.done_actions import execute_action


DEFAULT_ACTION = ["remove"]


class HarvestError(JobError):
    pass


class HarvestPlugin:
    name = "harvest"
    description = "a harvesting plugin"

    def __init__(self, conn):
        self.conn = conn
        self.plugins = load_plugins()
        self.existence = Existence(conn)

    def create_job(self, description, config):
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
            self.plugins, self.existence, self.conn, description
        )


class HarvestJob:
    def __init__(self, plugins, existence, conn, description):
        self.plugins = plugins
        self.existence = existence
        self.conn = conn
        self.description = description
        if 'description' in description:
            self.description.update(description['description'])

    def __str__(self):
        return "'{}'".format(self.description["uri"])

    def execute(self):
        try:
            data_source_name = self.description["data_source"]

            with closing(self.conn.cursor()) as cursor:
                data_source = DataSource.get_by_name(data_source_name)(cursor)

                if data_source is None:
                    raise HarvestError(
                        "no data source with name '{}'".format(data_source_name)
                    )

            parser_config = self.description.get("parser_config", {})
            uri = self.description["uri"]

            update_existence = parser_config.get("update_existence", None)

            data_type = self.description["data_type"]
        except Exception as err:
            err.args += (str(self.description),)
            raise

        try:
            plugin = self.plugins[data_type]
        except KeyError:
            raise HarvestError(
                "could not load parser plugin '{}' - not in {}".format(data_type, ', '.join(self.plugins.keys()))
            )

        parser = plugin.create_parser(parser_config)

        encoding = self.description.get("encoding", "utf-8")

        data_stream = open_uri(uri, encoding)

        logging.debug("opened uri '{}'".format(uri))

        try:
            for store_cmd in map(parser.store_command(), parser.packages(data_stream, os.path.basename(uri))):
                store_cmd(data_source)(
                    self.conn
                )
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
