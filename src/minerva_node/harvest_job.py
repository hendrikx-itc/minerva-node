# -*- coding: utf-8 -*-
import os
import logging
from functools import partial
import codecs
import gzip
from contextlib import closing

import psycopg2

from minerva.directory import DataSource
from minerva.directory.entitytype import NoSuchEntityType
from minerva.storage.trend.trendstore import NoSuchTrendStore
from minerva.storage.trend.datapackage import DataPackage

from minerva_node.error import JobError
from minerva_node.done_actions import execute_action


DEFAULT_ACTION = ["remove"]


class HarvestError(JobError):
    pass


class HarvestJob:
    def __init__(self, plugins, description):
        self.plugins = plugins
        self.description = description
        if 'description' in description:
            self.description.update(description['description'])

    def __str__(self):
        return "'{}'".format(self.description["uri"])

    def execute(self, conn):
        try:
            data_source_name = self.description["data_source"]

            with closing(conn.cursor()) as cursor:
                data_source = DataSource.get_by_name(data_source_name)(cursor)

                if data_source is None:
                    raise HarvestError(
                        "no data source with name '{}'".format(data_source_name)
                    )

            parser_config = self.description.get("parser_config", {})
            uri = self.description["uri"]

            data_type = self.description["data_type"]
        except Exception as err:
            err.args += (str(self.description),)
            raise

        try:
            plugin = self.plugins[data_type]
        except KeyError:
            raise HarvestError(
                "could not load parser plugin '{}' - not in {}".format(
                    data_type, ', '.join(self.plugins.keys())
                )
            )

        parser = plugin.create_parser(parser_config)

        encoding = self.description.get("encoding", "utf-8")

        try:
            data_stream = open_uri(uri, encoding)
        except Exception as exc:
            raise JobError(str(exc))

        logging.debug("Opened '{}'".format(uri))

        action = {
            'type': 'harvest',
            'plugin': data_type,
            'uri': uri
        }


        try:
            job_id = start_job(conn, action)

            store_commands = (
                parser.store_command()(package, job_id)
                for package in DataPackage.merge_packages(
                    parser.load_packages(
                        data_stream, os.path.basename(uri)
                    )
                )
            )

            for store_cmd in store_commands:
                try:
                    store_cmd(data_source)(conn)
                except NoSuchTrendStore as exc:
                    # This can happen normally, when no trend store is
                    # configured for the data in the package, but you might
                    # want to see what data you are missing using debug
                    # logging.
                    conn.rollback()
                    logging.debug(str(exc))
                except NoSuchEntityType as exc:
                    # For a similar reason as the above exception, no entity
                    # type might be found.
                    conn.rollback()
                    logging.debug(str(exc))

        except Exception as exc:
            logging.error("Failure executing job '{}': {}".format(uri, str(exc)))

            execute_action(
                uri, self.description.get("on_failure", DEFAULT_ACTION)
            )

            raise JobError(str(exc))
        else:
            end_job(conn, job_id)

            execute_action(
                uri, self.description.get("on_success", DEFAULT_ACTION)
            )

            logging.debug("Finished job '{}'".format(uri))


def start_job(conn, description: dict) -> int:
    with conn.cursor() as cursor:
        cursor.execute(
            "SELECT logging.start_job(%s)",
            (psycopg2.extras.Json(description),)
        )

        job_id = cursor.fetchone()[0]

    return job_id


def end_job(conn, job_id: int):
    with conn.cursor() as cursor:
        cursor.execute(
            "SELECT logging.end_job(%s)",
            (job_id,)
        )


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
