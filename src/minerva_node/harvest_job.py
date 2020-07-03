# -*- coding: utf-8 -*-
import logging

from minerva.directory.entitytype import NoSuchEntityType
from minerva.storage.trend.trendstore import NoSuchTrendStore
from minerva.instance import MinervaInstance

from minerva_node.error import JobError
from minerva_node.done_actions import execute_action


DEFAULT_ACTION = ["remove"]


class HarvestError(JobError):
    pass


class HarvestJob:
    def __init__(self, parse_method, conn, description):
        self.parse = parse_method
        self.conn = conn
        print(description)
        self.uri = description.get("uri")
        instance = MinervaInstance.load()
        self.attribute_stores = {
            str(attribute_store).lower(): attribute_store
            for attribute_store in instance.load_attribute_stores()
        }
        self.data_source = description.get("data_source")
        self.description = description

    def __str__(self):
        return "'{}'".format(self.uri)

    def execute(self):
        try:
            self.parse(self.conn, self.uri, self.attribute_stores, self.data_source)

        except NoSuchTrendStore as exc:
            # This can happen normally, when no trend store is
            # configured for the data in the package, but you might
            # want to see what data you are missing using debug
            # logging.
            self.conn.rollback()
            logging.debug(str(exc))
        except NoSuchEntityType as exc:
            # For a similar reason as the above exception, no entity
            # type might be found.
            self.conn.rollback()
            logging.debug(str(exc))
        except Exception as exc:
            logging.error("Failure executing job '{}': {}".format(self.uri, str(exc)))
            execute_action(
                self.uri, self.description.get("on_failure", DEFAULT_ACTION)
            )
        else:
            execute_action(
                self.uri, self.description.get("on_success", DEFAULT_ACTION)
            )

            logging.debug("Finished job '{}'".format(self.uri))
