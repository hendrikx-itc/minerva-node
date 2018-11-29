import logging

from minerva_node.pluginapi import NodePlugin
from minerva.system.jobqueue import fail_job, finish_job


class Job():
    """
    The Node job base class.
    """
    def __init__(self, type_, id_, description):
        self.type = type_
        self.description = description
        self.id = id

    def execute(self):
        """
        Execute the actual job logic. This method should be overridden in child
        classes that implement the job specifics.
        """
        logging.info(self)

    def __str__(self):
        return "{0.type} {0.id}, '{0.description}'".format(self)

    def finish(self, cursor):
        finish_job(self.id)(cursor)

    def fail(self, message):
        return fail_job(self.id, message)
