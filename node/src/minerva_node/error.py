# -*- coding: utf-8 -*-
"""
This module contains Harvester error class.
"""


class JobError(Exception):
    """
    Base class for all job creation or job execution errors.
    """
    pass


class JobDescriptionError(Exception):
    """
    Indicates an error in the JSON description of the job.
    """
    pass


class JobExecutionError(Exception):
    """
    Indicates an error during the execution of the job.
    """
    pass


class NodeError(Exception):
    """
    Base for all Node specific errors.
    """
    pass
