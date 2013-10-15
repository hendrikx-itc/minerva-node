# -*- coding: utf-8 -*-
"""
This module contains Harvester error class.
"""
__docformat__ = "restructuredtext en"

__copyright__ = """
Copyright (C) 2008-2012 Hendrikx-ITC B.V.

Distributed under the terms of the GNU General Public License as published by
the Free Software Foundation; either version 3, or (at your option) any later
version.  The full license is in the file COPYING, distributed as part of
this software.
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
