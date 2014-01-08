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

class DataError(Exception):
    pass


class HarvesterError(Exception):
    """
    Base for all Harvester specific errors.
    """
    pass