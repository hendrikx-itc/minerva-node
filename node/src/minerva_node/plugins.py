# -*- coding: utf-8 -*-
__docformat__ = "restructuredtext en"

__copyright__ = """
Copyright (C) 2008-2012 Hendrikx-ITC B.V.

Distributed under the terms of the GNU General Public License as published by
the Free Software Foundation; either version 3, or (at your option) any later
version.  The full license is in the file COPYING, distributed as part of
this software.
"""
import pkg_resources

ENTRY_POINT = "node.plugins"


def load_plugins():
    """
    Load and return a list with plugins.
    """
    return [
        entry_point.load()
        for entry_point in pkg_resources.iter_entry_points(group=ENTRY_POINT)
    ]
