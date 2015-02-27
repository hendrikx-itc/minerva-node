# -*- coding: utf-8 -*-
"""
Provides plugin loading functionality.
"""
__docformat__ = "restructuredtext en"

__copyright__ = """
Copyright (C) 2008-2015 Hendrikx-ITC B.V.

Distributed under the terms of the GNU General Public License as published by
the Free Software Foundation; either version 3, or (at your option) any later
version.  The full license is in the file COPYING, distributed as part of
this software.
"""
import pkg_resources

ENTRY_POINT = "harvester.plugins"


def iter_entry_points():
    return pkg_resources.iter_entry_points(group=ENTRY_POINT)


def load_plugins():
    """
    Load and return a dictionary with plugins by their names.
    """
    return {
        entry_point.name: entry_point.load()()
        for entry_point in iter_entry_points()
    }


def get_plugin(name):
    return next(
        entry_point.load()()
        for entry_point in iter_entry_points()
        if entry_point.name == name
    )
