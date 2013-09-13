# -*- coding: utf-8 -*-
"""
Provides plugin loading functionality.
"""
__docformat__ = "restructuredtext en"

__copyright__ = """
Copyright (C) 2008-2012 Hendrikx-ITC B.V.

Distributed under the terms of the GNU General Public License as published by
the Free Software Foundation; either version 3, or (at your option) any later
version.  The full license is in the file COPYING, distributed as part of
this software.
"""
import pkg_resources

ENTRYPOINT = "harvester.plugins"

def load_plugins():
    """
    Load and return a dictionary with plugins by their names.
    """
    return dict((entrypoint.name, entrypoint.load()())
        for entrypoint in pkg_resources.iter_entry_points(group=ENTRYPOINT))
