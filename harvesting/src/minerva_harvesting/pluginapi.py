# -*- coding: utf-8 -*-
__docformat__ = "restructuredtext en"

__copyright__ = """
Copyright (C) 2008-2013 Hendrikx-ITC B.V.

Distributed under the terms of the GNU General Public License as published by
the Free Software Foundation; either version 3, or (at your option) any later
version.  The full license is in the file COPYING, distributed as part of
this software.
"""


class HarvestPlugin(object):
    @staticmethod
    def storagetype():
        """Return the required storage type for the plugin."""
        raise NotImplementedError()

    @staticmethod
    def create_parser(rawdatapackage_handler, config):
        """
        Create and return new parser instance.
        """
        raise NotImplementedError()
