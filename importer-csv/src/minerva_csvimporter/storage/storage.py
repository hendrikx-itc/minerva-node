# -*- coding: utf-8 -*-
__docformat__ = "restructuredtext en"

__copyright__ = """
Copyright (C) 2014 Hendrikx-ITC B.V.

Distributed under the terms of the GNU General Public License as published by
the Free Software Foundation; either version 3, or (at your option) any later
version.  The full license is in the file COPYING, distributed as part of
this software.
"""


class Storage(object):
    def connect(self, conn):
        raise NotImplementedError()

    def store(self, column_names, value_mapping, raw_data_rows):
        raise NotImplementedError()