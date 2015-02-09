# -*- coding: utf-8 -*-
__docformat__ = "restructuredtext en"

__copyright__ = """
Copyright (C) 2014 Hendrikx-ITC B.V.

Distributed under the terms of the GNU General Public License as published by
the Free Software Foundation; either version 3, or (at your option) any later
version.  The full license is in the file COPYING, distributed as part of
this software.
"""


class DataExtractor(object):
    def from_record(self, record):
        raise NotImplementedError()

    def header_check(self):
        return None

    def record_check(self):
        return None
