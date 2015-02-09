# -*- coding: utf-8 -*-
__docformat__ = "restructuredtext en"

__copyright__ = """
Copyright (C) 2014 Hendrikx-ITC B.V.

Distributed under the terms of the GNU General Public License as published by
the Free Software Foundation; either version 3, or (at your option) any later
version.  The full license is in the file COPYING, distributed as part of
this software.
"""
import operator

from minerva_csvimporter.data_extractor import DataExtractor
from minerva_csvimporter.util import as_functor


class ValuesExtractor(DataExtractor):
    """
    Extracts/constructs the identifier (distinguised name) from records based
    on a template.
    """
    def __init__(self, fields):
        self.fields = fields
        self.value_getters = list(map(operator.itemgetter, fields))

    def from_record(self, record):
        """
        Return all values specified by the fields attribute.
        """
        return list(map(as_functor(record), self.value_getters))
