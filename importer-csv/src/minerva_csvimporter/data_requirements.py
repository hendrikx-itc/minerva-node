# -*- coding: utf-8 -*-
__docformat__ = "restructuredtext en"

__copyright__ = """
Copyright (C) 2014 Hendrikx-ITC B.V.

Distributed under the terms of the GNU General Public License as published by
the Free Software Foundation; either version 3, or (at your option) any later
version.  The full license is in the file COPYING, distributed as part of
this software.
"""


def any_field_empty(field_names):
    """Return True if value of one of `field_names` in `record` equals ''."""
    def fn(record):
        return any(record[field_name] == "" for field_name in field_names)

    return fn