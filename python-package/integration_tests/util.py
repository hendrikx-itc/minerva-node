# -*- coding: utf-8 -*-
__docformat__ = "restructuredtext en"

__copyright__ = """
Copyright (C) 2012-2013 Hendrikx-ITC B.V.

Distributed under the terms of the GNU General Public License as published by
the Free Software Foundation; either version 3, or (at your option) any later
version.  The full license is in the file COPYING, distributed as part of
this software.
"""
from minerva.util import first, head, tail, k, lines, unlines
from minerva.util.tabulate import render_table


def render_datapackage(datapackage):
    """
    Renders a data 'source' in the form of a table-like object:

    [
        ('column_1', 'column_2', 'column_3', ...),
        (1, 2, 3,...),
        (4, 5, 6,...),
        ...
    ]
    """
    column_names = datapackage.trend_names
    column_align = ">" * len(column_names)
    column_sizes = ["max"] * len(column_names)
    rows = [(r[0], datapackage.timestamp) + tuple(r[1]) for r in datapackage.rows]

    return render_table(column_names, column_align, column_sizes, rows)
