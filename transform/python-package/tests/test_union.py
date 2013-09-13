# -*- coding: utf-8 -*-
__docformat__ = "restructuredtext en"

__copyright__ = """
Copyright (C) 2008-2012 Hendrikx-ITC B.V.

Distributed under the terms of the GNU General Public License as published by
the Free Software Foundation; either version 3, or (at your option) any later
version.  The full license is in the file COPYING, distributed as part of
this software.
"""
from operator import itemgetter, getslice
from datetime import datetime

from nose.tools import assert_raises, assert_true, assert_false, assert_equal

from minerva.transform import union


def test_union():
    """
    Check that packages are merged correctly
    """
    timestamp = datetime.now()

    data_set_a = ('entity_id', 'timestamp', 'counter1', 'counter2', 'counter3'), [
        (1234, timestamp, 42.42, 12, 5),
        (1235, timestamp, 12.56, 32, 3),
        (1236, timestamp, 98.34, 67, 8)]

    data_set_b = ('entity_id', 'timestamp', 'counter1', 'counter2'), [
        (1237, timestamp, 34.93, 32),
        (1238, timestamp, 93.22, 33),
        (1239, timestamp, 90.20, 19)]

    data_sets = [data_set_a, data_set_b]

    unioned = union(data_sets)

    assert_equal(len(unioned), len(data_set_a[1]) + len(data_set_b[1]))

    for record in unioned:
        assert_equal(len(record), 5)
