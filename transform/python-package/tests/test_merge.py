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

from minerva_transform import merge


def test_merge():
    """
    Check that packages are merged correctly
    """
    timestamp = datetime.now()

    data_set_a = [
        (1234, timestamp, 42.42, 12, 5, 'enabled'),
        (1235, timestamp, 12.56, 32, 3, 'enabled'),
        (1236, timestamp, 98.34, 67, 8, 'enabled'),
        (1237, timestamp, 34.39, 43, 8, 'enabled')]

    data_set_b = [
        (1234, timestamp, 9877),
        (1235, timestamp, 4233),
        (1236, timestamp, 4344)]

    data_sets = [data_set_a, data_set_b]

    def get_key(record):
        return record[0:2]

    def get_values(record):
        return record[2:]

    merged = merge(data_sets, key_fn=get_key, payload_fn=get_values)

    assert_equal(len(data_set_a), len(merged))

    sorted_merged = sorted(merged, key=itemgetter(0))

    assert_equal(sorted_merged[0], (1234, timestamp, 42.42, 12, 5, 'enabled', 9877))
