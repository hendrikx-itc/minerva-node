# -*- coding: utf-8 -*-
__docformat__ = "restructuredtext en"

__copyright__ = """
Copyright (C) 2008-2012 Hendrikx-ITC B.V.

Distributed under the terms of the GNU General Public License as published by
the Free Software Foundation; either version 3, or (at your option) any later
version.  The full license is in the file COPYING, distributed as part of
this software.
"""
from datetime import datetime, timedelta
import pytz

from nose.tools import assert_raises, assert_true, assert_false, assert_equal
from minerva_transform.helpers import calc_dest_timestamp

def test_dest_timestamp():
    """
    Test destination timestamp
    """
    tz = pytz.timezone("Europe/Amsterdam")

    dest_granularity = 604800
    source_timestamp = tz.localize(datetime(2012, 10, 8, 0,0,0))
    dest_timestamp = tz.localize(datetime(2012, 10, 8, 0, 0, 0))

    assert_equal(calc_dest_timestamp(dest_granularity, source_timestamp), dest_timestamp)

    dest_granularity = 86400
    source_timestamp = tz.localize(datetime(2012, 10, 9, 2, 0, 0))
    dest_timestamp = tz.localize(datetime(2012, 10, 10, 0, 0, 0))

    assert_equal(calc_dest_timestamp(dest_granularity, source_timestamp), dest_timestamp)

    dest_granularity = 86400
    source_timestamp = tz.localize(datetime(2012, 10, 27, 23, 0, 0))
    dest_timestamp = tz.localize(datetime(2012, 10, 28, 0, 0, 0))

    assert_equal(calc_dest_timestamp(dest_granularity, source_timestamp), dest_timestamp)

    # TO DO: test DST issues (2012-10-28 3:00 -> 2012-10-28 2:00)
    dest_granularity = 86400
    source_timestamp = tz.localize(datetime(2012, 10, 28, 1, 0, 0))
    dest_timestamp = tz.localize(datetime(2012, 10, 29, 0, 0, 0))

    assert_equal(calc_dest_timestamp(dest_granularity, source_timestamp), dest_timestamp)

