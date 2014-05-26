# -*- coding: utf-8 -*-
__docformat__ = "restructuredtext en"

__copyright__ = """
Copyright (C) 2014 Hendrikx-ITC B.V.

Distributed under the terms of the GNU General Public License as published by
the Free Software Foundation; either version 3, or (at your option) any later
version.  The full license is in the file COPYING, distributed as part of
this software.
"""
from datetime import datetime

from nose.tools import eq_
import pytz

from minerva_csvimporter.datatype import DataTypeTimestampWithTimeZone, \
    DataTypeTimestamp, DataTypeSmallInt, DataTypeInteger


def test_timestampwithtimezone():
    value = DataTypeTimestampWithTimeZone("%Y%m%d").from_string("20140519")

    eq_(value, pytz.utc.localize(datetime(2014, 5, 19)))


def test_timestamp():
    value = DataTypeTimestamp("%Y%m%d").from_string("20140519")

    eq_(value, datetime(2014, 5, 19))


def test_smallint():
    value = DataTypeSmallInt().from_string("42")

    eq_(value, 42)


def test_integer():
    value = DataTypeInteger().from_string("8900432")

    eq_(value, 8900432)

    value = DataTypeInteger().from_string("")

    eq_(value, None)