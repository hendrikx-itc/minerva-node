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
    DataTypeTimestamp, DataTypeSmallInt, DataTypeInteger, load_data_format


def test_timestampwithtimezone():
    value = DataTypeTimestampWithTimeZone.string_parser(
        {
            "null_value": "\\N",
            "format": "%Y%m%d",
            "tzinfo": pytz.utc
        }
    )("20140519")

    eq_(value, pytz.utc.localize(datetime(2014, 5, 19)))


def test_timestamp():
    value = DataTypeTimestamp.string_parser(
        {
            "null_value": "\\N",
            "format": "%Y%m%d"
        }
    )("20140519")

    eq_(value, datetime(2014, 5, 19))


def test_smallint():
    value = DataTypeSmallInt.string_parser({"null_value": "\\N"})("42")

    eq_(value, 42)


def test_integer():
    value = DataTypeInteger.string_parser({"null_value": "\\N"})("8900432")

    eq_(value, 8900432)

    value = DataTypeInteger.string_parser({"null_value": ""})("")

    eq_(value, None)

    value = DataTypeInteger.string_parser({"null_value": "\\N"})("\\N")

    eq_(value, None)


def test_data_format_def():
    data_type, parser = load_data_format({
        "datatype": "timestamp with time zone",
        "string_format": {
            "null_value": "\\N",
            "format": "%Y-%m-%d %H:%M:%S",
            "tzinfo": "Europe/Amsterdam"
        }
    })

    parser("2014-05-27 08:04:00")
