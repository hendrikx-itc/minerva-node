# -*- coding: utf-8 -*-
from datetime import datetime

from nose.tools import eq_
import pytz

from minerva.storage.datatype import DataTypeTimestampWithTimeZone, \
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
