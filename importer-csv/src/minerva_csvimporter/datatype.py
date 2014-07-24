# -*- coding: utf-8 -*-
__docformat__ = "restructuredtext en"

__copyright__ = """
Copyright (C) 2014 Hendrikx-ITC B.V.

Distributed under the terms of the GNU General Public License as published by
the Free Software Foundation; either version 3, or (at your option) any later
version.  The full license is in the file COPYING, distributed as part of
this software.
"""
from datetime import datetime, tzinfo
from functools import partial

import pytz

from minerva.storage import datatype


class DataType(object):
    @classmethod
    def string_parser_config(cls, config):
        raise NotImplementedError()

    @classmethod
    def string_parser(cls, config):
        raise NotImplementedError()

    @classmethod
    def string_serializer(cls, config):
        raise NotImplementedError()


def assure_tzinfo(tz):
    if isinstance(tz, tzinfo):
        return tz
    else:
        return pytz.timezone(tz)


class DataTypeTimestampWithTimeZone(DataType):
    name = 'timestamp with time zone'

    @classmethod
    def string_parser_config(cls, config):
        return {
            "null_value": config["null_value"],
            "tzinfo": assure_tzinfo(config["tzinfo"]),
            "format": config["format"]
        }

    @classmethod
    def string_parser(cls, config):
        """
        Return function that can parse a string representation of a TimestampWithTimeZone value.

        :param config: a dictionary with the form {"tzinfo", <tzinfo>, "format", <format_string>}
        :return: a function (str_value) -> value
        """
        null_value = config["null_value"]
        tz = config["tzinfo"]
        format_str = config["format"]

        def parse(value):
            if value == null_value:
                return None
            else:
                return tz.localize(datetime.strptime(value, format_str))

        return parse


class DataTypeTimestamp(DataType):
    name = 'timestamp'

    @classmethod
    def string_parser(cls, config):
        def parse(value):
            if value == config["null_value"]:
                return None
            else:
                return datetime.strptime(value, config["format"])

        return parse


class DataTypeSmallInt(DataType):
    name = 'smallint'

    @classmethod
    def string_parser(cls, config):
        def parse(value):
            if value == config["null_value"]:
                return None
            else:
                return int(value)

        return parse


class DataTypeInteger(DataType):
    name = 'integer'

    @classmethod
    def string_parser(cls, config):
        def parse(value):
            if value == config["null_value"]:
                return None
            else:
                return int(value)

        return parse


class DataTypeReal(DataType):
    name = 'real'

    @classmethod
    def string_parser(cls, config):
        def parse(value):
            """
            Parse value and return float value. If value is empty ('') or None,
            None is returned.
            :param value: string representation of a real value, e.g.; '34.00034',
            '343', ''
            :return: float value
            """
            if value == config["null_value"]:
                return None
            else:
                return float(value)

        return parse


class DataTypeDoublePrecision(DataType):
    name = 'double precision'

    @classmethod
    def string_parser(cls, config):
        def parse(value):
            if value == config["null_value"]:
                return None
            else:
                return float(value)

        return parse


class DataTypeText(DataType):
    name = 'text'

    @classmethod
    def string_parser(cls, config):
        def parse(value):
            if value == config["null_value"]:
                return None
            else:
                return value

        return parse


data_types = [
    DataTypeTimestamp,
    DataTypeTimestampWithTimeZone,
    DataTypeReal,
    DataTypeDoublePrecision,
    DataTypeInteger,
    DataTypeSmallInt,
    DataTypeText
]


type_map = {d.name: d for d in data_types}


def parse_values(data_types, values):
    return [
        data_type.from_string(value)
        for data_type, value in zip(data_types, values)
    ]


DEFAULT_DATA_TYPE = "smallint"


def deduce_data_types(rows):
    """
    Return a list of the minimal required datatypes to store the values, in the
    same order as the values and thus matching the order of attribute_names.
    """
    row_length = len(rows[0])

    return reduce(
        datatype.max_datatypes,
        map(types_from_values, rows),
        [DEFAULT_DATA_TYPE] * row_length
    )


types_from_values = partial(map, datatype.deduce_from_value)


def load_data_format(format):
    datatype_name = format["datatype"]

    try:
        data_type = type_map[datatype_name]
    except KeyError:
        raise Exception("No such data type: {}".format(datatype_name))
    else:
        config = data_type.string_parser_config(format["string_format"])

        return data_type, data_type.string_parser(config)