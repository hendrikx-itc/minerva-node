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
    def from_string(self, value):
        raise NotImplementedError()

    def to_string(self, value):
        raise NotImplementedError()


class DataTypeTimestampWithTimeZone(DataType):
    name = 'timestamp with time zone'

    def __init__(self, format, timezone=pytz.utc):
        self.format = format

        if isinstance(timezone, tzinfo):
            self.tzinfo = timezone
        elif isinstance(timezone, str):
            self.tzinfo = pytz.timezone(timezone)

    def from_string(self, value):
        return self.tzinfo.localize(datetime.strptime(value, self.format))


class DataTypeTimestamp(DataType):
    name = 'timestamp'

    def __init__(self, format):
        self.format = format

    def from_string(self, value):
        return datetime.strptime(value, self.format)


class DataTypeSmallInt(DataType):
    name = 'smallint'

    def from_string(self, value):
        if value:
            return int(value)
        else:
            return None


class DataTypeInteger(DataType):
    name = 'integer'

    def from_string(self, value):
        if value:
            return int(value)
        else:
            return None


class DataTypeReal(DataType):
    name = 'real'

    def from_string(self, value):
        """
        Parse value and return float value. If value is empty ('') or None,
        None is returned.
        :param value: string representation of a real value, e.g.; '34.00034',
        '343', ''
        :return: float value
        """
        if value:
            return float(value)
        else:
            return None


class DataTypeDoublePrecision(DataType):
    name = 'double precision'

    def from_string(self, value):
        if value:
            return float(value)
        else:
            return None


class DataTypeText(DataType):
    name = 'text'

    def from_string(self, value):
        return value


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