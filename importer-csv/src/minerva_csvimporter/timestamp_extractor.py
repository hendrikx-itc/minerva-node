# -*- coding: utf-8 -*-
__docformat__ = "restructuredtext en"

__copyright__ = """
Copyright (C) 2014 Hendrikx-ITC B.V.

Distributed under the terms of the GNU General Public License as published by
the Free Software Foundation; either version 3, or (at your option) any later
version.  The full license is in the file COPYING, distributed as part of
this software.
"""
import re
from datetime import datetime

import pytz

from minerva_csvimporter.importer import ConfigurationError
from minerva_csvimporter.data_extractor import DataExtractor


class TimestampExtractor(DataExtractor):
    def set_filename(self, filename):
        raise NotImplementedError()


class TimestampFromColumn(TimestampExtractor):
    def __init__(self, name, timezone, format):
        self.column_name = name
        self.timezone = timezone
        self.timestamp_format = format
        self.tzinfo = pytz.timezone(timezone)

    def set_filename(self, filename):
        pass

    def from_record(self, record):
        return self.tzinfo.localize(datetime.strptime(
            record[self.column_name],
            self.timestamp_format
        ))

    def header_check(self):
        def check_for_column_name(header):
            if not self.column_name in header:
                raise Exception(
                    "timestamp column '{}' not in header".format(self.column_name)
                )

        return check_for_column_name


class TimestampFromFilename(object):
    def __init__(self, pattern, timezone, timestamp_format):
        self.pattern = pattern
        self.regex = re.compile(pattern)
        self.timezone = timezone
        self.tzinfo = pytz.timezone(timezone)
        self.timestamp_format = timestamp_format
        self.timestamp = None

    def set_filename(self, filename):
        m = self.regex.match(filename)

        if m:
            naive_timestamp = datetime.strptime(
                m.group(1),
                self.timestamp_format
            )
        else:
            raise ConfigurationError(
                "Could not match timestamp pattern '{}' in filename '{}'".format(
                    self.pattern, filename))

        self.timestamp = self.tzinfo.localize(naive_timestamp)

    def from_record(self, record):
        return self.timestamp


class TimestampNow(object):
    def __init__(self):
        self.timestamp = pytz.utc.localize(datetime.utcnow())

    def set_filename(self, filename):
        pass

    def from_record(self, record):
        return self.timestamp


timestamp_functions = {
    "from_column": TimestampFromColumn,
    "from_filename": TimestampFromFilename,
    "now": TimestampNow
}


def create_timestamp_fn(t):
    type_name = t["type"]

    try:
        return timestamp_functions[type_name](**t.get("config", {}))
    except KeyError:
        raise Exception("No such timestamp function: {}".format(type_name))