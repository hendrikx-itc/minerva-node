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

from minerva_csvimporter.importer import ConfigurationError


class TimestampExtractor(object):
    def set_filename(self, filename):
        raise NotImplementedError()

    def set_timezone(self, tzinfo):
        raise NotImplementedError()

    def from_record(self, record):
        raise NotImplementedError()

    def check_header(self, header):
        pass


class TimestampFromColumn(object):
    def __init__(self, name, format):
        self.column_name = name
        self.timestamp_format = format
        self.tzinfo = None

    def set_filename(self, filename):
        pass

    def set_timezone(self, tzinfo):
        self.tzinfo = tzinfo

    def from_record(self, record):
        return self.tzinfo.localize(datetime.strptime(
            record[self.column_name],
            self.timestamp_format
        ))

    def check_header(self, header):
        if not self.column_name in header:
            raise Exception(
                "timestamp column '{}' not in header".format(self.column_name)
            )


class TimestampFromFilename(object):
    def __init__(self, pattern, timestamp_format):
        self.pattern = pattern
        self.regex = re.compile(pattern)
        self.timestamp_format = timestamp_format
        self.naive_timestamp = None
        self.timestamp = None

    def set_filename(self, filename):
        m = self.regex.match(filename)

        if m:
            self.naive_timestamp = datetime.strptime(
                m.group(1),
                self.timestamp_format
            )
        else:
            raise ConfigurationError(
                "Could not match timestamp pattern '{}' in filename '{}'".format(
                    self.pattern, filename))

    def set_timezone(self, tzinfo):
        self.timestamp = tzinfo.localize(self.naive_timestamp)

    def from_record(self, record):
        return self.timestamp


class TimestampNow(object):
    def __init__(self):
        self.naive_timestamp = datetime.now()
        self.timestamp = None

    def set_filename(self, filename):
        pass

    def set_timezone(self, tzinfo):
        self.timestamp = tzinfo.localize(self.naive_timestamp)

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