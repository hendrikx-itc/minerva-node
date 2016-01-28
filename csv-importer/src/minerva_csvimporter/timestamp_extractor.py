# -*- coding: utf-8 -*-
import re
from datetime import datetime

import pytz

from minerva.util import k

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
            if self.column_name not in header:
                raise Exception(
                    "timestamp column '{}' not in header".format(
                        self.column_name
                    )
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
                "Could not match timestamp pattern '{}' in "
                "filename '{}'".format(
                    self.pattern, filename
                )
            )

        self.timestamp = self.tzinfo.localize(naive_timestamp)

    def from_record(self, record):
        return self.timestamp


class TimestampNow(object):
    def __init__(self):
        self.timestamp = pytz.utc.localize(datetime.utcnow())

    def set_filename(self, filename):
        pass

    def header_check(self):
        def check_nothing(header):
            pass

        return check_nothing

    def record_check(self):
        return k(True)

    def from_record(self, record):
        return self.timestamp


class TimestampFixed(object):
    def __init__(self, timestamp, format):
        self.timestamp = timestamp
        self.format = format

    def set_filename(self, filename):
        pass

    def header_check(self):
        def check_for_column_name(header):
            pass

        return check_for_column_name

    def from_record(self, record):
        return pytz.timezone('Europe/Amsterdam').localize(
            datetime.strptime(self.timestamp, self.format))

    def record_check(self):
        return k(True)


timestamp_functions = {
    "from_column": TimestampFromColumn,
    "from_filename": TimestampFromFilename,
    "fixed": TimestampFixed,
    "now": TimestampNow
}


def create_timestamp_fn(t):
    type_name = t["type"]

    try:
        return timestamp_functions[type_name](**t.get("config", {}))
    except KeyError:
        raise Exception("No such timestamp function: {}".format(type_name))