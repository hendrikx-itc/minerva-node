from future_builtins import map, filter
# -*- coding: utf-8 -*-
"""Provides main csv importing function `import_csv`."""
__docformat__ = "restructuredtext en"

__copyright__ = """
Copyright (C) 2008-2013 Hendrikx-ITC B.V.

Distributed under the terms of the GNU General Public License as published by
the Free Software Foundation; either version 3, or (at your option) any later
version.  The full license is in the file COPYING, distributed as part of
this software.
"""
import csv
from datetime import datetime
from contextlib import closing
from functools import partial
import logging.handlers
import codecs
import operator

import pytz

from minerva.util import compose, identity
from minerva.directory.helpers_v4 import name_to_datasource


class DataError(Exception):

    """Indicating errors in the data."""

    pass


class ConfigurationError(Exception):

    """Indicating errors in the configuration."""

    pass


def import_csv(conn, profile, datasource_name, csvfile):
    storage = profile["storage"]

    logging.debug("Storage: {}".format(storage))
    logging.debug("Data source: {}".format(datasource_name))

    with closing(conn.cursor()) as cursor:
        datasource = name_to_datasource(cursor, datasource_name)

    conn.commit()

    column_names, raw_data_rows = load_csv(profile, datasource, csvfile)

    storage.connect(conn)

    storage.store_raw(datasource, column_names, raw_data_rows)


def load_csv(profile, datasource, csvfile):
    identifier = profile["identifier"]
    timestamp = profile["timestamp"]
    field_selector = profile["field_selector"]
    ignore_field_mismatches = profile["ignore_field_mismatches"]
    character_encoding = profile["character_encoding"]
    dialect = profile["dialect"]
    value_mapping = profile["value_mapping"]

    logging.debug("\n".join([
        "Importing: {}.".format(csvfile),
        "Identifier: {}".format(identifier),
        "Timestamp: {}".format(timestamp)]))

    dialect = dialect(csvfile)

    unicode_reader = codecs.getreader(character_encoding)(csvfile)

    utf8recoder = remove_nul(recode_utf8(unicode_reader))

    csv_reader = csv.reader(utf8recoder, dialect=dialect)

    fields = field_selector(csv_reader.next())

    timestamp.check_header(fields)

    unicode_reader.seek(0)

    timestamp.set_timezone(datasource.tzinfo)

    record_checks = list(identifier.record_requirements())

    include_record = record_passes_checks(record_checks)

    def include_row(line_nr, column_names, row):
        field_count = len(column_names)
        value_count = len(row)

        include = True

        if value_count != field_count:
            msg = (
                "Field mismatch in line {0} (expected {1} vs found {2}): "
                "{3}").format(
                    line_nr, field_count, value_count, row)

            if ignore_field_mismatches:
                logging.info(msg)
                include = False
            else:
                raise DataError(msg)

        return include


    extract_values = apply_all(create_values_extrators(fields, value_mapping))

    extract_raw_data_row = compose(tuple, apply_all((
        identifier.get_dn_from_record, timestamp.from_record, extract_values
    )))

    records = filter(
        include_record,
        read_records(csv_reader, fields, include_row)
    )

    return fields, map(extract_raw_data_row, records)


def create_values_extrators(fields, value_mapping):
    for field in fields:
        get_value = itemgetter(field)

        mapping = value_mapping.get(field)

        if mapping:
            yield compose(mapping, get_value)
        else:
            yield get_value


def apply_all(fs):
    def fn(value):
        return map(as_functor(value), fs)

    return fn


def as_functor(x):
    def fmap(f):
        return f(x)

    return fmap


def mapping_values_extractor(fields, value_mapping):
    def fn(record):
        return [
            value_mapping.get(field, {}).get(record[field], record[field])
            for field in fields
        ]

    return fn


def record_passes_checks(checks):
    """Return True if record passes all checks and False otherwise."""
    return compose(all, apply_all(checks))


def is_field_empty(field_name, record):
    """Return True if value of field `field_name` in `record` equals ''."""
    return record[field_name] == ""


def offset_timestamp(offset, timestamp):
    ts_with_offset = (timestamp.astimezone(pytz.utc) + offset).astimezone(
        timestamp.tzinfo)
    #Deal with DST
    utc_offset_delta = timestamp.utcoffset() - ts_with_offset.utcoffset()

    return ts_with_offset + utc_offset_delta


def parse_timestamp(tzinfo, timestamp_format, timestamp_string):
    return tzinfo.localize(
        datetime.strptime(timestamp_string, timestamp_format))


def check_header(header):
    """
    Return None if header is OK, otherwise raise exception.
    """
    if any(len(field) == 0 for field in header):
        raise DataError(
            "Empty field name in header: {}".format(",".join(header)))

    duplicates = get_duplicates(header)

    if duplicates:
        raise DataError("Ambiguous field(s) found in header: {}.".format(
            ", ".join(duplicates)))


def get_duplicates(strings):
    """
    Return list with duplicate strings using case insensitive matching.
    """
    unique = set()
    duplicates = []

    for string in strings:
        lower_string = string.lower()

        if not lower_string in unique:
            unique.add(lower_string)
        else:
            duplicates.append(string)

    return duplicates


def read_records(csv_reader, fields, include_row):
    column_names = csv_reader.next()

    check_header(column_names)

    missing_fields = [field for field in fields if not field in column_names]

    if missing_fields:
        raise DataError(
            "Field(s) {} not found in header".format(missing_fields))

    for line_nr, row in enumerate(csv_reader):
        if include_row(line_nr, column_names, row):
            yield dict(
                zip(column_names, (item.decode('utf-8') for item in row))
            )


def recode_utf8(lines):
    return (line.encode("utf-8") for line in lines)


def remove_nul(lines):
    return (line.replace("\0", "") for line in lines)
