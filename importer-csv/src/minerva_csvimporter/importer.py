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

from minerva.util import compose

from minerva_csvimporter.util import as_functor
from minerva_csvimporter.values_extractor import ValuesExtractor


class DataError(Exception):

    """Indicating errors in the data."""

    pass


class ConfigurationError(Exception):

    """Indicating errors in the configuration."""

    pass


def import_csv(conn, profile, csv_file):
    column_names, rows = load_csv(profile, csv_file)

    profile.storage.connect(conn)
    profile.storage.store(column_names, profile.fields, rows)


def load_csv(profile, csv_file):
    """
    Return tuple (column_names, data_rows).

    column_names - a list with selected column names
    data_rows - an iterator over row tuples (dn, timestamp, values)
    """
    csv_reader = create_csv_reader(profile, csv_file)

    header = csv_reader.next()

    fields = profile.field_selector(header)
    values = ValuesExtractor(fields)

    check_header(header, fields)

    header_checks = [
        check for check in
        [
            profile.timestamp.header_check(),
            profile.identifier.header_check()
        ]
        if not check is None
    ]

    for check in header_checks:
        check(header)

    record_checks = [
        check for check in
        [
            profile.timestamp.record_check(),
            profile.identifier.record_check()
        ]
        if not check is None
    ]

    include_record = partial(record_passes_checks, record_checks)

    include_row = create_row_check(header)

    records = filter(
        include_record,
        (
            dict(zip(header, [item.decode('utf-8') for item in row]))
            for line_nr, row in enumerate(csv_reader)
            if include_row(line_nr, row)
        )
    )

    extract_raw_data_row = compose(tuple, raw_data_row_extractor(
        profile.identifier.from_record,
        profile.timestamp.from_record,
        values.from_record
    ))

    return fields, map(extract_raw_data_row, records)


def create_csv_reader(profile, csv_file):
    dialect = profile.dialect(csv_file)

    unicode_reader = codecs.getreader(profile.character_encoding)(csv_file)

    # Recoding to UTF-8 and NULL byte filtering are required for the standard
    # csv.reader: https://docs.python.org/2/library/csv.html
    utf8recoder = remove_nul(recode_utf8(unicode_reader))

    return csv.reader(utf8recoder, dialect=dialect)


def raw_data_row_extractor(*args):
    def fn(record):
        return list(map(as_functor(record), args))

    return fn


def create_row_check(expected_columns):
    field_count = len(expected_columns)

    def include_row(line_nr, row):
        value_count = len(row)

        if value_count != field_count:
            msg = (
                "Field mismatch in line {0} (expected {1} vs found {2}): {3}"
            ).format(line_nr, field_count, value_count, row)

            raise DataError(msg)

        return True

    return include_row


def record_passes_checks(checks, record):
    """Return True if record passes all checks and False otherwise."""
    return all(check(record) for check in checks)


def is_field_empty(field_name, record):
    """Return True if value of field `field_name` in `record` equals ''."""
    return record[field_name] == ""


def parse_timestamp(tzinfo, timestamp_format, timestamp_string):
    return tzinfo.localize(
        datetime.strptime(timestamp_string, timestamp_format))


def check_header(header, required_fields):
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

    missing_fields = [
        field for field in required_fields if not field in header
    ]

    if missing_fields:
        raise DataError(
            "Field(s) {} not found in header".format(missing_fields))


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


def get_alias_type_id(conn, alias_type_name):
    query = "SELECT id FROM directory.aliastype WHERE name = %s"

    with closing(conn.cursor()) as cursor:
        cursor.execute(query, (alias_type_name,))
        type_id, = cursor.fetchone()

    return type_id


def get_dn_by_entitytype_and_alias(conn, entitytype_id, alias_type_id, alias):
    query = (
        "SELECT dn FROM directory.entity e "
        "JOIN directory.alias a ON e.id = a.entity_id "
        "WHERE a.name = %s AND e.entitytype_id = %s AND a.type_id = %s")

    with closing(conn.cursor()) as cursor:
        cursor.execute(query, (alias, entitytype_id, alias_type_id))

        if cursor.rowcount == 1:
            dn, = cursor.fetchone()

            return dn
        elif cursor.rowcount == 0:
            raise ConfigurationError(
                "Identifier {} is not found".format(alias))
        elif cursor.rowcount > 1:
            raise ConfigurationError(
                "Identifier {} is not unique".format(alias))


def recode_utf8(lines):
    return (line.encode("utf-8") for line in lines)


def remove_nul(lines):
    return (line.replace("\0", "") for line in lines)
