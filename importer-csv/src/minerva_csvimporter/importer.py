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
from datetime import datetime, timedelta
import re
import operator
from contextlib import closing
from functools import partial
import logging.handlers
import codecs

import pytz

from minerva.util import compose, identity, k, grouped_by
from minerva.storage import get_plugin
from minerva.directory.helpers import NoSuitablePluginError
from minerva.directory.helpers_v4 import name_to_datasource, name_to_entitytype
from minerva.directory.distinguishedname import explode

from minerva_csvimporter import dialects


class DataError(Exception):

    """Indicating errors in the data."""

    pass


class ConfigurationError(Exception):

    """Indicating errors in the configuration."""

    pass


def import_csv(conn, profile, datasource_name, storagetype, timestamp, csvfile,
               file_name):
    alias_type = "name"

    granularity = profile["granularity"]
    identifier = profile["identifier"]
    timestampformat = profile["timestamp_format"]
    timestampcolumn = profile["timestamp_column"]
    identifier_regex = profile["identifier_regex"]
    identifier_is_alias = profile["identifier_is_alias"]
    fields = [f for f in profile["fields"] if f]
    ignore_fields = profile["ignore_fields"]
    ignore_field_mismatches = profile["ignore_field_mismatches"]
    timestamp_is_start = profile["timestamp_is_start"]
    character_encoding = profile["character_encoding"]
    dialect = profile["dialect"]
    value_mapping = profile["value_mapping"]
    timestamp_from_filename_regex = profile["timestamp_from_filename_regex"]

    logging.debug("\n".join([
        "Importing: {}.".format(csvfile),
        "Granularity: {}".format(granularity),
        "Identifier: {}".format(identifier),
        "Data source: {}".format(datasource_name),
        "Timestamp: {}".format(timestamp)]))

    if dialect == "auto":
        dialect = get_dialect(csvfile)
    elif dialect == "prime":
        dialect = dialects.Prime()

    unicode_reader = codecs.getreader(character_encoding)(csvfile)

    utf8recoder = remove_nul(recode_utf8(unicode_reader))

    csv_reader = csv.reader(utf8recoder, dialect=dialect)

    if not fields:
        fields = csv_reader.next()
        unicode_reader.seek(0)

    for ignore_field in ignore_fields:
        try:
            del(fields)[fields.index(ignore_field)]
        except ValueError:
            continue

    entitytype_name = explode(identifier)[-1][0]

    with closing(conn.cursor()) as cursor:
        datasource = name_to_datasource(cursor, datasource_name)
        entitytype = name_to_entitytype(cursor, entitytype_name)

    conn.commit()

    if identifier_is_alias:
        alias_type_id = get_alias_type_id(conn, alias_type)
        identifier_to_dn = partial(
            get_dn_by_entitytype_and_alias, conn, entitytype.id, alias_type_id)
    else:
        # Assumed that identifier is distinguished name
        identifier_to_dn = identity

    parse_ts = partial(parse_timestamp, datasource.tzinfo, timestampformat)

    if timestamp_is_start:
        offset = timedelta(0, granularity)

        parse_ts = compose(partial(offset_timestamp, offset), parse_ts)

    record_checks = []

    identifier_fields = re.findall(r"{(\w+)}", identifier)

    #composed identifier (e.g. '{fld1}-{fld2}, {fld1}:{fld2}')
    is_identifier_available = compose(
        operator.not_, partial(any_field_empty, identifier_fields))

    record_checks.append(is_identifier_available)

    if timestampcolumn and timestamp:
        # Use timestamp found in timestamp column, when empty use 'timestamp'
        get_timestamp_value = partial(get_value_by_key, timestampcolumn,
                                      default=timestamp)

    elif timestampcolumn:
        get_timestamp_value = partial(get_value_by_key, timestampcolumn)
        is_timestamp_field_not_empty = compose(
            operator.not_, partial(is_field_empty, timestampcolumn))
        record_checks.append(is_timestamp_field_not_empty)
    elif timestamp:
        get_timestamp_value = k(timestamp)
    elif timestamp_from_filename_regex:
        m = re.match(timestamp_from_filename_regex, file_name)

        if m:
            get_timestamp_value = k(m.group(1))
        else:
            raise ConfigurationError(
                "Could not match timestamp pattern '{}' "
                "in filename '{}'".format(
                    timestamp_from_filename_regex, file_name))
    else:
        raise ConfigurationError(
            "No timestamp or column with timestamp found: "
            "please specify timestamp or timestampcolumn")

    include_record = partial(record_passes_checks, record_checks)

    def include_row(line_nr, column_names, row):
        field_count = len(column_names)
        value_count = len(row)

        include = True

        if value_count != field_count:
            msg = "Field mismatch in line {0} ({1} vs {2}): {3}".format(
                line_nr, field_count, value_count, row)

            if ignore_field_mismatches:
                logging.info(msg)
                include = False
            else:
                raise DataError(msg)

        return include

    extract_ident = partial(extract_identifier, identifier_regex)

    if identifier_fields:
        #composed identifier (e.g. '{fld1}-{fld2}, {fld1}:{fld2}')
        get_identifier = compose(
            extract_ident,
            partial(get_value_by_identifier_format, identifier))
    else:
        get_identifier = compose(
            extract_ident,
            partial(get_value_by_key, identifier))

    get_dn_from_record = compose(identifier_to_dn, get_identifier)

    if value_mapping:
        extract_values = mapping_values_extractor(fields, value_mapping)
    else:
        extract_values = plain_values_extractor(fields)

    get_timestamp = compose(parse_ts, get_timestamp_value)

    extract_raw_data_row = raw_data_row_extractor(
        get_dn_from_record, get_timestamp, extract_values)

    records = [record
               for record in read_records(csv_reader, fields, include_row)
               if include_record(record)]

    raw_data_rows = map(extract_raw_data_row, records)

    plugin = init_plugin(conn, storagetype)

    if storagetype == "attribute":
        raw_datapackage = plugin.RawDataPackage(fields, raw_data_rows)

        plugin.store_raw(datasource, raw_datapackage)

    elif storagetype == "trend":
        granularity = plugin.create_granularity(granularity)

        for timestamp, grouped_rows in grouped_by(raw_data_rows, operator.itemgetter(1)):
            rows = [
                (dn, values)
                for dn, _, values in grouped_rows
            ]

            utc_timestamp = timestamp.astimezone(pytz.utc)
            utc_timestamp_str = utc_timestamp.strftime("%Y-%m-%dT%H:%M:%S")

            raw_datapackage = plugin.RawDataPackage(
                granularity, utc_timestamp_str, fields, rows)

            plugin.store_raw(datasource, raw_datapackage)
    else:
        raise Exception("Unsupported storage class: {}".format(storagetype))


def raw_data_row_extractor(extract_dn, extract_timestamp, extract_values):
    def fn(record):
        return extract_dn(record), extract_timestamp(record), extract_values(record)

    return fn


def mapping_values_extractor(fields, value_mapping):
    def fn(record):
        return [
            value_mapping.get(field, {}).get(record[field], record[field])
            for field in fields
        ]

    return fn


def plain_values_extractor(fields):
    def fn(record):
        return [record[field] for field in fields]

    return fn


def record_passes_checks(checks, record):
    """Return True if record passes all checks and False otherwise."""
    return all(check(record) for check in checks)


def is_field_empty(field_name, record):
    """Return True if value of field `field_name` in `record` equals ''."""
    return record[field_name] == ""


def any_field_empty(field_names, record):
    """Return True if value of one of `field_names` in `record` equals ''."""
    return any(record[field_name] == "" for field_name in field_names)


def get_value_by_key(key, record, default=None):
    """
    Return value with key `key` from `record` or otherwise `default`.

    This has the same function as dict.get, but with different argument order
    to support partial application.

    """
    v = record[key]
    if v:
        return v
    else:
        return default


def get_value_by_identifier_format(identifier_format, record):
    return identifier_format.format(**record)


def get_value_by_index(index, iterable):
    return iterable[index]


def init_plugin(conn, storagetype):
    plugin = get_plugin(storagetype)(conn, api_version=4)

    if not plugin:
        raise NoSuitablePluginError(
            "Missing storage plugin {}".format(storagetype))

    return plugin


def offset_timestamp(offset, timestamp):
    ts_with_offset = (timestamp.astimezone(pytz.utc) + offset).astimezone(
        timestamp.tzinfo)
    #Deal with DST
    utc_offset_delta = timestamp.utcoffset() - ts_with_offset.utcoffset()

    return ts_with_offset + utc_offset_delta


def parse_timestamp(tzinfo, timestamp_format, timestamp_string):
    return tzinfo.localize(
        datetime.strptime(timestamp_string, timestamp_format))


def extract_identifier(pattern, value):
    regex = re.compile(pattern)

    m = regex.match(value)

    return "".join(m.groups())


def get_dialect(csv_file):
    sample = csv_file.read(128000)
    csv_file.seek(0)

    return csv.Sniffer().sniff(sample)


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
