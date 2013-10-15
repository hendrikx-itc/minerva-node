# -*- coding: utf-8 -*-
__docformat__ = "restructuredtext en"

__copyright__ = """
Copyright (C) 2012-2013 Hendrikx-ITC B.V.

Distributed under the terms of the GNU General Public License as published by
the Free Software Foundation; either version 3, or (at your option) any later
version.  The full license is in the file COPYING, distributed as part of
this software.
"""
import logging
from functools import partial
from contextlib import closing
import itertools
from datetime import timedelta
from operator import itemgetter, attrgetter, truth

from minerva.util import head, compose
from minerva.storage import get_plugin
from minerva.db.query import Table, Column, Literal, Argument, As, Select, \
        ands, ors, And, Eq, Gt, LtEq, GtEq, extract_tables, smart_quote, Call, \
        Function, Or, Value
from minerva.directory.helpers_v4 import get_datasource_by_id, get_entitytype_by_id
from minerva_storage_trend.granularity import create_granularity
from minerva_storage_trend.helpers import get_table_names
from minerva_storage_trend.trendstore import TrendStore
from minerva_storage_trend.tables import PARTITION_SIZES

from minerva_transform.types import FunctionSet, FunctionMapping


SCHEMA = "transform"


class NoSuchFunctionSetError(Exception):
    pass


class NoSuchFunctionMappingError(Exception):
    pass


def get_table_from_type_id(cursor, relationtype_id):
    """
    Return relation table name from relation type id
    """
    query = "SELECT name FROM relation.type WHERE id = %s"
    args = (relationtype_id,)

    cursor.execute(query, args)

    table_name, = cursor.fetchone()

    return table_name


def calc_dest_timestamp(dest_granularity, source_timestamp):
    # Hack for whole hour timestamps
    offset = timedelta(0, 1)

    plugin = get_plugin("trend")
    most_recent = plugin.get_most_recent_timestamp(source_timestamp - offset, dest_granularity)
    return plugin.get_next_timestamp(most_recent, dest_granularity)


def get_all_function_sets(conn):
    """
    Return all function sets in a list
    """
    query = (
        "SELECT id, name, description, mapping_signature, "
            "source_datasource_ids, source_entitytype_id, source_granularity, "
            "dest_datasource_id, dest_entitytype_id, dest_granularity, "
            "filter_sub_query, group_by, relation_type_id, enabled "
        "FROM {0}.function_set").format(SCHEMA)

    with closing(conn.cursor()) as cursor:
        cursor.execute(query)

        return map(partial(function_set_from_row, cursor), cursor.fetchall())


def get_function_set(cursor, id):
    query = (
        "SELECT id, name, description, mapping_signature, "
            "source_datasource_ids, source_entitytype_id, source_granularity, "
            "dest_datasource_id, dest_entitytype_id, dest_granularity, "
            "filter_sub_query, group_by, relation_type_id, enabled "
        "FROM {0}.function_set "
        "WHERE id = %s").format(SCHEMA)

    args = id,

    cursor.execute(query, args)

    if cursor.rowcount == 1:
        row = cursor.fetchone()

        return function_set_from_row(cursor, row)
    else:
        raise NoSuchFunctionSetError()


trend_table = partial(Table, 'trend')


def function_call_from_mapping(cursor, mapping_id):
    function_mapping = FunctionMapping.load(mapping_id)(cursor)

    arg_columns = ",".join(function_mapping.signature_columns)

    if function_mapping.name is None:
        if len(function_mapping.signature_columns) > 1:
            raise Exception("cannot specify multiple columns in signature without a transforming function")

        return Column(function_mapping.signature_columns[0])
    else:
        # deal with native (e.g. SUM, MAX) and custom transform functions
        if is_custom_function(cursor, SCHEMA, function_mapping.name):
            function = Function(SCHEMA, function_mapping.name)
        else:
            function = Function(function_mapping.name)

        def signature_column_to_arg(c):
            try:
                int_val = int(c)
            except ValueError:
                return Column(c)
            else:
                return Value(int_val)

        args = map(signature_column_to_arg, function_mapping.signature_columns)

        return function.call(*args)


def dest_column_from_mapping(cursor, mapping_id):
    function_mapping = FunctionMapping.load(mapping_id)(cursor)

    return function_mapping.dest_column


def retrieve(conn, function_set, timestamp):
    """
    Retrieve specific transformed data
    """
    with closing(conn.cursor()) as cursor:
        map_to_function_call = partial(function_call_from_mapping, cursor)

        column_expressions = map(map_to_function_call, function_set.mapping_signature)

    plugin = get_plugin("trend")(conn, api_version=4)

    result_rows = []
    max_modified = None

    if function_set.group_by:
        end = timestamp
        start = function_set.dest_trendstore.granularity.decr(timestamp)
        interval = (start, end)

        modified_column = Call("max", Column("modified"))

        columns = [modified_column] + column_expressions

        trendstore = function_set.source_trendstores[0]

        fetchall = partial(plugin.retrieve_aggregated, trendstore, columns,
                interval, function_set.group_by, subquery_filter=None,
                relation_table_name=function_set.relation_table)

        for row in fetchall():
            r = row[:3] + row[4:]

            result_rows.append(r)

            modified = row[3]

            if max_modified is None:
                max_modified = modified
            else:
                max_modified = max(max_modified, modified)
    else:
        table_names = get_table_names(function_set.source_trendstores, timestamp, timestamp)

        tables = map(Table, table_names)

        modified_columns = [Column(table, "modified") for table in tables]

        modified_column = Call("greatest", *modified_columns)

        columns = [modified_column] + column_expressions

        fetchall = partial(plugin.retrieve,
            function_set.source_trendstores, columns, None, timestamp, timestamp,
            subquery_filter=None, relation_table_name=function_set.relation_table)

        for row in fetchall():
            modified = row[2]

            if modified:
                r = row[:2] + row[3:]

                result_rows.append(r)

                if max_modified is None:
                    max_modified = modified
                else:
                    max_modified = max(max_modified, modified)

    return max_modified, result_rows


def store_txn(conn, transformation, transformed_rows):
    """
    Stores transformed data in destination as specified in function_set
    """
    timestamp = transformation.dest_timestamp.astimezone(transformation.function_set.dest_trendstore.datasource.tzinfo)

    with closing(conn.cursor()) as cursor:
        column_names = transformation.function_set.get_dest_columns(cursor)

    rows = [(row[0], ([transformation.function_set.id],) + row[2:])
        for row in transformed_rows if row[0]]

    plugin = get_plugin("trend")(conn, api_version=4)

    datapackage = plugin.DataPackage(transformation.function_set.dest_trendstore.granularity,
            transformation.dest_timestamp, column_names, rows)

    return plugin.store_txn(transformation.function_set.dest_trendstore, datapackage)


row_has_entity_id = compose(truth, head)


def function_set_from_row(cursor, row):
    id, name, description, mapping_signature, source_datasource_ids, \
        source_entitytype_id, source_granularity_str, dest_datasource_id, \
        dest_entitytype_id, dest_granularity_str, filter_sub_query, group_by, \
        relation_type_id, enabled = row

    get_datasource = partial(get_datasource_by_id, cursor)
    get_entitytype = partial(get_entitytype_by_id, cursor)

    source_granularity = create_granularity(str(source_granularity_str))
    dest_granularity = create_granularity(str(dest_granularity_str))

    source_datasources = map(get_datasource, source_datasource_ids)
    source_entitytype = get_entitytype(source_entitytype_id)
    dest_datasource = get_datasource(dest_datasource_id)
    dest_entitytype = get_entitytype(dest_entitytype_id)

    if relation_type_id is None:
        relation_table = None
    else:
        relation_table = get_table_from_type_id(cursor, relation_type_id)

    dest_trendstore = TrendStore.get(cursor, dest_datasource, dest_entitytype,
            dest_granularity)

    if not dest_trendstore:
        version = 3

        partition_size = PARTITION_SIZES[str(dest_granularity)]

        dest_trendstore = TrendStore(dest_datasource, dest_entitytype,
                dest_granularity, partition_size, "table").create(cursor)

        logging.info("created trendstore {}".format(dest_trendstore))

    def map_to_trendstore(cursor, datasource, entitytype, granularity):
        trendstore = TrendStore.get(cursor, datasource, source_entitytype,
                source_granularity)

        if trendstore is None:
            msg = (
                "missing trendstore for datasource {} {}, entitytype {} {}, "
                "granularity {}").format(datasource.id, datasource.name, entitytype.id,
                        entitytype.name, granularity.name)

            logging.info(msg)

        return trendstore

    source_trendstores = [map_to_trendstore(cursor, datasource, source_entitytype,
                source_granularity) for datasource in source_datasources]

    return FunctionSet(id, name, description, mapping_signature,
        source_trendstores, dest_trendstore, filter_sub_query, group_by,
        relation_table, enabled)


def is_custom_function(cursor, schema, function_name):
    """
    Returns list of custom transform functions.
    """
    query = (
        "SELECT proname FROM pg_proc "
        "JOIN pg_namespace ON pg_namespace.oid = pg_proc.pronamespace "
        "WHERE pg_namespace.nspname = %s AND pg_proc.proname = %s")

    args = schema, function_name

    cursor.execute(query, args)

    return cursor.rowcount > 0
