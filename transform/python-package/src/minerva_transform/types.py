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
from contextlib import closing
import itertools
from datetime import timedelta
from operator import itemgetter, attrgetter, truth
from functools import partial
from itertools import repeat

from minerva import storage
from minerva.util import zipapply
from minerva.util import head, compose
from minerva.util.tabulate import render_table
from minerva.directory.helpers_v4 import get_datasource_by_id, \
    get_entitytype_by_id
from minerva.storage.trend.granularity import create_granularity
from minerva.storage.trend.helpers import get_table_names
from minerva.storage.trend.tables import PARTITION_SIZES
from minerva.storage import get_plugin
from minerva.db.dbtransaction import DbAction
from minerva.db.query import Table, Column, Literal, Argument, As, Select, \
    ands, ors, And, Eq, Gt, LtEq, GtEq, extract_tables, smart_quote, Call, \
    Function, Or, Value
from minerva.directory.helpers_v4 import get_datasource_by_id, \
    get_entitytype_by_id


SCHEMA = "transform"


class MinervaContext(object):
    """
    Maintains a Minerva context, including things like a connection to the
    writable Minerva database, a connection to the readable Minerva database and
    current Minerva process Id.
    """
    def __init__(self, writer_conn, reader_conn):
        self.writer_conn = writer_conn
        self.reader_conn = reader_conn
        self.storage_providers = dict([(name, p(writer_conn)) for name, p in storage.load_plugins().iteritems()])

        for name in self.storage_providers.keys():
            logging.info("loaded storage plugin '{}'".format(name))


class Transformation(object):
    def __init__(self, function_set, dest_timestamp):
        self.function_set = function_set
        self.dest_timestamp = dest_timestamp

    def execute(self, minerva_context):
        datasource = self.function_set.source_trendstores[0].datasource
        timestamp_with_timezone = self.dest_timestamp.astimezone(datasource.tzinfo)

        max_modified, rows = retrieve(minerva_context.reader_conn, self.function_set,
                timestamp_with_timezone)

        minerva_context.reader_conn.commit()

        transaction = store_txn(minerva_context.writer_conn, self, rows)

        if max_modified:
            transaction.append(UpdateProcessedMaxModified(self, max_modified))

        transaction.run(minerva_context.writer_conn)

        logging.info("'{0}'(id: {1}) transformation resulted in {2} records "
            "for timestamp: {3}".format(
            self.function_set.name, self.function_set.id, len(rows),
            self.dest_timestamp.isoformat()))


class View(object):
    """
    Meta data for creation of views
    """
    def __init__(self):
        self.id = None
        self.datasource = None
        self.entitytype = None
        self.granularity = None
        self.sources = []
        self.sql = None
        self.description = None

    def __str__(self):
        lines = ["<{}, {}, {}>".format(self.datasource.name, self.entitytype.name, self.granularity)]

        for s in self.sources:
            lines.append(" -- {}".format(s))

        return "\n".join(lines)

    def compile_sql(self, timestamp, get_table_name):
        mapping = [(s.name, get_table_name(s.datasource, s.granularity, s.entitytype.name, timestamp)) for s in self.sources]

        result = self.sql

        for name, table_name in mapping:
            result = result.replace("{{source:{}}}".format(name), table_name)

        return result


class ViewSource(object):
    """
    Meta data for the source tables of views
    """
    def __init__(self):
        self.id = None
        self.name = None
        self.datasource = None
        self.entitytype = None
        self.granularity = None

    def __str__(self):
        return "<{}, {}, {}, {}>".format(self.name, self.datasource.name, self.entitytype.name, self.granularity)


class FunctionMapping(object):
    def __init__(self, id, name, signature_columns, dest_column):
        self.id = id
        self.name = name
        self.signature_columns = signature_columns
        self.dest_column = dest_column

    @staticmethod
    def load(function_mapping_id):
        """
        Return function mapping tuple (function name, signature, dest_column)
        """
        def f(cursor):
            query = (
                "SELECT function_name, signature_columns, dest_column "
                "FROM {0}.function_mapping "
                "WHERE id = %s").format(SCHEMA)

            args = function_mapping_id,

            cursor.execute(query, args)

            if cursor.rowcount > 0:
                name, signature_columns, dest_column = cursor.fetchone()

                return FunctionMapping(function_mapping_id, name, signature_columns, dest_column)

        return f


class FunctionSet(object):
    def __init__(self, id, name, description, mapping_signature,
            source_trendstores, dest_trendstore, filter_sub_query, group_by,
            relation_table, enabled):

        self.id = id
        self.name = name
        self.description = description
        self.mapping_signature = mapping_signature
        self.source_trendstores = source_trendstores
        self.dest_trendstore = dest_trendstore
        self.filter_sub_query = filter_sub_query
        self.group_by = group_by
        self.relation_table = relation_table
        self.enabled = enabled

    def source_granularity(self):
        return self.source_trendstores[0].granularity

    def source_entitytype(self):
        return self.source_trendstores[0].entitytype

    def __eq__(self, other):
        return self.id == other.id

    def __lt__(self, other):
        return self.id < other.id

    def __str__(self):
        return self.name

    def source_table_names(self, cursor, timestamp):
        """
        Return source table names based on source items and timestamp
        """
        views = get_views(cursor)

        source_datasource_ids = [trendstore.datasource.id for trendstore in self.source_trendstores]

        def view_matches_trendstore(view, trendstore):
            return view.datasource.id == trendstore.datasource.id \
                and view.entitytype.id == trendstore.entitytype.id \
                and view.granularity.name == trendstore.granularity.name

        matching_views = [view for view in views if any(view_matches_trendstore(view, trendstore) for trendstore in self.source_trendstores)]

        table_names = [trendstore.make_table_name(timestamp)
                for trendstore in self.source_trendstores]

        plugin = get_plugin("trend")

        for view in matching_views:
            for source in view.sources:
                table_name = plugin.get_table_name(source.datasource, source.granularity, source.entitytype.name, timestamp)
                table_names.append(table_name)

        return table_names

    def has_type_mapping(self):
        return self.source_trendstores[0].entitytype.id != self.dest_trendstore.entitytype.id

    def dest_table_name(self, timestamp):
        """
        Return destination table name based on dest items and timestamp
        """
        return self.dest_trendstore.make_table_name(timestamp)

    def dest_partition(self, timestamp):
        return self.dest_trendstore.partition(timestamp)

    def transforming_columns(self):
        if self.group_by:
            return ["function_set_ids", "samples"]
        else:
            return ["function_set_ids"]

    def get_dest_columns(self, cursor):
        mapping_loaders = map(FunctionMapping.load, self.mapping_signature)

        mappings = zipapply(mapping_loaders, repeat(cursor, len(mapping_loaders)))

        mapping_columns = [m.dest_column for m in mappings]

        return self.transforming_columns() + mapping_columns


def get_views(cursor):
    query = (
        "SELECT id, description, datasource_id, entitytype_id, granularity, sql "
        "FROM trend.view")

    cursor.execute(query)

    return map(partial(view_from_row, cursor), cursor.fetchall())


def view_from_row(cursor, row):
    id, description, datasource_id, entitytype_id, granularity, sql = row

    view = View()
    view.id = id
    view.sql = sql
    view.description = description
    view.datasource = get_datasource_by_id(cursor, datasource_id)
    view.entitytype = get_entitytype_by_id(cursor, entitytype_id)
    view.granularity = create_granularity(granularity)

    view.sources = get_sources_for_view(cursor, id)

    return view


def get_sources_for_view(cursor, view_id):
    query = (
        "SELECT id, name, datasource_id, entitytype_id, granularity "
        "FROM trend.view_source vs "
        "JOIN trend.view_source_link vsl ON vsl.view_source_id = vs.id "
        "WHERE vsl.view_id = %s")

    args = (view_id, )

    cursor.execute(query, args)

    return map(partial(view_source_from_row, cursor), cursor.fetchall())


def view_source_from_row(cursor, row):
    id, name, datasource_id, entitytype_id, granularity = row

    view_source = ViewSource()
    view_source.id = id
    view_source.name = name
    view_source.datasource = get_datasource_by_id(cursor, datasource_id)
    view_source.entitytype = get_entitytype_by_id(cursor, entitytype_id)
    view_source.granularity = granularity
    return view_source


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

    dest_trendstore = get_trendstore(cursor, dest_datasource, dest_entitytype,
            dest_granularity)

    if not dest_trendstore:
        version = 4

        partition_size = PARTITION_SIZES[str(dest_granularity)]

        dest_trendstore = create_trendstore(cursor, dest_datasource, dest_entitytype,
                dest_granularity, partition_size, "table", version)

        logging.info("created trendstore {}".format(dest_trendstore))

    def map_to_trendstore(cursor, datasource, entitytype, granularity):
        trendstore = get_trendstore(cursor, datasource, source_entitytype,
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


class UpdateProcessedMaxModified(DbAction):
    def __init__(self, transformation, processed_max_modified):
        self.transformation = transformation
        self.processed_max_modified = processed_max_modified

    def execute(self, cursor, state):
        update_processed_max_modified(cursor, self.transformation.function_set,
                self.transformation.dest_timestamp, self.processed_max_modified)


def update_processed_max_modified(cursor, function_set, dest_timestamp, processed_max_modified):
    query = (
        "UPDATE transform.state SET processed_max_modified = %s "
        "WHERE function_set_id = %s AND dest_timestamp = %s")

    args = (processed_max_modified, function_set.id, dest_timestamp)

    cursor.execute(query, args)

    if cursor.rowcount == 0:
        query = (
            "INSERT INTO transform.state(function_set_id, dest_timestamp, processed_max_modified, max_modified) "
            "VALUES(%s, %s, %s, %s)")
        args = (function_set.id, dest_timestamp, processed_max_modified, processed_max_modified)

        cursor.execute(query, args)


def render_function_set_table(function_sets):
    def make_table_row(function_set):
        enabled = 'Yes' if function_set.enabled else 'No'

        return [function_set.id, function_set.name, function_set.source_entitytype().name,
                function_set.source_granularity(), "->", function_set.dest_trendstore.entitytype.name,
                function_set.dest_trendstore.granularity, enabled]

    rows = map(make_table_row, function_sets)

    column_names = ["id", "name", "src entitytype", "src granularity", "->",
            "dest entitytype", "dest granularity", "enabled"]
    column_align = "><<><<><"
    column_sizes = ["max"] * len(column_names)

    table = render_table(column_names, column_align, column_sizes, rows)

    lines = table + ["({} function sets)".format(len(function_sets))]

    return "\n".join(lines)
