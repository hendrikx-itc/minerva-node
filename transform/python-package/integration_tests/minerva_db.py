# -*- coding: utf-8 -*-
__docformat__ = "restructuredtext en"

__copyright__ = """
Copyright (C) 2012-2013 Hendrikx-ITC B.V.

Distributed under the terms of the GNU General Public License as published by
the Free Software Foundation; either version 3, or (at your option) any later
version.  The full license is in the file COPYING, distributed as part of
this software.
"""
import os
import re
import logging
from contextlib import closing
import psycopg2.extras
from functools import wraps, partial

from minerva.db import parse_db_url
from minerva.db.query import Table, Column
from minerva.directory.helpers_v4 import get_entitytype, get_datasource, \
        create_entitytype, get_entity, create_entity, none_or, \
        get_entitytype_by_id, get_datasource, create_datasource
from minerva.directory.basetypes import DataSource, Entity, EntityType
import minerva.system.helpers as system_helpers
from minerva.util import first, identity
from minerva.util.debug import log_call, log_call_basic
from minerva.util.tabulate import render_table
from minerva.storage.trend import schema as trend_schema


from minerva_transform import helpers
from minerva_transform.error import ConfigurationError
from minerva_transform.types import FunctionMapping


TIMEZONE = "Europe/Amsterdam"


def connect():
    db_url = os.getenv("TEST_DB_URL")

    if db_url is None:
        raise ConfigurationError("Environment variable TEST_DB_URL not set")

    scheme, user, password, host, port, database = parse_db_url(db_url)

    if scheme != "postgresql":
        raise ConfigurationError("Only PostgreSQL connections are supported")

    conn = psycopg2.connect(database=database, user=user, password=password,
         host=host, port=port, connection_factory=psycopg2.extras.LoggingConnection)

    logging.info("connected to database {0}/{1}".format(host, database))

    conn.initialize(logging.getLogger(""))

    return conn


def reset_db(cursor):
    #cursor.execute("DELETE FROM directory.datasource")
    #cursor.execute("DELETE FROM directory.entitytype")
    trend_schema.reset(cursor)

    cursor.execute("DELETE FROM transform.function_set")
    cursor.execute("DELETE FROM transform.function_mapping")
    cursor.execute("DELETE FROM transform.state")

    cursor.execute("DELETE FROM directory.entitytype CASCADE")


def with_connection(f):
    @wraps(f)
    def decorated(*args, **kwargs):
        with closing(connect()) as conn:
            conn.commit = log_call_basic(conn.commit)
            conn.rollback = log_call_basic(conn.rollback)
            return f(conn, *args, **kwargs)

    return decorated


def render_result(cursor):
    column_names = [c.name for c in cursor.description]
    column_align = ">" * len(column_names)
    column_sizes = ["max"] * len(column_names)
    rows = cursor.fetchall()

    return render_table(column_names, column_align, column_sizes, rows)


def add_dummy_entitytype(cursor, id, name):
    query = (
        "INSERT INTO directory.entitytype "
            "(id, name, description) "
        "VALUES "
            "(%s, %s, '')")

    args = (id, name)

    cursor.execute(query, args)

    return EntityType(id, name, '')


def get_dummy_entitytype(cursor, name):
    et = get_entitytype(cursor, name)

    if et:
        return et
    else:
        return create_entitytype(cursor, name, "")


def get_dummy_datasource(cursor, name):
    datasource = get_datasource(cursor, name)

    if not datasource:
        datasource = create_datasource(cursor, name, "Dummy source for integration test", TIMEZONE, "trend")

    return datasource


def format_array(values):
    return "{{}}".format(",".join(map(str, values)))


def add_function_set(cursor, *args):
    col_names = ["name", "description", "mapping_signature", "source_datasource_ids",
        "source_entitytype_id", "source_granularity", "dest_datasource_id",
        "dest_entitytype_id", "dest_granularity", "filter_sub_query", "group_by",
        "relation_type_id", "enabled"]

    columns = map(Column, col_names)

    table = Table("transform", "function_set")

    insert_query = table.insert(columns).returning("id")
    insert_query.execute(cursor, args)

    id, = cursor.fetchone()

    row = (id,) + args

    return helpers.function_set_from_row(cursor, row)


def add_function_mapping(cursor, *args):
    query = (
        "INSERT INTO transform.function_mapping "
            "(function_name, signature_columns, dest_column) "
        "VALUES "
            "(%s, %s, %s) "
        "RETURNING id")

    cursor.execute(query, args)

    id, = cursor.fetchone()

    return FunctionMapping.load(id)(cursor)


def get_or_create_entity(cursor, dn):
    value = get_entity(cursor, dn)

    if_none = partial(create_entity, cursor, dn)

    return none_or(if_none, identity)(value)
