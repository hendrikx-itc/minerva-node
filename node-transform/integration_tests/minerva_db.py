import os
import re
import logging
from contextlib import closing
import psycopg2.extras
from functools import wraps, partial

from minerva.db import parse_db_url
from minerva.db.query import Table, Column
from minerva.directory.helpers_v4 import get_entitytype, get_datasource, \
        create_entitytype, get_entity, create_entity, none_or
from minerva.directory.basetypes import DataSource, Entity, EntityType
import minerva.system.helpers as system_helpers
from minerva.util import first, identity
from minerva.util.tabulate import render_table


from minerva_node_transform import helpers


QUERY_SEP = "\n"
TIMEZONE = "Europe/Amsterdam"


def connect():
    db_url = os.getenv("TEST_DB_URL")

    if db_url is None:
        raise Exception("Environment variable TEST_DB_URL not set")

    scheme, user, password, host, port, database = parse_db_url(db_url)

    if scheme != "postgresql":
        raise Exception("Only PostgreSQL connections are supported")

    conn = psycopg2.connect(database=database, user=user, password=password,
         host=host, port=port, connection_factory=psycopg2.extras.LoggingConnection)

    logging.info("connected to database {0}/{1}".format(host, database))

    conn.initialize(logging.getLogger(""))

    return conn


def with_connection(f):
    @wraps(f)
    def decorated(*args, **kwargs):
        with closing(connect()) as conn:
            return f(conn, *args, **kwargs)

    return decorated


def render_result(cursor):
    column_names = [c.name for c in cursor.description]
    column_align = ">" * len(column_names)
    column_sizes = ["max"] * len(column_names)
    rows = cursor.fetchall()

    return render_table(column_names, column_align, column_sizes, rows)


def clear_database(conn):
    with closing(conn.cursor()) as cursor:
        pass


def drop_tables(cursor, schema, table_name_regex):
    regex = re.compile(table_name_regex)

    tables = [table for table in get_tables(cursor, schema) if regex.match(table)]

    for table_name in tables:
        drop_table(cursor, schema, table_name)

        logging.info("dropped table {0}".format(table_name))


def get_tables(cursor, schema):
    query = QUERY_SEP.join([
        "SELECT table_name",
        "FROM information_schema.tables",
        "WHERE table_schema='{0}'".format(schema)])

    cursor.execute(query)

    return [table_name for table_name, in cursor.fetchall()]


def drop_table(cursor, schema, table):
    cursor.execute("DROP TABLE {0}.{1}".format(schema, table))


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


def add_dummy_datasource(cursor, id, name):
    query = (
        "INSERT INTO directory.datasource "
            "(id, name, description, timezone, storagetype) "
        "VALUES "
            "(%s, %s, '', %s, 'trend')")

    args = (id, name, TIMEZONE)

    cursor.execute(query, args)

    return DataSource(id, name, '', TIMEZONE)


def get_dummy_datasource(cursor, name):
    ds = get_datasource(cursor, name)

    if ds:
        return ds
    else:
        return add_dummy_datasource(cursor, name)


def format_array(values):
    return "{{}}".format(",".join(map(str, values)))


def add_function_set(cursor, *args):
    col_names = ["id", "name", "mapping_signature", "source_datasource_ids",
        "source_entitytype_id", "source_granularity", "dest_datasource_id",
        "dest_entitytype_id", "dest_granularity", "filter_sub_query", "group_by", "enabled"]

    columns = map(Column, col_names)

    table = Table("transform", "function_set")

    table.insert(columns).execute(cursor, args)


def get_function_set(cursor, *args):
    id = first(args)

    try:
        return helpers.get_function_set(cursor, id)
    except helpers.NoSuchFunctionSetError:
        add_function_set(cursor, *args)

        return helpers.get_function_set(cursor, id)


def add_function_mapping(cursor, *args):
    query = (
        "INSERT INTO transform.function_mapping "
            "(id, function_name, signature_columns, dest_column) "
        "VALUES "
            "(%s, %s, %s, %s)")

    cursor.execute(query, args)


def get_or_create_entity(cursor, dn):
    value = get_entity(cursor, dn)

    if_none = partial(create_entity, cursor, dn)

    return none_or(if_none, identity)(value)


def get_function_mapping(cursor, *args):
    id = first(args)

    try:
        return helpers.get_function_mapping(cursor, id)
    except:
        add_function_mapping(cursor, *args)

        return helpers.get_function_mapping(cursor, id)
