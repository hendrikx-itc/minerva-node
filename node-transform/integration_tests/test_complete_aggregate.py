from pkg_resources import iter_entry_points
from contextlib import closing
from datetime import datetime
from functools import partial

import pytz

from nose.tools import eq_, raises, assert_not_equal, assert_raises, with_setup

from minerva.util import first, head, tail, k, compose
from minerva.storage import get_plugin
from minerva.db.query import Table, Column, Insert, Truncate, Select, Value, Eq, \
        Argument, And, table_exists, SqlType

from minerva.node import MinervaContext

from minerva_db import connect, clear_database, get_tables, with_connection, \
        get_dummy_datasource, get_dummy_entitytype, TIMEZONE, get_function_set, \
        get_function_mapping, render_result
from trend import render_source, get_entitytype

ENTRYPOINT = "node.plugins"


def get_entrypoint():
    return first(list(iter_entry_points(group=ENTRYPOINT, name="transform")))


def load_plugin():
    """
    Load and return the plugin.
    """
    entrypoint = get_entrypoint()

    return entrypoint.load()

tzinfo = pytz.timezone(TIMEZONE)

local_timestamp = compose(tzinfo.localize, datetime)

src_timestamp_1 = local_timestamp(2012, 12, 11, 13, 15, 0)
src_timestamp_2 = local_timestamp(2012, 12, 11, 13, 30, 0)
src_timestamp_3 = local_timestamp(2012, 12, 11, 13, 45, 0)
src_timestamp_4 = local_timestamp(2012, 12, 11, 14, 0, 0)

modified_a = local_timestamp(2012, 12, 11, 14, 3, 27)
modified_b = local_timestamp(2012, 12, 11, 14, 7, 14)

dest_timestamp = local_timestamp(2012, 12, 11, 14, 0, 0)

source_1 = [
    ("entity_id", "timestamp", "modified", "counter_a", "counter_b"),
    (1000, src_timestamp_1, modified_a, 4, 0),
    (1000, src_timestamp_2, modified_b, 9, 0),
    (1000, src_timestamp_3, modified_a, 1, 0),
    (1000, src_timestamp_4, modified_a, 7, 0)]


def store(cursor, table, source):
    columns = map(Column, head(source))

    insert = partial(Insert(table, columns).execute, cursor)

    for row in tail(source):
        insert(row)


@with_connection
def test_run(conn):
    clear_database(conn)
    plugin = load_plugin()

    minerva_context = MinervaContext(conn, conn)

    instance = plugin(minerva_context)

    job_id = 67
    description = {
        "function_set_id": 43,
        "dest_timestamp": "2012-12-11 13:00:00",
        "processed_max_modified": "2012-12-11 13:03:29"}
    config = {}

    job = instance.create_job(job_id, description, config)

    assert_not_equal(job, None)

    dest_granularity = 3600

    with closing(conn.cursor()) as cursor:
        source_datasource_1 = get_dummy_datasource(cursor, 5, "dummy-src-5")
        dest_datasource = get_dummy_datasource(cursor, 6, "dummy-transform-src")

        dest_entitytype = get_dummy_entitytype(cursor, 45, "dummy_type_aggregate")

        get_function_mapping(cursor, 11, "sum", ["counter_a"], "sum_a")

        get_function_set(cursor, 43, "test_set_agg", [11], [5], 45, 900, 6,
            dest_entitytype.id, dest_granularity, None, ["entity_id"])

        args = 1, "unittest", "transform", ""
        add_job_source(cursor, *args)

        size = 233
        job_source_id = 1
        args = job_id, "transform", "", size, "2012-12-11 14:34:00", None, None, None, job_source_id, "running"
        add_job(cursor, *args)

        args = 43, description["dest_timestamp"], description["processed_max_modified"], "2012-12-11 13:03:00", job_id
        add_state(cursor, *args)

        table_name = "dummy-src-5_dummy_type_aggregate_qtr_20121211"
        columns = [
            Column("entity_id"),
            Column("timestamp", type_=SqlType("timestamp with time zone")),
            Column("modified", type_=SqlType("timestamp with time zone")),
            Column("counter_a"),
            Column("counter_b")]
        table = Table("trend", table_name, columns=columns)

        if table_exists(cursor, table):
            table.drop().execute(cursor)

        table.create().execute(cursor)

        store(cursor, table, source_1)

        result_table = Table("trend", "dummy-transform-src_dummy_type_aggregate_hr_20121207")

        if table_exists(cursor, result_table):
            result_table.truncate().execute(cursor)

    conn.commit()

    print("source_1")
    print("\n".join(render_source(source_1)))

    job.execute()

    columns = map(Column, ["entity_id", "sum_a"])

    query = result_table.select(columns)

    with closing(conn.cursor()) as cursor:
        query.execute(cursor, args)

        print("\n".join(render_result(cursor)))

        query.execute(cursor, args)

        row = cursor.fetchone()

        eq_(row[1], 21)


def add_state(cursor, *args):
    table = Table("transform", "state")
    col_names = ["function_set_id", "dest_timestamp", "processed_max_modified", "max_modified", "job_id"]
    columns = map(Column, col_names)

    select = table.select(1).where_(And(Eq(columns[0]), Eq(columns[1])))

    select.execute(cursor, args[0:2])

    if cursor.rowcount == 0:
        table.insert(columns).execute(cursor, args)


def add_job_source(cursor, *args):
    table = Table("system", "job_source")
    col_names = "id", "name", "job_type", "config"
    columns = map(Column, col_names)

    column_id = columns[0]

    select = Select(1, from_=table, where_=Eq(column_id))

    select.execute(cursor, (args[0], ))

    if cursor.rowcount == 0:
        table.insert(columns).execute(cursor, args)


def add_job(cursor, *args):
    table = Table("system", "job")
    col_names = "id", "type", "description", "size", "created", "started", "finished", "success", "job_source_id", "state"
    columns = map(Column, col_names)

    column_id = columns[0]

    select = Select(1, from_=table, where_=Eq(column_id))

    select.execute(cursor, (args[0], ))

    if cursor.rowcount == 0:
        table.insert(columns).execute(cursor, args)


if __name__ == '__main__':
    test_run()
