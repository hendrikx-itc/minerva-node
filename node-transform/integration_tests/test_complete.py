import pkg_resources
from contextlib import closing
from datetime import datetime
from functools import partial

import pytz

from nose.tools import eq_, raises, assert_not_equal, assert_raises, with_setup

from minerva.util import first, head, tail, k
from minerva.storage import get_plugin
from minerva.db.query import Table, Column, Insert, Truncate, Select, Value, Eq, \
        Argument, And, table_exists, SqlType, Call

from minerva.node import MinervaContext

from minerva_db import connect, clear_database, get_tables, with_connection, \
        get_dummy_datasource, get_dummy_entitytype, TIMEZONE, get_function_set, \
        get_function_mapping, render_result, get_or_create_entity
from trend import render_source, get_entitytype, unlines

ENTRYPOINT = "node.plugins"


def load_plugin():
    """
    Load and return the plugin.
    """
    return first([entrypoint.load() for entrypoint in pkg_resources.iter_entry_points(group=ENTRYPOINT, name="transform")])

tzinfo = pytz.timezone(TIMEZONE)
timestamp = tzinfo.localize(datetime(2012, 12, 11, 14, 0, 0))
modified_a = tzinfo.localize(datetime(2012, 12, 11, 14, 3, 22))
modified_b = tzinfo.localize(datetime(2012, 12, 11, 14, 7, 14))

dns = ["dummy_type_standard=dummy_{}".format(i) for i in range(1000, 1009)]

def create_source_1(entities):
    columns = ("entity_id", "timestamp", "modified", "counter_a", "counter_b")

    values = [
        (timestamp, modified_a, 443, 0),
        (timestamp, modified_a, 216, 8),
        (timestamp, modified_a, 322, 2),
        (timestamp, modified_a, 357, 0),
        (timestamp, modified_a, 108, 1),
        (timestamp, modified_b, 443, 0),
        (timestamp, modified_a, 443, 0),
        (timestamp, modified_a, 216, 8),
        (timestamp, modified_b, 322, 2),
        (timestamp, modified_a, 357, 0)]

    rows = [(entity.id,) + row for entity, row in zip(entities, values)]

    return [columns] + rows

def create_source_2(entities):
    columns = ("entity_id", "timestamp", "modified", "counter_c")

    values = [
        (timestamp, modified_a, 13),
        (timestamp, modified_a, 17),
        (timestamp, modified_a, 6),
        (timestamp, modified_a, 24),
        (timestamp, modified_a, 32),
        (timestamp, modified_a, 13),
        (timestamp, modified_a, 13),
        (timestamp, modified_a, 17),
        (timestamp, modified_a, 6),
        (timestamp, modified_a, 24)]

    rows = [(entity.id,) + row for entity, row in zip(entities, values)]

    return [columns] + rows


def store(cursor, table, source):
    columns = map(Column, head(source))

    insert = partial(Insert(table, columns).execute, cursor)

    for row in tail(source):
        insert(row)


function_set_table = Table("transform", "function_set")
modified_table = Table("trend", "modified")
state_table = Table("transform", "state")


@with_connection
def test_run(conn):
    clear_database(conn)
    plugin = load_plugin()

    minerva_context = MinervaContext(conn, conn)

    instance = plugin(minerva_context)

    job_id = 67
    description = {
        "function_set_id": 42,
        "dest_timestamp": timestamp.isoformat(),
        "processed_max_modified": "2012-12-11 14:03:29+01:00"}
    config = {}

    job = instance.create_job(job_id, description, config)

    assert_not_equal(job, None)

    dest_granularity = 900

    function_mapping_table = Table("transform", "function_mapping")

    with closing(conn.cursor()) as cursor:
        state_table.truncate().execute(cursor)
        function_set_table.truncate(cascade=True).execute(cursor)

        source_datasource_1 = get_dummy_datasource(cursor, "dummy-src-1")
        source_datasource_2 = get_dummy_datasource(cursor, "dummy-src-2")
        dest_datasource = get_dummy_datasource(cursor, "dummy-transform-src")

        dest_entitytype = get_dummy_entitytype(cursor, "dummy_type_standard")

        function_mapping_table.truncate().execute(cursor)

        get_function_mapping(cursor, 1, None, ["counter_a"], "identity_a")
        get_function_mapping(cursor, 2, None, ["counter_b"], "identity_b")
        get_function_mapping(cursor, 3, None, ["counter_c"], "identity_c")
        get_function_mapping(cursor, 4, "add", ["counter_a", "counter_b"], "add_a_b")
        get_function_mapping(cursor, 5, "multiply", ["counter_a", "300"], "a_times_300")

        get_function_set(cursor, 42, "test_set", [1, 2, 3, 4, 5], [3, 4], 42, 900, 6,
            dest_entitytype.id, dest_granularity, None, [], True)

        args = 1, "unittest", "transform", ""
        add_job_source(cursor, *args)

        size = 233
        job_source_id = 1
        args = job_id, "transform", "", size, "2012-12-11 14:34:00", None, None, None, job_source_id, "running"

        add_job(cursor, *args)

        args = 42, description["dest_timestamp"], description["processed_max_modified"], "2012-12-11 13:03:00", job_id
        add_state(cursor, *args)

        table_name = "dummy-src-1_dummy_type_standard_qtr_20121211"
        columns = [
            Column("entity_id"),
            Column("timestamp", type_=SqlType("timestamp with time zone")),
            Column("modified", type_=SqlType("timestamp with time zone")),
            Column("counter_a"),
            Column("counter_b")]
        src_table_1 = Table("trend", table_name, columns=columns)

        if table_exists(cursor, src_table_1):
            src_table_1.drop().execute(cursor)

        src_table_1.create().execute(cursor)

        entities = map(partial(get_or_create_entity, cursor), dns)

        source_1 = create_source_1(entities)

        store(cursor, src_table_1, source_1)

        table_name = "dummy-src-2_dummy_type_standard_qtr_20121211"
        columns = [
            Column("entity_id"),
            Column("timestamp", type_=SqlType("timestamp with time zone")),
            Column("modified", type_=SqlType("timestamp with time zone")),
            Column("counter_c")]
        src_table_2 = Table("trend", table_name, columns=columns)

        if table_exists(cursor, src_table_2):
            src_table_2.drop().execute(cursor)

        src_table_2.create().execute(cursor)

        source_2 = create_source_2(entities)

        store(cursor, src_table_2, source_2)

        result_table = Table("trend", "dummy-transform-src_dummy_type_standard_qtr_20121211")

        if table_exists(cursor, result_table):
            result_table.truncate().execute(cursor)

    conn.commit()

    print("source_1")
    print(unlines(render_source(source_1)))

    print("source_2")
    print(unlines(render_source(source_2)))

    job.execute()

    columns = map(Column, ["entity_id", "identity_a", "identity_b", "add_a_b", "a_times_300"])

    query = result_table.select(columns)

    with closing(conn.cursor()) as cursor:
        query.execute(cursor, args)

        print(unlines(render_result(cursor)))

        query = src_table_1.select(Call("max", Column("modified")))
        query.execute(cursor)
        src1_max_modified = first(cursor.fetchone())

        query = src_table_2.select(Call("max", Column("modified")))
        query.execute(cursor)
        src2_max_modified = first(cursor.fetchone())

        query = modified_table.select(Column("end")).where_(Eq(Column("table_name"), result_table.name))
        query.execute(cursor)

        query = state_table.select(Column("processed_max_modified")).where_(Eq(Column("function_set_id")))
        query.execute(cursor, (42,))
        processed_max_modified = first(cursor.fetchone())

        eq_(max(src1_max_modified, src2_max_modified), processed_max_modified)


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
