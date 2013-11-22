# -*- coding: utf-8 -*-
__docformat__ = "restructuredtext en"

__copyright__ = """
Copyright (C) 2012-2013 Hendrikx-ITC B.V.

Distributed under the terms of the GNU General Public License as published by
the Free Software Foundation; either version 3, or (at your option) any later
version.  The full license is in the file COPYING, distributed as part of
this software.
"""
from contextlib import closing
from datetime import datetime
from functools import partial
import logging

import pytz

from nose.tools import eq_

from minerva.util import first, unlines, k
from minerva.db.query import Table, Column, \
    Eq, Call
from minerva.db.dbtransaction import DbTransaction, UpdateState
from minerva.node import MinervaContext
from minerva.storage.trend.granularity import create_granularity
from minerva.storage.trend.datapackage import DataPackage
from minerva.storage.trend.trendstore import TrendStore, CopyFrom

from minerva_db import reset_db, with_connection, \
    get_dummy_datasource, get_dummy_entitytype, TIMEZONE, add_function_set, \
    add_function_mapping, render_result, get_or_create_entity

from minerva_transform.types import Transformation
from util import render_datapackage

ENTRYPOINT = "node.plugins"


tzinfo = pytz.timezone(TIMEZONE)
timestamp = tzinfo.localize(datetime(2012, 12, 11, 14, 0, 0))
modified_a = tzinfo.localize(datetime(2012, 12, 11, 14, 3, 22))
modified_b = tzinfo.localize(datetime(2012, 12, 11, 14, 7, 14))

dummy_type_name = "dummy_type_standard"

dns = ["{}=dummy_{}".format(dummy_type_name, i) for i in range(1000, 1009)]


def create_source_1(granularity, entities):
    trend_names = "counter_a", "counter_b"

    values = [
        (443, 0),
        (216, 8),
        (322, 2),
        (357, 0),
        (108, 1),
        (443, 0),
        (443, 0),
        (216, 8),
        (322, 2),
        (357, 0)]

    rows = [(entity.id, row) for entity, row in zip(entities, values)]

    return DataPackage(granularity, timestamp, trend_names, rows)


def create_source_2(granularity, entities):
    trend_names = ("counter_c", )

    values = [
        (13,),
        (17,),
        (6,),
        (24,),
        (32,),
        (13,),
        (13,),
        (17,),
        (6,),
        (24,)]

    rows = [(entity.id, row) for entity, row in zip(entities, values)]

    return DataPackage(granularity, timestamp, trend_names, rows)


function_set_table = Table("transform", "function_set")
modified_table = Table("trend", "modified")
state_table = Table("transform", "state")
function_mapping_table = Table("transform", "function_mapping")


@with_connection
def test_run(conn):
    with closing(conn.cursor()) as cursor:
        reset_db(cursor)

    conn.commit()

    minerva_context = MinervaContext(conn, conn)

    source_granularity = create_granularity("900")
    dest_granularity = create_granularity("900")

    with closing(conn.cursor()) as cursor:
        source_datasource_1 = get_dummy_datasource(cursor, "dummy-src-1")
        source_datasource_2 = get_dummy_datasource(cursor, "dummy-src-2")
        dest_datasource = get_dummy_datasource(cursor, "dummy-transform-src")

        entitytype = get_dummy_entitytype(cursor, dummy_type_name)

        partition_size = 86400

        trendstore_1 = TrendStore(
            source_datasource_1, entitytype, source_granularity,
            partition_size, "table")
        trendstore_1.create(cursor)
        trendstore_2 = TrendStore(
            source_datasource_2, entitytype, source_granularity,
            partition_size, "table")
        trendstore_2.create(cursor)
        result_trendstore = TrendStore(
            dest_datasource, entitytype, dest_granularity, partition_size,
            "table")
        result_trendstore.create(cursor)

        function_mappings = [
            add_function_mapping(cursor, None, ["counter_a"], "identity_a"),
            add_function_mapping(cursor, None, ["counter_b"], "identity_b"),
            add_function_mapping(cursor, None, ["counter_c"], "identity_c"),
            add_function_mapping(cursor, "add", ["counter_a", "counter_b"], "add_a_b"),
            add_function_mapping(cursor, "multiply", ["counter_a", "300"], "a_times_300")]

        function_mapping_ids = [fm.id for fm in function_mappings]

        function_set_qtr = add_function_set(cursor, "test_set", "", function_mapping_ids,
                [source_datasource_1.id, source_datasource_2.id], entitytype.id, source_granularity.name, dest_datasource.id, entitytype.id,
                dest_granularity.name, None, [], None, True)

        entities = map(partial(get_or_create_entity, cursor), dns)

        conn.commit()

        source_1 = create_source_1(source_granularity, entities)

        def store_modified_at(trendstore, datapackage, modified):
            def set_modified(state):
                state["modified"] = modified

            partition = trendstore.partition(datapackage.timestamp)
            set_modified_action = UpdateState(set_modified)
            copy_from = CopyFrom(k(partition), k(datapackage))

            return DbTransaction(set_modified_action, copy_from)

        transaction = store_modified_at(trendstore_1, source_1, modified_a)
        transaction.run(conn)

        source_2 = create_source_2(source_granularity, entities)

        transaction = store_modified_at(trendstore_2, source_2, modified_a)
        transaction.run(conn)

        result_partition = result_trendstore.partition(timestamp)

        result_table = result_partition.table()

    conn.commit()

    logging.debug("source_1")
    logging.debug(unlines(render_datapackage(source_1)))

    logging.debug("source_2")
    logging.debug(unlines(render_datapackage(source_2)))

    dest_timestamp = timestamp

    transformation = Transformation(function_set_qtr, dest_timestamp)

    transformation.execute(minerva_context)

    columns = map(Column, ["entity_id", "identity_a", "identity_b", "add_a_b", "a_times_300"])

    query = result_table.select(columns)

    with closing(conn.cursor()) as cursor:
        query.execute(cursor)

        logging.debug(unlines(render_result(cursor)))

        src_table_1 = trendstore_1.partition(timestamp).table()
        query = src_table_1.select(Call("max", Column("modified")))
        query.execute(cursor)
        src1_max_modified = first(cursor.fetchone())

        src_table_2 = trendstore_2.partition(timestamp).table()
        query = src_table_2.select(Call("max", Column("modified")))
        query.execute(cursor)
        src2_max_modified = first(cursor.fetchone())

        query = modified_table.select(Column("end")).where_(Eq(Column("table_name"), result_table.name))
        query.execute(cursor)

        query = state_table.select(Column("processed_max_modified")).where_(Eq(Column("function_set_id")))
        query.execute(cursor, (function_set_qtr.id,))
        processed_max_modified = first(cursor.fetchone())

        eq_(max(src1_max_modified, src2_max_modified), processed_max_modified)
