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

from minerva.util import head, tail, compose, unlines, k
from minerva.db.query import Column, Insert
from minerva.db.dbtransaction import DbTransaction, UpdateState

from minerva.node import MinervaContext
from minerva.transform.types import Transformation
from minerva_db import reset_db, with_connection, \
        get_dummy_datasource, get_dummy_entitytype, TIMEZONE, add_function_set, \
        add_function_mapping, render_result

from util import render_datapackage

from minerva_storage_trend.store import CopyFrom
from minerva_storage_trend.types_v4 import DataPackage, TrendStore3
from minerva_storage_trend.granularity import create_granularity


tzinfo = pytz.timezone(TIMEZONE)

local_timestamp = compose(tzinfo.localize, datetime)

src_timestamp_1 = local_timestamp(2012, 12, 11, 13, 15, 0)
src_timestamp_2 = local_timestamp(2012, 12, 11, 13, 30, 0)
src_timestamp_3 = local_timestamp(2012, 12, 11, 13, 45, 0)
src_timestamp_4 = local_timestamp(2012, 12, 11, 14, 0, 0)

modified_a = local_timestamp(2012, 12, 11, 14, 3, 27)
modified_b = local_timestamp(2012, 12, 11, 14, 7, 14)

dest_timestamp = local_timestamp(2012, 12, 11, 14, 0, 0)

granularity = create_granularity("900")

trend_names = ("counter_a", "counter_b")
source_1_1 = DataPackage(granularity, src_timestamp_1, trend_names, [(1000, (4, 0))])
source_1_2 = DataPackage(granularity, src_timestamp_2, trend_names, [(1000, (9, 0))])
source_1_3 = DataPackage(granularity, src_timestamp_3, trend_names, [(1000, (1, 0))])
source_1_4 = DataPackage(granularity, src_timestamp_4, trend_names, [(1000, (7, 0))])


def store(cursor, table, source):
    columns = map(Column, head(source))

    insert = partial(Insert(table, columns).execute, cursor)

    for row in tail(source):
        insert(row)


def store_modified_at(trendstore, datapackage, modified):
    def set_modified(state):
        state["modified"] = modified

    partition = trendstore.partition(datapackage.timestamp)
    set_modified_action = UpdateState(set_modified)
    copy_from = CopyFrom(k(partition), k(datapackage))

    return DbTransaction(set_modified_action, copy_from)


@with_connection
def test_run_hr(conn):
    with closing(conn.cursor()) as cursor:
        reset_db(cursor)

    conn.commit()

    source_granularity = create_granularity("900")
    dest_granularity = create_granularity("3600")

    with closing(conn.cursor()) as cursor:
        source_datasource = get_dummy_datasource(cursor, "dummy-src-5")
        dest_datasource = get_dummy_datasource(cursor, "dummy-transform-src")

        dest_entitytype = get_dummy_entitytype(cursor, "dummy_type_aggregate")

        function_mapping = add_function_mapping(cursor, "sum", ["counter_a"], "sum_a")

        partition_size = 86400

        trendstore_1 = TrendStore3(source_datasource, dest_entitytype,
                source_granularity, partition_size, "table")
        trendstore_1.create(cursor)

        dest_trendstore = TrendStore3(dest_datasource, dest_entitytype,
                dest_granularity, partition_size, "table")
        dest_trendstore.create(cursor)

        function_set_qtr = add_function_set(cursor, "test_set_agg", "",
                [function_mapping.id], [source_datasource.id], dest_entitytype.id,
                source_granularity.name, dest_datasource.id, dest_entitytype.id,
                dest_granularity.name, None, ["entity_id"], None, True)

        conn.commit()

        transaction = store_modified_at(trendstore_1, source_1_1, modified_a)
        transaction.run(conn)
        transaction = store_modified_at(trendstore_1, source_1_2, modified_b)
        transaction.run(conn)
        transaction = store_modified_at(trendstore_1, source_1_3, modified_a)
        transaction.run(conn)
        transaction = store_modified_at(trendstore_1, source_1_4, modified_a)
        transaction.run(conn)

    logging.debug("source_1")
    logging.debug(unlines(render_datapackage(source_1_1)))

    processed_max_modified = None

    minerva_context = MinervaContext(conn, conn)

    transformation = Transformation(function_set_qtr, dest_timestamp)

    transformation.execute(minerva_context)

    columns = map(Column, ["entity_id", "sum_a"])

    dest_partition = dest_trendstore.partition(dest_timestamp)
    result_table = dest_partition.table()

    query = result_table.select(columns)

    with closing(conn.cursor()) as cursor:
        query.execute(cursor)

        logging.debug(unlines(render_result(cursor)))

        query.execute(cursor)

        row = cursor.fetchone()

        eq_(row[1], 21)


@with_connection
def test_run_month(conn):
    with closing(conn.cursor()) as cursor:
        reset_db(cursor)

    conn.commit()

    minerva_context = MinervaContext(conn, conn)

    source_granularity = create_granularity("900")
    dest_granularity = create_granularity("month")
    dest_timestamp = local_timestamp(2013, 1, 1)

    with closing(conn.cursor()) as cursor:
        source_datasource_1 = get_dummy_datasource(cursor, "dummy-src-5")
        dest_datasource = get_dummy_datasource(cursor, "dummy-transform-src")

        dest_entitytype = get_dummy_entitytype(cursor, "dummy_type_aggregate")

        function_mapping = add_function_mapping(cursor, "sum", ["counter_a"], "sum_a")

        partition_size = 86400

        trendstore_1 = TrendStore3(source_datasource_1, dest_entitytype,
                source_granularity, partition_size, "table")
        trendstore_1.create(cursor)
        dest_trendstore = TrendStore3(dest_datasource, dest_entitytype,
                dest_granularity, partition_size, "table")
        dest_trendstore.create(cursor)

        function_set = add_function_set(cursor, "test_set_agg_month", "",
                [function_mapping.id], [source_datasource_1.id], dest_entitytype.id,
                source_granularity.name, dest_datasource.id, dest_entitytype.id,
                dest_granularity.name, None, ["entity_id"], None, True)

        conn.commit()

        transaction = store_modified_at(trendstore_1, source_1_1, modified_a)
        transaction.run(conn)
        transaction = store_modified_at(trendstore_1, source_1_2, modified_b)
        transaction.run(conn)
        transaction = store_modified_at(trendstore_1, source_1_3, modified_a)
        transaction.run(conn)
        transaction = store_modified_at(trendstore_1, source_1_4, modified_a)
        transaction.run(conn)

        dest_partition = dest_trendstore.partition(dest_timestamp)

    conn.commit()

    logging.debug("source_1")
    logging.debug(unlines(render_datapackage(source_1_1)))
    logging.debug(unlines(render_datapackage(source_1_2)))
    logging.debug(unlines(render_datapackage(source_1_3)))
    logging.debug(unlines(render_datapackage(source_1_4)))

    transformation = Transformation(function_set, dest_timestamp)

    transformation.execute(minerva_context)

    columns = map(Column, ["entity_id", "sum_a"])

    result_table = dest_partition.table()

    query = result_table.select(columns)

    with closing(conn.cursor()) as cursor:
        query.execute(cursor)

        logging.debug(unlines(render_result(cursor)))

        query.execute(cursor)

        row = cursor.fetchone()

        eq_(row[1], 21)
