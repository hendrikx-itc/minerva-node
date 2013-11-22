import os
import time
import logging
from contextlib import closing
from pytz import timezone

from nose.tools import eq_, raises, assert_not_equal, assert_raises, with_setup

from minerva_db import connect, clear_database, get_tables, with_connection, \
        add_function_set, get_dummy_entitytype, get_dummy_datasource

from minerva_node_transform.helpers import get_function_set, \
        NoSuchFunctionSetError


@raises(NoSuchFunctionSetError)
@with_connection
def test_get_function_set_1(conn):
    clear_database(conn)

    with closing(conn.cursor()) as cursor:
        get_function_set(cursor, 12)


@with_connection
def test_get_function_set_2(conn):
    clear_database(conn)

    with closing(conn.cursor()) as cursor:
        entitytype = get_dummy_entitytype(cursor, 42, "dummy_type")
        src_datasource = get_dummy_datasource(cursor, 3, "dummy-src-1")
        dst_datasource = get_dummy_datasource(cursor, 4, "dummy-src-2")

        args = 14, "test_set", [1, 2, 3], [3], 42, 900, 4, entitytype.id, 900, None, []

        add_function_set(cursor, *args)

        function_set = get_function_set(cursor, 14)

    eq_(function_set.name, 'test_set')
