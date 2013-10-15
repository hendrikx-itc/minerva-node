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

from nose.tools import eq_, raises

from minerva_db import reset_db, with_connection, \
        add_function_set, get_dummy_entitytype, get_dummy_datasource

from minerva_transform.helpers import get_function_set, NoSuchFunctionSetError


@raises(NoSuchFunctionSetError)
@with_connection
def test_get_function_set_1(conn):
    with closing(conn.cursor()) as cursor:
        reset_db(cursor)

    conn.commit()

    with closing(conn.cursor()) as cursor:
        get_function_set(cursor, 12)


@with_connection
def test_get_function_set_2(conn):
    with closing(conn.cursor()) as cursor:
        reset_db(cursor)

    conn.commit()

    with closing(conn.cursor()) as cursor:
        entitytype = get_dummy_entitytype(cursor, "dummy_type")
        src_datasource = get_dummy_datasource(cursor, "dummy-src-1")
        dst_datasource = get_dummy_datasource(cursor, "dummy-src-2")

        args = ("test_set", "", [1, 2, 3], [src_datasource.id], entitytype.id,
                "900", dst_datasource.id, entitytype.id, "900", None, [], None, True)

        function_set = add_function_set(cursor, *args)

        function_set = get_function_set(cursor, function_set.id)

    eq_(function_set.name, 'test_set')
