import os
import logging
from functools import wraps
from contextlib import closing

import psycopg2.extras

from minerva.util.debug import log_call_basic
from minerva.db import parse_db_url, extract_safe_url


def clear_database(conn):
    with closing(conn.cursor()) as cursor:
        cursor.execute("DELETE FROM trend.trend CASCADE")
        cursor.execute("DELETE FROM directory.datasource CASCADE")
        cursor.execute("DELETE FROM directory.entitytype CASCADE")


def with_data(test_set):
    def __init__(i):
        i.data = None
        i.conn = None

    def setup(i):
        i.data = test_set()
        i.conn = connect()

        with closing(i.conn.cursor()) as cursor:
            clear_database(cursor)

            i.data.load(cursor)

        i.conn.commit()


    def teardown(i):
        i.conn.close()

    return type('C', (object,), {
        "__init__": __init__,
        "setup": setup,
        "teardown": teardown})
