# -*- coding: utf-8 -*-
__docformat__ = "restructuredtext en"

__copyright__ = """
Copyright (C) 2013 Hendrikx-ITC B.V.

Distributed under the terms of the GNU General Public License as published by
the Free Software Foundation; either version 3, or (at your option) any later
version.  The full license is in the file COPYING, distributed as part of
this software.
"""
import psycopg2

from minerva.db.error import translate_postgresql_exception
from minerva.storage.trend.trendstore import TrendStore


class Materialization(object):
    def __init__(self, id):
        self.id = id

    def chunk(self, timestamp):
        return MaterializationChunk(self, timestamp)


class MaterializationChunk(object):
    def __init__(self, type, timestamp):
        self.type = type
        self.timestamp = timestamp

    def execute(self, cursor):
        args = (self.type.id, self.timestamp)

        try:
            cursor.callproc("materialization.materialize", args)
        except psycopg2.ProgrammingError as exc:
            raise translate_postgresql_exception(exc)

        return cursor.fetchone()


def update_processed_max_modified(cursor, type_id, timestamp,
                                  processed_max_modified):
    query = (
        "UPDATE materialization.state SET processed_max_modified = %s "
        "WHERE type_id = %s AND timestamp = %s")

    args = processed_max_modified, type_id, timestamp

    cursor.execute(query, args)

    if cursor.rowcount == 0:
        query = (
            "INSERT INTO materialization.state(type_id, timestamp, "
            "processed_max_modified, max_modified) "
            "VALUES(%s, %s, %s, %s)")
        args = (type_id, timestamp, processed_max_modified,
                processed_max_modified)

        cursor.execute(query, args)

