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

from minerva_storage_trend.trendstore import TrendStore


class Materialization(object):
    def __init__(self, src_trendstore, dst_trendstore, enabled=False):
        self.id = None
        self.src_trendstore = src_trendstore
        self.dst_trendstore = dst_trendstore

    def chunk(self, timestamp):
        return MaterializationChunk(self, timestamp)

    def __str__(self):
        return "{}->{}".format(self.src_trendstore, self.dst_trendstore)

    @staticmethod
    def define_from_view(view):
        def f(cursor):
            query = (
                "SELECT (m.new).* "
                "FROM (SELECT materialization.define(view) AS new "
                "FROM trend.view "
                "WHERE id = %s) as m")

            args = view.id,

            cursor.execute(query, args)

            return Materialization.from_row(*cursor.fetchone())(cursor)

        return f

    @staticmethod
    def load_by_id(id):
        args = id,
        query = (
            "SELECT id, src_trendstore_id, dst_trendstore_id, enabled "
            "FROM materialization.type "
            "WHERE id = %s")

        def f(cursor):
            cursor.execute(query, args)

            return Materialization.from_row(*cursor.fetchone())(cursor)

        return f

    @staticmethod
    def from_row(id, src_trendstore_id, dst_trendstore_id, enabled):
        def f(cursor):
            src_trendstore = TrendStore.get_by_id(cursor, src_trendstore_id)
            dst_trendstore = TrendStore.get_by_id(cursor, dst_trendstore_id)

            materialization = Materialization(src_trendstore, dst_trendstore,
                                              enabled)
            materialization.id = id
            return materialization

        return f


class MaterializationChunk(object):
    def __init__(self, type, timestamp):
        self.type = type
        self.timestamp = timestamp

    def execute(self, cursor):
        args = (self.type.src_trendstore.id,
                self.type.dst_trendstore.id, self.timestamp)

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
