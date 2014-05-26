# -*- coding: utf-8 -*-
__docformat__ = "restructuredtext en"

__copyright__ = """
Copyright (C) 2014 Hendrikx-ITC B.V.

Distributed under the terms of the GNU General Public License as published by
the Free Software Foundation; either version 3, or (at your option) any later
version.  The full license is in the file COPYING, distributed as part of
this software.
"""
from minerva.storage.trend.granularity import create_granularity
from minerva.storage.trend.trendstore import TrendStore
from minerva.storage.trend.rawdatapackage import RawDataPackage

from minerva_csvimporter.storage import Storage
from minerva_csvimporter.util import offset_timestamp


class TrendStorage(Storage):
    def __init__(self, datasource, granularity, timestamp_is_start):
        self.datasource = datasource
        self.granularity = create_granularity(granularity)
        self.timestamp_is_start = timestamp_is_start
        self.conn = None

        if self.timestamp_is_start:
            self.offset = partial(offset_timestamp, timedelta(0, self.granularity))
        else:
            self.offset = identity

    def connect(self, conn):
        self.conn = conn

    def __str__(self):
        return "trend(granularity={})".format(self.granularity)

    def store(self, column_names, fields, raw_data_rows):
        get_timestamp = operator.itemgetter(1)

        for timestamp, grouped_rows in grouped_by(raw_data_rows, get_timestamp):
            rows = [
                (dn, values)
                for dn, _, values in grouped_rows
            ]

            entity_ref = EntityDnRef(rows[0][0])

            with closing(self.conn.cursor()) as cursor:
                datasource = DataSource.from_name(cursor, self.datasource)

                entitytype = entity_ref.get_entitytype(cursor)

                trendstore = TrendStore.get(
                    cursor, datasource, entitytype, self.granularity
                )

                if not trendstore:
                    partition_size = 86400

                    trendstore = TrendStore(datasource, entitytype,
                            self.granularity, partition_size, "table").create(cursor)

                self.conn.commit()

            utc_timestamp = timestamp.astimezone(pytz.utc)
            utc_timestamp_str = self.offset(utc_timestamp).strftime("%Y-%m-%dT%H:%M:%S")

            raw_datapackage = RawDataPackage(
                self.granularity, utc_timestamp_str, column_names, rows)

            trendstore.store_raw(raw_datapackage).run(self.conn)
