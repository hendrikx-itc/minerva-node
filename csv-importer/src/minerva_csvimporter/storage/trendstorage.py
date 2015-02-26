# -*- coding: utf-8 -*-
__docformat__ = "restructuredtext en"

__copyright__ = """
Copyright (C) 2014 Hendrikx-ITC B.V.

Distributed under the terms of the GNU General Public License as published by
the Free Software Foundation; either version 3, or (at your option) any later
version.  The full license is in the file COPYING, distributed as part of
this software.
"""
import logging
from datetime import timedelta
import operator
from functools import partial
from contextlib import closing

from minerva.util import identity, grouped_by
from minerva.directory import DataSource
from minerva.storage.trend.granularity import create_granularity
from minerva.storage.trend.tabletrendstore import TableTrendStore, \
    TableTrendStoreDescriptor
from minerva.storage.trend.trend import TrendDescriptor
from minerva.storage.trend.datapackage import DefaultPackage
from minerva.storage.datatype import deduce_data_types

from minerva_csvimporter.error import ConfigurationError
from minerva_csvimporter.storage import Storage
from minerva_csvimporter.util import offset_timestamp
from minerva.directory.entityref import EntityDnRef


class TrendStorage(Storage):
    def __init__(
            self, data_source, granularity, timestamp_is_start,
            auto_create_trend_store, auto_create_trends):
        self.data_source_name = data_source
        self.granularity = create_granularity(granularity)
        self.timestamp_is_start = timestamp_is_start
        self.conn = None

        if self.timestamp_is_start:
            self.offset = partial(
                offset_timestamp, timedelta(0, self.granularity)
            )
        else:
            self.offset = identity

        self.auto_create_trend_store = auto_create_trend_store
        self.auto_create_trends = auto_create_trends

    def __str__(self):
        return "trend(granularity={})".format(self.granularity)

    def get_trend_store(self, conn, entity_ref):
        with closing(conn.cursor()) as cursor:
            data_source = DataSource.get_by_name(self.data_source_name)(cursor)

            if data_source is None:
                raise ConfigurationError(
                    "No such data source: {}".format(self.data_source_name)
                )

            entity_type = entity_ref.get_entity_type(cursor)

            if entity_type is None:
                raise ConfigurationError(
                    "No entity type found for: {}".format(entity_ref)
                )

            trend_store = TableTrendStore.get(
                data_source, entity_type, self.granularity
            )(cursor)

            if not trend_store:
                if self.auto_create_trend_store:
                    partition_size = 86400 * 7

                    trend_store = TableTrendStore.create(
                        TableTrendStoreDescriptor(
                            data_source, entity_type, self.granularity,
                            [], partition_size
                        )
                    )(cursor)
                else:
                    raise ConfigurationError(
                        "No trend store exists for data source '{}',"
                        "entity type '{}', granularity '{}'".format(
                            data_source.name,
                            entity_type.name,
                            self.granularity
                        )
                    )

        conn.commit()

        return trend_store

    def rows_to_packages(self, column_names, rows):
        get_timestamp = operator.itemgetter(1)

        for timestamp, grouped_rows in grouped_by(rows, get_timestamp):
            yield DefaultPackage(
                self.granularity,
                self.offset(timestamp),
                column_names,
                [
                    (dn, values)
                    for dn, _, values in grouped_rows
                ]
            )

    def store(self, column_names, fields, raw_data_rows):
        def f(conn):
            packages = self.rows_to_packages(column_names, raw_data_rows)

            for data_package in packages:
                trend_store = self.get_trend_store(
                    conn, EntityDnRef(data_package.rows[0][0])
                )

                missing_trends = [
                    name
                    for name in data_package.trend_names
                    if name not in set(t.name for t in trend_store.trends)
                ]

                if len(missing_trends) > 0:
                    if self.auto_create_trends:
                        logging.info(
                            'creating missing trends {} in '
                            'trend store {}'.format(
                                missing_trends, trend_store
                            )
                        )

                        data_types = deduce_data_types(
                            values
                            for dn, values in
                            data_package.rows
                        )

                        trend_descriptors = [
                            TrendDescriptor(name, data_type, '')
                            for name, data_type
                            in zip(data_package.trend_names, data_types)
                        ]

                        with closing(conn.cursor()) as cursor:
                            trend_store = trend_store.check_trends_exist(
                                trend_descriptors
                            )(cursor)

                        conn.commit()
                    else:
                        raise Exception(
                            'trends {} not defined in trend store {}'.format(
                                missing_trends, trend_store
                            )
                        )

                trend_store.store(data_package).run(conn)

        return f
