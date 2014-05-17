# -*- coding: utf-8 -*-
__docformat__ = "restructuredtext en"

__copyright__ = """
Copyright (C) 2014 Hendrikx-ITC B.V.

Distributed under the terms of the GNU General Public License as published by
the Free Software Foundation; either version 3, or (at your option) any later
version.  The full license is in the file COPYING, distributed as part of
this software.
"""
import operator
from datetime import timedelta

import pytz

from minerva.storage import get_plugin
from minerva.storage.trend.granularity import create_granularity
from minerva.util import compose, grouped_by


class Storage(object):
    def __init__(self):
        pass

    def connect(self, conn):
        raise NotImplementedError()

    def store_raw(self, datasource):
        raise NotImplementedError()


class TrendStorage(object):
    def __init__(self, granularity, timestamp_is_start):
        self.granularity = create_granularity(granularity)
        self.plugin = None
        self.timestamp_is_start = timestamp_is_start

    def connect(self, conn):
        self.plugin = init_plugin(conn, "trend")

    def __str__(self):
        return "trend(granularity={})".format(self.granularity)

    def store_raw(self, datasource, column_names, raw_data_rows):
        if self.timestamp_is_start:
            offset = timedelta(0, self.granularity)

            parse_ts = compose(partial(offset_timestamp, offset), parse_ts)

        for timestamp, grouped_rows in grouped_by(raw_data_rows, operator.itemgetter(1)):
            rows = [
                (dn, values)
                for dn, _, values in grouped_rows
            ]

            utc_timestamp = timestamp.astimezone(pytz.utc)
            utc_timestamp_str = utc_timestamp.strftime("%Y-%m-%dT%H:%M:%S")

            raw_datapackage = self.plugin.RawDataPackage(
                self.granularity, utc_timestamp_str, column_names, rows)

            self.plugin.store_raw(datasource, raw_datapackage)


class AttributeStorage(object):
    def __init__(self):
        self.plugin = None

    def connect(self, conn):
        self.plugin = init_plugin(conn, "trend")

    def __str__(self):
        return "attribute()"

    def store_raw(self, datasource, column_names, raw_data_rows):
        raw_datapackage = self.plugin.RawDataPackage(column_names, raw_data_rows)

        self.plugin.store_raw(datasource, raw_datapackage)


def init_plugin(conn, storagetype):
    plugin = get_plugin(storagetype)(conn, api_version=4)

    if not plugin:
        raise NoSuitablePluginError(
            "Missing storage plugin {}".format(storagetype))

    return plugin


type_map = {
    "trend": TrendStorage,
    "attribute": AttributeStorage
}
