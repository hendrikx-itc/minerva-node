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
from functools import partial
from datetime import timedelta
from contextlib import closing

import pytz

from minerva.directory.basetypes import DataSource
from minerva.storage.notification.entityref import EntityDnRef
from minerva.storage.notification.types import NotificationStore, Record, Attribute
from minerva.util import grouped_by, identity

from minerva_csvimporter.datatype import deduce_data_types, parse_values, type_map as datatype_map


from minerva_csvimporter.storage.storage import Storage


class NotificationStorage(Storage):
    def __init__(self, datasource):
        self.datasource = datasource
        self.conn = None

    def connect(self, conn):
        self.conn = conn

    def __str__(self):
        return "notification()"

    def store(self, column_names, fields, raw_data_rows):
        with closing(self.conn.cursor()) as cursor:
            datasource = DataSource.from_name(cursor, self.datasource)

            notificationstore = NotificationStore.load(cursor, datasource)

            rows = list(raw_data_rows)

            if notificationstore:
                datatype_dict = {
                    attribute.name: attribute.data_type
                    for attribute in notificationstore.attributes
                }

                def merge_datatypes():
                    for name in column_names:
                        configured_type = fields.get(name)

                        notificationstore_type = datatype_dict[name]

                        if configured_type:
                            if configured_type.name != notificationstore_type:
                                raise Exception("Attribute type of notificationstore does not match configured type {} <> {}".format(notificationstore_type, configured_type.name))

                            yield configured_type
                        else:
                            yield datatype_map[notificationstore_type]()

                datatypes = list(merge_datatypes())
            else:
                deduced_datatype_names = deduce_data_types(
                    map(operator.itemgetter(2), rows)
                )

                def merge_datatypes():
                    for column_name, datatype_name in zip(column_names, deduced_datatype_names):
                        configured_type = fields.get(column_name)

                        if configured_type:
                            yield configured_type
                        else:
                            yield datatype_map[datatype_name]()

                datatypes = list(merge_datatypes())

                attributes = [
                    Attribute(name, datatype.name, '')
                    for name, datatype in zip(column_names, datatypes)
                ]

                notificationstore = NotificationStore(
                    datasource, attributes
                ).create(cursor)

                self.conn.commit()

            for dn, timestamp, values in rows:
                record = Record(
                    EntityDnRef(dn), timestamp, column_names,
                    parse_values(datatypes, values)
                )

                notificationstore.store_record(record)(cursor)

        self.conn.commit()