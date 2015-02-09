# -*- coding: utf-8 -*-
__docformat__ = "restructuredtext en"

__copyright__ = """
Copyright (C) 2014 Hendrikx-ITC B.V.

Distributed under the terms of the GNU General Public License as published by
the Free Software Foundation; either version 3, or (at your option) any later
version.  The full license is in the file COPYING, distributed as part of
this software.
"""
from contextlib import closing

from minerva.directory.basetypes import DataSource
from minerva.storage.attribute.rawdatapackage import RawDataPackage
from minerva.storage.attribute.attributestore import AttributeStore
from minerva.storage.notification.entityref import EntityDnRef

from minerva_csvimporter.storage import Storage


class AttributeStorage(Storage):
    def __init__(self, datasource):
        self.datasource = datasource
        self.conn = None

    def connect(self, conn):
        self.conn = conn

    def __str__(self):
        return "attribute()"

    def store(self, column_names, fields, raw_data_rows):
        rows = list(raw_data_rows)
        raw_datapackage = RawDataPackage(column_names, rows)
        attributes = raw_datapackage.deduce_attributes()

        entity_ref = EntityDnRef(rows[0][0])

        with closing(self.conn.cursor()) as cursor:
            datasource = DataSource.from_name(cursor, self.datasource)

            entitytype = entity_ref.get_entitytype(cursor)

            attributestore = AttributeStore.from_attributes(
                cursor, datasource, entitytype, attributes
            )

        self.conn.commit()

        attributestore.store_raw(raw_datapackage).run(self.conn)
