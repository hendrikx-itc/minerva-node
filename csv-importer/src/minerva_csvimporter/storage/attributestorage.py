# -*- coding: utf-8 -*-
from contextlib import closing

from minerva.directory import DataSource
from minerva.storage.attribute.datapackage import DataPackage
from minerva.storage.attribute.attributestore import AttributeStore
from minerva.directory.entityref import EntityDnRef

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
        raw_datapackage = DataPackage(column_names, rows)
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
