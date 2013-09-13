import os
import time
import logging
from contextlib import closing
import StringIO

from nose.tools import eq_
from minerva.csvimporter import import_csv

from minerva_db import connect


def test_timestamp_source_data():
    conn = connect()

    try:
        # make sure entitytype is known
        with closing(conn.cursor()) as cursor:
            cursor.execute("INSERT INTO directory.entitytype (name, description) VALUES ('integration_test_entity', 'test')")

        readfile = StringIO.StringIO('CIC;CCR;Drops;t_source\n10023;0.9919;17;20111111_0000\n10047;0.9963;18;20101010_0000\n')

        profile = {
            "granularity": 86400,
            "id_column": "CIC",
            "timestamp_format": "%Y%m%d_%H%M",
            "timestamp_column": "t_source",
            "entity_type": "Cell",
            "identifier_regex": "(.*)",
            "identifier_is_alias": False,
            "fields": [],
            "ignore_fields": [],
            "ignore_field_mismatches": False,
            "timestamp_is_start": True,
            "character_encoding": "utf-8",
            "dialect": "auto",
            "value_mapping": ""
        }

        datasource_name = 'csvimporter'
        timestamp = None
        import_csv(conn, profile, datasource_name, timestamp, readfile)
        #import_csv(conn, 'integration_test_entity', 86400, 'CIC', 'csvimporter', 'trend', 't_source', readfile, "fakefile")
        readfile.close()

    finally:
        with closing(conn.cursor()) as cursor:
            cursor.execute("DELETE FROM directory.entitytype WHERE name = 'integration_test_entity'")
        conn.commit()
        conn.close()
