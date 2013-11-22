# -*- coding: utf-8 -*-
"""Integration test for function `import_csv`."""
__docformat__ = "restructuredtext en"

__copyright__ = """
Copyright (C) 2008-2013 Hendrikx-ITC B.V.

Distributed under the terms of the GNU General Public License as published by
the Free Software Foundation; either version 3, or (at your option) any later
version.  The full license is in the file COPYING, distributed as part of
this software.
"""
import StringIO

from minerva_csvimporter import import_csv

from minerva_db import connect


def test_timestamp_source_data():
    conn = connect()

    readfile = StringIO.StringIO(
        'CIC;CCR;Drops;t_source\n'
        '10023;0.9919;17;20111111_0000\n'
        '10047;0.9963;18;20101010_0000\n')

    profile = {
        "granularity": 86400,
        "identifier": "Cell={CIC}",
        "timestamp_format": "%Y%m%d_%H%M",
        "timestamp_column": "t_source",
        "timestamp_from_filename_regex": None,
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

    import_csv(conn, profile, datasource_name, 'trend', timestamp, readfile,
               'test.csv')
