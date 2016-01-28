# -*- coding: utf-8 -*-
"""Integration test for function `import_csv`."""
from io import StringIO
from contextlib import closing

from minerva_csvimporter import import_csv
from minerva_csvimporter.profile import Profile

from minerva_db import connect


def test_timestamp_source_data():
    with closing(connect()) as conn:
        readfile = StringIO(
            u'CIC;CCR;Drops;t_source\n'
            '10023;0.9919;17;20111111_0000\n'
            '10047;0.9963;18;20101010_0000\n')

        profile = Profile({
            "storage": {
                "type": "trend",
                "config": {
                    "granularity": 86400,
                    "datasource": "integration_test",
                    "timestamp_is_start": False
                }
            },
            "identifier": {
                "template": "Cell={CIC}",
                "regex": "(.*)"
            },
            "timestamp": {
                "type": "from_column",
                "config": {
                    "format": "%Y%m%d_%H%M",
                    "name": "t_source",
                    "timezone": "UTC"
                }
            },
            "identifier_is_alias": False,
            "field_selector": {
                "type": "all"
            },
            "fields": {},
            "timestamp_is_start": True,
            "character_encoding": "utf-8",
            "dialect": {
                "type": "auto"
            },
            "value_mapping": {}
        })

        import_csv(conn, profile, readfile)


def test_timestamp_as_data():
    with closing(connect()) as conn:
        readfile = StringIO(
            u'ts;CIC;CCR;Drops;created\n'
            '20140511_1300;10023;0.9919;17;20111111_0000\n'
            '20140511_1300;10047;0.9963;18;20101010_0000\n')

        profile = Profile({
            "storage": {
                "type": "trend",
                "config": {
                    "granularity": 86400,
                    "datasource": "integration_test",
                    "timestamp_is_start": False
                }
            },
            "identifier": {
                "template": "Cell={CIC}",
                "regex": "(.*)"
            },
            "timestamp": {
                "type": "from_column",
                "config": {
                    "format": "%Y%m%d_%H%M",
                    "name": "ts",
                    "timezone": "UTC"
                }
            },
            "identifier_is_alias": False,
            "field_selector": {
                "type": "all"
            },
            "timestamp_is_start": True,
            "character_encoding": "utf-8",
            "dialect": {
                "type": "auto"
            },
            "fields": {
                "created": {
                    "datatype": "timestamp",
                    "string_format": {
                        "format": "%Y%m%d_%H%M"
                    }
                }
            }
        })

        import_csv(conn, profile, readfile)