from datetime import datetime
from contextlib import closing
import gzip

import pytz

from minerva_node_harvest import HarvestJob

from minerva.test import clear_database
from minerva.directory import DataSource, EntityType
from minerva.directory.existence import Existence
from minerva.storage.trend.granularity import create_granularity
from minerva.storage.trend.datapackage import DefaultPackage
from minerva.storage.trend import TrendDescriptor
from minerva.storage.trend.tabletrendstore import TableTrendStore, \
    TableTrendStoreDescriptor
from minerva.storage.trend.engine import TrendEngine
from minerva.storage import datatype
from minerva_harvesting.pluginapi import HarvestPlugin
from minerva_node import MinervaContext


from psycopg2 import connect


class DummyParser(object):
    @staticmethod
    def parse(data, file_name):
        return tuple()


class IntegerParser(object):
    EXPECTED_VALUE = 42

    @staticmethod
    def parse(data, file_name):
        line = data.readline()

        yield TrendEngine.store(
            DefaultPackage(
                create_granularity('3600 seconds'),
                pytz.utc.localize(datetime(2015, 2, 27, 15, 0)),
                ['x'],
                [
                    ('Node=001', (int(line), ))
                ]
            )
        )


test_parsers = {
    'dummy': DummyParser,
    'integer': IntegerParser
}


class TestPlugin(HarvestPlugin):
    @staticmethod
    def storage_type():
        return 'trend'

    @staticmethod
    def create_parser(config):
        return test_parsers[config['sub-type']]()


def test_execute():
    file_path = '/tmp/data.csv'

    with open(file_path, 'w') as test_file:
        test_file.write('42\n')

    with closing(connect('')) as conn:
        clear_database(conn)

        with closing(conn.cursor()) as cursor:
            DataSource.create(
                'pm-system-1', 'data source for integration test'
            )(cursor)

        job = HarvestJob(
            id_=1000,
            plugins={
                'test-data': TestPlugin()
            },
            existence=Existence(conn),
            minerva_context=MinervaContext(conn, conn),
            description={
                "data_type": "test-data",
                "on_success": [
                    "do_nothing"
                ],
                "on_failure": [
                    "do_nothing"
                ],
                "parser_config": {
                    "sub-type": "dummy"
                },
                "uri": file_path,
                "data_source": "pm-system-1"
            }
        )

        job.execute()


def test_execute_gzipped():
    file_path = '/tmp/data.csv.gz'

    with gzip.open(file_path, 'wt') as test_file:
        test_file.write('{}\n'.format(IntegerParser.EXPECTED_VALUE))

    with closing(connect('')) as conn:
        clear_database(conn)

        with closing(conn.cursor()) as cursor:
            data_source = DataSource.create(
                'pm-system-1', 'data source for integration test'
            )(cursor)

            entity_type = EntityType.create(
                'Node', 'entity type for integration test'
            )(cursor)

            TableTrendStore.create(TableTrendStoreDescriptor(
                data_source,
                entity_type,
                create_granularity('3600 seconds'),
                [
                    TrendDescriptor('x', datatype.Integer, '')
                ],
                86400 * 7
            ))(cursor)

        conn.commit()

        job = HarvestJob(
            id_=1001,
            plugins={
                'test-data': TestPlugin()
            },
            existence=Existence(conn),
            minerva_context=MinervaContext(conn, conn),
            description={
                "data_type": "test-data",
                "on_success": [
                    "do_nothing"
                ],
                "on_failure": [
                    "do_nothing"
                ],
                "parser_config": {
                    "sub-type": "integer"
                },
                "uri": file_path,
                "data_source": "pm-system-1"
            }
        )

        job.execute()
