from contextlib import closing
import gzip

from harvest import HarvestJob

from minerva.directory.helpers import create_datasource
from minerva.directory.existence import Existence
from minerva_harvesting.pluginapi import HarvestPlugin
from minerva_node import MinervaContext


from psycopg2 import connect


class DummyParser(object):
    @staticmethod
    def parse(data, file_name):
        pass


class IntegerParser(object):
    EXPECTED_VALUE = 42

    @staticmethod
    def parse(data, file_name):
        line = data.readline()

        assert int(line) == IntegerParser.EXPECTED_VALUE


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
        create_datasource(
            conn, 'pm-system-1', 'data source for integration test',
            'Europe/Amsterdam'
        )

        job = HarvestJob(
            plugins={
                'test-data': TestPlugin()
            },
            existence=Existence(conn),
            minerva_context=MinervaContext(conn, conn),
            id=1000,
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

    with gzip.open(file_path, 'w') as test_file:
        test_file.write('{}\n'.format(IntegerParser.EXPECTED_VALUE))

    with closing(connect('')) as conn:
        create_datasource(
            conn, 'pm-system-1', 'data source for integration test',
            'Europe/Amsterdam'
        )

        job = HarvestJob(
            plugins={
                'test-data': TestPlugin()
            },
            existence=Existence(conn),
            minerva_context=MinervaContext(conn, conn),
            id=1001,
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
