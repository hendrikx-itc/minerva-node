from contextlib import closing
import gzip

from harvest import HarvestJob

from minerva.directory.helpers import create_datasource
from minerva.directory.existence import Existence
from minerva_harvesting.pluginapi import HarvestPlugin
from minerva_node import MinervaContext


from psycopg2 import connect


class DummyParser(object):
    def parse(self, data, file_name):
        pass


class IntegerParser(object):
    EXPECTED_VALUE = 42

    def parse(self, data, file_name):
        line = data.readline()

        assert int(line) == IntegerParser.EXPECTED_VALUE


test_parsers = {
    'dummy': DummyParser,
    'integer': IntegerParser
}


class TestPlugin(HarvestPlugin):
    @staticmethod
    def storagetype():
        return 'trend'

    @staticmethod
    def create_parser(rawdatapackage_handler, config):
        return test_parsers[config['sub-type']]()


def test_execute():
    file_path = '/tmp/data.csv'

    with open(file_path, 'w') as test_file:
        test_file.write('42\n')

    with closing(connect('')) as conn:
        create_datasource(
            conn, 'pm-system-1', 'datasource for integration test',
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
                "datatype": "test-data",
                "on_success": {
                    "name": "do_nothing",
                    "args": []
                },
                "on_failure": {
                    "name": "do_nothing",
                    "args": []
                },
                "parser_config": {
                    "sub-type": "dummy"
                },
                "uri": file_path,
                "datasource": "pm-system-1"
            }
        )

        job.execute()


def test_execute_gzipped():
    file_path = '/tmp/data.csv.gz'

    with gzip.open(file_path, 'w') as test_file:
        test_file.write('{}\n'.format(IntegerParser.EXPECTED_VALUE))

    with closing(connect('')) as conn:
        create_datasource(
            conn, 'pm-system-1', 'datasource for integration test',
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
                "datatype": "test-data",
                "on_success": {
                    "name": "do_nothing",
                    "args": []
                },
                "on_failure": {
                    "name": "do_nothing",
                    "args": []
                },
                "parser_config": {
                    "sub-type": "integer"
                },
                "uri": file_path,
                "datasource": "pm-system-1"
            }
        )

        job.execute()
