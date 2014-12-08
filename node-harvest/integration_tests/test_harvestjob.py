from contextlib import closing

from harvest import HarvestJob

from minerva.directory.helpers import create_datasource
from minerva.directory.existence import Existence
from minerva_harvesting.pluginapi import HarvestPlugin
from minerva_node import MinervaContext


from psycopg2 import connect


class DummyParser(object):
    def parse(self, data, file_name):
        pass


class DummyPlugin(HarvestPlugin):
    @staticmethod
    def storagetype():
        return 'trend'

    @staticmethod
    def create_parser(rawdatapackage_handler, config):
        return DummyParser()


def test_execute():
    plugins = {
        'dummy-data': DummyPlugin()
    }

    job_id = 42
    description = {
        "datatype": "dummy-data",
        "on_success": {
            "name": "do_nothing",
            "args": []
        },
        "on_failure": {
            "name": "do_nothing",
            "args": []
        },
        "parser_config": {
        },
        "uri": "/tmp/data.csv",
        "datasource": "pm-system-1"
    }

    with closing(connect('')) as conn:
        create_datasource(
            conn, 'pm-system-1', 'datasource for integration test',
            'Europe/Amsterdam'
        )

        existence = Existence(conn)

        minerva_context = MinervaContext(conn, conn)

        job = HarvestJob(
            plugins, existence, minerva_context, job_id, description
        )

        job.execute()