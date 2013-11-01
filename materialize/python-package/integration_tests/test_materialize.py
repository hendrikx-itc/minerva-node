from contextlib import closing
import datetime

from nose.tools import eq_
from minerva.test import with_conn, with_dataset
from minerva.directory.helpers_v4 import name_to_datasource, name_to_entitytype

from minerva.storage.trend.test import DataSet
from minerva.storage.trend.view import View
from minerva.storage.trend.trendstore import TrendStore, store_copy_from, \
    mark_modified
from minerva.storage.trend.datapackage import DataPackage
from minerva.storage.trend.granularity import create_granularity

from minerva_materialize.types import Materialization

from minerva_db import clear_database


class NormalSet(DataSet):
    def load(self, cursor):
        entitytype = name_to_entitytype(cursor, "materialize_dummytype001")
        self.datasource = name_to_datasource(cursor,
                                             "materialize_src_normal001")
        view_datasource = name_to_datasource(cursor, "vmaterialize_normal001")
        granularity = create_granularity('900')

        self.timestamp = self.datasource.tzinfo.localize(
            datetime.datetime(2013, 8, 26, 22, 0, 0))
        trend_names = ["cntr"]
        rows_small = [
            (1234, (55,)),
            (1235, (56,))]

        self.small_datapackage = DataPackage(granularity, self.timestamp,
                                             trend_names, rows_small)

        rows_large = [
            (1234, (55243444334,)),
            (1235, (56242343242,))]

        self.large_datapackage = DataPackage(granularity, self.timestamp,
                                             trend_names, rows_large)

        self.trendstore = TrendStore(self.datasource, entitytype, granularity,
                                     86400, 'table')
        self.trendstore.create(cursor)
        partition = self.trendstore.partition(self.timestamp)
        partition.create(cursor)
        self.trendstore.check_columns_exist(trend_names, ["smallint"])(cursor)
        modified = self.datasource.tzinfo.localize(datetime.datetime.now())
        store_copy_from(cursor, partition.table(), self.small_datapackage,
                        modified)
        mark_modified(cursor, partition.table(), self.timestamp, modified)

        view_trendstore = TrendStore(view_datasource, entitytype, granularity,
                                     0, 'view').create(cursor)
        sql = (
            "SELECT "
            "entity_id, "
            "timestamp, "
            'cntr FROM {}').format(self.trendstore.base_table().render())
        self.view = View(view_trendstore, sql).define(cursor).create(cursor)

    def update_type(self, cursor):
        self.trendstore.clear_timestamp(self.timestamp)(cursor)
        names = ["cntr"]
        types = ["bigint"]
        self.trendstore.check_column_types(names, types)(cursor)
        partition = self.trendstore.partition(self.timestamp)
        modified = self.datasource.tzinfo.localize(datetime.datetime.now())
        store_copy_from(cursor, partition.table(), self.large_datapackage,
                        modified)


@with_conn(clear_database)
@with_dataset(NormalSet)
def test_define_type_from_view(conn, dataset):
    with closing(conn.cursor()) as cursor:
        materialization = Materialization.define_from_view(dataset.view)(
            cursor)

        assert not materialization is None

        type_id = materialization.id

        assert type_id > 0


@with_conn(clear_database)
@with_dataset(NormalSet)
def test_materialize(conn, dataset):
    with closing(conn.cursor()) as cursor:
        materialization = Materialization.define_from_view(dataset.view)(
            cursor)

        materialization_chunk = materialization.chunk(dataset.timestamp)

        result = materialization_chunk.execute(cursor)

        assert not result is None


@with_conn(clear_database)
@with_dataset(NormalSet)
def test_materialize_altered_column(conn, dataset):
    conn.commit()

    with closing(conn.cursor()) as cursor:
        materialization = Materialization.define_from_view(dataset.view)(
            cursor)

        materialization_chunk = materialization.chunk(dataset.timestamp)

        max_modified, row_count = materialization_chunk.execute(cursor)

        eq_(row_count, 2)

        dataset.update_type(cursor)

        max_modified, row_count = materialization_chunk.execute(cursor)

        eq_(row_count, 2)

        conn.commit()
