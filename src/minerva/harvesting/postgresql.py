import logging
from functools import partial
from contextlib import closing
import StringIO


def drop_table(conn, name):
	sql = "DROP TABLE IF EXISTS {}".format(name)

	exec_sql(conn, sql)


def create_temp_table(conn, name, columns):
	columns_part = ",".join(columns)

	sql = "CREATE TEMP TABLE {} ({})".format(name, columns_part)

	exec_sql(conn, sql)


def create_index(conn, table, columns):
	name = "ix_{0}_{1}".format(table, columns[0])

	sql = "CREATE INDEX {0} ON {0} ({2})".format(name, table, ",".join(columns))

	exec_sql(conn, sql)


def create_unique_index(conn, table, columns):
	name = "ix_{0}_{1}".format(table, columns[0])

	sql = "CREATE UNIQUE INDEX {0} ON {1} ({2})".format(name, table, ",".join(columns))

	exec_sql(conn, sql)


def exec_sql(conn, *args, **kwargs):
	with closing(conn.cursor()) as cursor:
		cursor.execute(*args, **kwargs)


def enquote_column_name(name):
	return '"{}"'.format(name)


def create_copy_from_query(table, columns):
	columns_part = ",".join(map(enquote_column_name, columns))

	return "COPY {0}({1}) FROM STDIN".format(table, columns_part)


def create_copy_from_file(tuples, formats):
	formatters = create_formatters(*formats)

	format_tuple = partial(zipapply, formatters)

	copy_from_file = StringIO.StringIO()

	copy_from_file.writelines("{}\n".format("\t".join(format_tuple(tup))) for tup in tuples)

	copy_from_file.seek(0)

	return copy_from_file


def create_formatters(*args):
	return [partial(str.format, "{:" + f + "}") for f in args]


def zipapply(functions, values):
	return [f(v) for f, v in zip(functions, values)]
