from minerva.directory.helpers import create_entitytype
from minerva.util import first, head, tail
from minerva.util.tabulate import render_table


def get_entitytype(conn, name, description):
	try:
		entitytype = get_entitytype(conn, name)
	except:
		entitytype = create_entitytype(conn, name, description)

	return entitytype


def lines(sep="\n", lines=""):
	"""
	Split string on line separators and return list with lines.
	"""
	return lines.split(sep)


def unlines(lines):
	"""
	Join lines with a newline character in between.
	"""
	return "\n".join(lines)


def render_source(source):
	"""
	Renders a data 'source' in the form of a table-like object:

	[
		('column_1', 'column_2', 'column_3', ...),
		(1, 2, 3,...),
		(4, 5, 6,...),
		...
	]
	"""
	column_names = head(source)
	column_align = ">" * len(column_names)
	column_sizes = ["max"] * len(column_names)
	rows = tail(source)

	return render_table(column_names, column_align, column_sizes, rows)
