import logging
from itertools import imap, groupby
from functools import wraps

from minerva.util import expand_args, first
from minerva.directory.distinguishedname import explode


def entitytype_name_from_dn(dn):
	"""
	Return type of last component of distinguished name
	"""
	return explode(dn)[-1][0]


def datarecords_to_packages(records):
	records_with_key = ((r.get_key(), r) for r in records)

	sorted_records_with_key = sorted(records_with_key, key=first)

	return imap(expand_args(package), groupby(sorted_records_with_key, first))


class DataRecord(object):
	def __init__(self, timestamp, dn, granularity, field_names, values):
		self.timestamp = timestamp
		self.dn = dn
		self.granularity = granularity
		self.field_names = field_names
		self.values = values

	def __str__(self):
		return str(self.get_key())

	def get_key(self):
		entitytype_name = entitytype_name_from_dn(self.dn)

		return self.timestamp, entitytype_name, self.granularity


def package(key, records):
	timestamp, _entitytype_name, granularity = key

	all_field_names = set()
	dict_rows_by_dn = {}

	for _key, r in records:
		value_dict = dict(zip(r.field_names, r.values))

		dict_rows_by_dn.setdefault(r.dn, {}).update(value_dict)

		all_field_names.update(r.field_names)

	field_names = list(all_field_names)

	rows = []
	for dn, value_dict in dict_rows_by_dn.iteritems():
		values = [value_dict.get(f, "") for f in field_names]

		row = dn, values

		rows.append(row)

	out_pattern = "%Y-%m-%dT%H:%M:%S"

	return granularity, timestamp.strftime(out_pattern), field_names, rows
