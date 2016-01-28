#!/usr/bin/env python
# -*- coding: utf-8 -*-
from minerva.storage.trend.datapackage import DefaultPackage
from minerva.util import expand_args, first, grouped_by
from minerva.directory.distinguishedname import entity_type_name_from_dn, \
    InvalidDistinguishedNameError


def records_to_packages(records):
    records_with_key = ((r.get_key(), r) for r in records)

    def record_has_key(record_with_key):
        key, record = record_with_key

        return key is not None

    records_with_key = filter(record_has_key, records_with_key)

    return map(expand_args(package), grouped_by(records_with_key, first))


class DataRecord:
    def __init__(self, timestamp, dn, granularity, field_names, values):
        self.timestamp = timestamp
        self.dn = dn
        self.granularity = granularity
        self.field_names = field_names
        self.values = values

    def __str__(self):
        return str(self.get_key())

    def get_key(self):
        try:
            entity_type_name = entity_type_name_from_dn(self.dn)
        except InvalidDistinguishedNameError:
            return None
        else:
            return self.timestamp, entity_type_name, self.granularity


def package(key, records):
    timestamp, _entity_type_name, granularity = key

    all_field_names = set()
    dict_rows_by_dn = {}

    for _key, r in records:
        value_dict = dict(zip(r.field_names, r.values))

        dict_rows_by_dn.setdefault(r.dn, {}).update(value_dict)

        all_field_names.update(r.field_names)

    field_names = list(all_field_names)

    rows = [
        (dn, [value_dict.get(f, "") for f in field_names])
        for dn, value_dict in dict_rows_by_dn.items()
    ]

    return DefaultPackage(granularity, timestamp, field_names, rows)
