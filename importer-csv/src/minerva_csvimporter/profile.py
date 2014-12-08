# -*- coding: utf-8 -*-
__docformat__ = "restructuredtext en"

__copyright__ = """
Copyright (C) 2014 Hendrikx-ITC B.V.

Distributed under the terms of the GNU General Public License as published by
the Free Software Foundation; either version 3, or (at your option) any later
version.  The full license is in the file COPYING, distributed as part of
this software.
"""
from minerva.util import identity

from minerva_csvimporter import storage
from minerva_csvimporter.identifier_extractor import IdentifierExtractor
from minerva_csvimporter.timestamp_extractor import create_timestamp_fn
from minerva_csvimporter.dialects import create_dialect
from minerva_csvimporter.util import expand_kwargs
from minerva_csvimporter.columndescriptor import create_column_descriptor


GRANULARITIES = {
    "qtr": 900,
    "hr": 3600,
    "day": 86400,
    "week": 604800}


def ensure_type(required_type, map_to_type=None):
    def fn(value):
        if value is not None:
            if isinstance(value, required_type):
                return value
            else:
                if map_to_type is not None:
                    return map_to_type(value)
                else:
                    return required_type(value)

    return fn


def type_mapping(schema):
    def fn(data):
        return {k: schema.get(k, identity)(v) for k, v in data.iteritems()}

    return fn


def to_storage(s):
    return storage.type_map[s["type"]](**s.get("config", {}))


def create_field_selector(f):
    type_name = f["type"]

    if type_name == "select":
        return select_fields(f["config"]["names"])

    elif type_name == "exclude":
        return exclude_fields(f["config"]["names"])

    elif type_name == "all":
        return identity

    else:
        raise Exception("No such field selector: {}".format(type_name))


def select_fields(names):
    def fn(all_names):
        return [name for name in names if name in all_names]

    return fn


def exclude_fields(exclude_names):
    def fn(all_names):
        return [name for name in all_names if not name in exclude_names]

    return fn


def create_row_mapping(conf):
    if conf:
        return {
            name: create_column_descriptor(name, sub_conf)
            for name, sub_conf in conf.iteritems()
        }
    else:
        return {}


def to_granularity_seconds(g):
    return int(GRANULARITIES.get(g, g))


def to_bool(b):
    return b.lower() in ('t', 'true', '1')


PROFILE_SCHEMA = {
    "storage": ensure_type(storage.Storage, to_storage),
    "field_selector": create_field_selector,
    "timezone": str,
    "identifier": expand_kwargs(IdentifierExtractor),
    "timestamp": create_timestamp_fn,
    "character_encoding": str,
    "dialect": create_dialect,
    "fields": create_row_mapping
}


class Profile(object):
    def __init__(self, configuration):
        typed_dict = type_mapping(PROFILE_SCHEMA)(configuration)

        for name, value in typed_dict.iteritems():
            setattr(self, name, value)
