# -*- coding: utf-8 -*-


def any_field_empty(field_names):
    """Return True if value of one of `field_names` in `record` equals ''."""
    def fn(record):
        return any(record[field_name] == "" for field_name in field_names)

    return fn