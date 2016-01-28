# -*- coding: utf-8 -*-
import pytz


def expand_kwargs(fn):
    def expander(kwargs):
        return fn(**kwargs)

    return expander


def as_functor(x):
    def fmap(f):
        return f(x)

    return fmap


def offset_timestamp(offset, timestamp):
    ts_with_offset = (timestamp.astimezone(pytz.utc) + offset).astimezone(
        timestamp.tzinfo
    )
    # Deal with DST
    utc_offset_delta = timestamp.utcoffset() - ts_with_offset.utcoffset()

    return ts_with_offset + utc_offset_delta