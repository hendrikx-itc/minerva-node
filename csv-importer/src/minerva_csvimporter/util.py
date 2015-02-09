# -*- coding: utf-8 -*-
__docformat__ = "restructuredtext en"

__copyright__ = """
Copyright (C) 2014 Hendrikx-ITC B.V.

Distributed under the terms of the GNU General Public License as published by
the Free Software Foundation; either version 3, or (at your option) any later
version.  The full license is in the file COPYING, distributed as part of
this software.
"""
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
        timestamp.tzinfo)
    #Deal with DST
    utc_offset_delta = timestamp.utcoffset() - ts_with_offset.utcoffset()

    return ts_with_offset + utc_offset_delta