# -*- coding: utf-8 -*-
"""
Unit tests for the core.datatype module
"""
__docformat__ = "restructuredtext en"

__copyright__ = """
Copyright (C) 2008-2010 Hendrikx-ITC B.V.

Distributed under the terms of the GNU General Public License as published by
the Free Software Foundation; either version 3, or (at your option) any later
version.  The full license is in the file COPYING, distributed as part of
this software.
"""
from nose.tools import assert_raises, assert_true, assert_false, assert_equal

from minerva.dispatcher import Consumer 
from minerva.system import DataSource

from minerva.core.minervaexception import MinervaException


def test_dispatcher():
    dispatcher = Dispatcher(datasources=[], max_queue_size=10, harvester_conn_provider=None)

def test_register_datasource():
    datasources = []

    def do_nothing():
        pass

    def do_nothing():
        pass

    datasource = DataSource("test_source", "measurement", "/data/measurement_data")
    datasource.recursive = True
    datasource.on_success = do_nothing
    datasource.on_error = do_nothing
    datasources.append(datasource)

    dispatcher = Dispatcher(datasources=datasources, max_queue_size=10, harvester_conn_provider=None)
