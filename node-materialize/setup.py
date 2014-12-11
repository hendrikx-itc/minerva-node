# -*- coding: utf-8 -*-
__docformat__ = "restructuredtext en"

__copyright__ = """
Copyright (C) 2013 Hendrikx-ITC B.V.

Distributed under the terms of the GNU General Public License as published by
the Free Software Foundation; either version 3, or (at your option) any later
version.  The full license is in the file COPYING, distributed as part of
this software.
"""

from setuptools import setup

__author__ = "Hendrikx ITC"

setup(
    name="materialize",
    version="4.5.14",
    description=__doc__,
    author=__author__,
    author_email="info@hendrikx-itc.nl",
    url = "http://hendrikx-itc.nl",
    install_requires=["minerva>=4.5.3"],
    packages=["minerva_node_materialize"],
    package_dir={"": "src"},
    entry_points= {"node.plugins": ["materialize = minerva_node_materialize:MaterializePlugin"]})
