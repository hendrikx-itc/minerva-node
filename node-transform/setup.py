# -*- coding: utf-8 -*-
__docformat__ = "restructuredtext en"

__copyright__ = """
Copyright (C) 2012 Hendrikx-ITC B.V.

Distributed under the terms of the GNU General Public License as published by
the Free Software Foundation; either version 3, or (at your option) any later
version.  The full license is in the file COPYING, distributed as part of
this software.
"""

from setuptools import setup

__author__ = "Hendrikx ITC"

setup(
    name="transform",
    version="1.0.0",
    description=__doc__,
    author=__author__,
    author_email="a.j.n.blokland@hendrikx-itc.nl",
    url = "http://hendrikx-itc.nl",
    packages=["minerva_node_transform"],
    package_dir={"": "src"},
    entry_points= {"node.plugins": ["transform = minerva_node_transform:TransformPlugin"]}
    )