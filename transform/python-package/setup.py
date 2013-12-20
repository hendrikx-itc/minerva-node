# -*- coding: utf-8 -*-
__docformat__ = "restructuredtext en"

__copyright__ = """
Copyright (C) 2008-2012 Hendrikx-ITC B.V.

Distributed under the terms of the GNU General Public License as published by
the Free Software Foundation; either version 3, or (at your option) any later
version.  The full license is in the file COPYING, distributed as part of
this software.
"""

from setuptools import setup

setup(
    name="transforming",
    version="4.5.3",
    author="Hendrikx ITC",
    author_email="info@hendrikx-itc.nl",
    install_requires=["minerva>=4.5.3"],
    test_suite="nose.collector",
    packages=["minerva_transform"],
    package_dir={"": "src"},
    package_data={"minerva_transform": ["defaults/*"]},
    scripts=["scripts/create-transform-functions", "scripts/transform",
             "scripts/create-transform-jobs", "scripts/compile-backlog"]
)
