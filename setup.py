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
    name="CSVImporter",
    version="3.0.0",
    author="Hendrikx ITC",
    author_email="info@hendrikx-itc.nl",
    install_requires=["minerva>=3", "configobj"],
    test_suite="nose.collector",
    namespace_packages=["minerva"],
    packages=["minerva", "minerva.csvimporter"],
    package_dir={"": "src"},
    package_data = {"minerva.csvimporter": ["defaults/*"]},
    scripts=["scripts/import-csv"])
