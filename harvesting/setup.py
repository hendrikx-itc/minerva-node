# -*- coding: utf-8 -*-
__docformat__ = "restructuredtext en"

__copyright__ = """
Copyright (C) 2008-2013 Hendrikx-ITC B.V.

Distributed under the terms of the GNU General Public License as published by
the Free Software Foundation; either version 3, or (at your option) any later
version.  The full license is in the file COPYING, distributed as part of
this software.
"""

from setuptools import setup

setup(
    name="minerva-harvesting",
    version="4.5.7",
    author="Hendrikx ITC",
    author_email="info@hendrikx-itc.nl",
    install_requires=["minerva>=4.5.3", "progressbar"],
    test_suite="nose.collector",
    packages=["minerva_harvesting"],
    package_dir={"": "src"},
    package_data={"minerva_harvesting": ["defaults/*"]},
    scripts=["scripts/processfile", "scripts/checkfile"])
