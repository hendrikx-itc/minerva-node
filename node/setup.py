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
    name="node",
    version="3.0.0",
    author="Hendrikx ITC",
    author_email="info@hendrikx-itc.nl",
    install_requires=["minerva>=3.0.0"],
    test_suite="nose.collector",
    namespace_packages=["minerva"],
    packages=["minerva", "minerva.node"],
    package_dir={"": "src"},
    package_data={"minerva.node": ["defaults/*"]},
    scripts=["scripts/minerva-node", "scripts/minerva-job-generator",
        "scripts/exec-node-job", "scripts/enqueue-node-job"])