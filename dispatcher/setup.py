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

setup(name="dispatcher",
    version="4.5.8",
    install_requires=["configobj",
        "minerva>=4.5.3", "pyinotify"],
    test_suite="nose.collector",
    package_data={"minerva_dispatcher": ["defaults/*"]},
    packages=["minerva_dispatcher"],
    package_dir={"": "src"},
    scripts=["scripts/dispatcher", "scripts/enqueue-job", "scripts/sweep-jobs"])
