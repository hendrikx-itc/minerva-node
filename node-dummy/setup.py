# -*- coding: utf-8 -*-

from setuptools import setup

__author__ = "Hendrikx ITC"

setup(
    name="dummy",
    version="4.4.0",
    description=__doc__,
    author=__author__,
    author_email="a.j.n.blokland@hendrikx-itc.nl",
    url = "http://hendrikx-itc.nl",
    packages=["dummy"],
    package_dir={"": "src"},
    entry_points= {"node.plugins": ["dummy = dummy:DummyPlugin"]}
    )
