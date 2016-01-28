# -*- coding: utf-8 -*-

from setuptools import setup

__author__ = "Hendrikx ITC"

setup(
    name="materialize",
    version="4.4.0",
    description=__doc__,
    author=__author__,
    author_email="info@hendrikx-itc.nl",
    url = "http://hendrikx-itc.nl",
    packages=["minerva_node_materialize"],
    package_dir={"": "src"},
    entry_points= {"node.plugins": ["materialize = minerva_node_materialize:MaterializePlugin"]})
