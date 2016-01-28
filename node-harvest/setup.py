# -*- coding: utf-8 -*-

from setuptools import setup

__author__ = "Hendrikx ITC"

setup(
    name="minerva-node-harvest",
    version="5.0.0",
    description=__doc__,
    author=__author__,
    author_email="a.j.n.blokland@hendrikx-itc.nl",
    url="http://hendrikx-itc.nl",
    install_requires=[
        "minerva>=5.0",
        "minerva-harvesting"
    ],
    packages=["minerva_node_harvest"],
    package_dir={"": "src"},
    entry_points={"node.plugins": ["harvest = minerva_node_harvest:HarvestPlugin"]}
)
