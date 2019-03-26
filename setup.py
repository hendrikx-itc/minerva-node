# -*- coding: utf-8 -*-

from setuptools import setup

setup(
    name="minerva-node",
    version="5.0.0",
    author="Hendrikx ITC",
    author_email="info@hendrikx-itc.nl",
    install_requires=["minerva>=5.0", "pika"],
    test_suite="nose.collector",
    packages=["minerva_node"],
    package_dir={"": "src"},
    package_data={"minerva_node": ["defaults/*"]},
    scripts=[
        "scripts/minerva-node",
    ]
)
