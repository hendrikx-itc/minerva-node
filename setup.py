# -*- coding: utf-8 -*-

from setuptools import setup

setup(
    name="minerva-node",
    author='Hendrikx ITC',
    author_email='info@hendrikx-itc.nl',
    version="5.0,0.dev1",
    install_requires=[
        "minerva>=5.0.0.dev2",
        "pika"
    ],
    test_suite="nose.collector",
    packages=["minerva_node"],
    package_dir={"": "src"},
    package_data={"minerva_node": ["defaults/*"]},
    scripts=[
        "scripts/minerva-node",
    ]
)
