# -*- coding: utf-8 -*-

from setuptools import setup

setup(
    name="minerva-node",
    author='Hendrikx ITC',
    author_email='info@hendrikx-itc.nl',
    version="v5.0.2",
    install_requires=[
        "minerva-etl>=5.1.0",
        "pika==0.13.0",
        "PyYAML"
    ],
    test_suite="nose.collector",
    packages=["minerva_node"],
    package_dir={"": "src"},
    package_data={"minerva_node": ["defaults/*"]},
    entry_points={
        'console_scripts': [
            'minerva-node = minerva_node.cli:main'
        ]
    }
)
