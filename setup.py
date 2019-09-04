# -*- coding: utf-8 -*-

from setuptools import setup

setup(
    name="minerva-node",
    author='Hendrikx ITC',
    author_email='info@hendrikx-itc.nl',
    version="5.0,0.dev25",
    install_requires=[
        "minerva-etl>=5.0.0.dev3",
        "pika==0.13.0"
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
