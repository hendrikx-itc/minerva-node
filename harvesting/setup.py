# -*- coding: utf-8 -*-

from setuptools import setup

setup(
    name="minerva-harvesting",
    version="5.0.0",
    author="Hendrikx ITC",
    author_email="info@hendrikx-itc.nl",
    install_requires=["minerva>=5.0.0"],
    test_suite="nose.collector",
    packages=["minerva_harvesting"],
    package_dir={"": "src"},
    package_data={"minerva_harvesting": ["defaults/*"]},
    scripts=[
        "scripts/processfile",
        "scripts/checkfile"
    ]
)
