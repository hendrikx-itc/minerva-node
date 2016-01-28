# -*- coding: utf-8 -*-

from setuptools import setup

setup(
    name="CSVImporter",
    version="4.4.0",
    author="Hendrikx ITC",
    author_email="info@hendrikx-itc.nl",
    install_requires=["minerva>=3", "configobj"],
    test_suite="nose.collector",
    packages=["minerva_csvimporter", "minerva_csvimporter/storage"],
    package_dir={"": "src"},
    package_data={"minerva_csvimporter": ["defaults/*"]},
    scripts=["scripts/import-csv"]
)
