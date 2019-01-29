# -*- coding: utf-8 -*-
"""Provides the JobSource class."""


class JobSource:
    """
    Encapsulates job source and provides loading and creating functionality.

    The default config serialization an deserialization functionality can be
    overridden in sub-classes to provide more elegant access to the type
    specific configuration.

    """

    def __init__(self, name, job_type, config):
        self.name = name
        self.job_type = job_type
        self.config = config
