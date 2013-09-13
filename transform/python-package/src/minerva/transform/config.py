# -*- coding: utf-8 -*-
__docformat__ = "restructuredtext en"

__copyright__ = """
Copyright (C) 2008-2012 Hendrikx-ITC B.V.

Distributed under the terms of the GNU General Public License as published by
the Free Software Foundation; either version 3, or (at your option) any later
version.  The full license is in the file COPYING, distributed as part of
this software.
"""
import os

from pkg_resources import resource_string
from configobj import ConfigObj


class ConfigError(Exception):
    """
    Base class for any configuration loading related exceptions.
    """
    pass


def get_defaults(name):
    """
    Return a string representing a default/template config file named `name`.
    """
    return resource_string("minerva.transform", "defaults/{}".format(name))
