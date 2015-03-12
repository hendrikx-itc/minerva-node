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

import pkg_resources
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
    return pkg_resources.resource_string(
        "minerva_node", "defaults/{}".format(name)
    ).decode(encoding='UTF-8')


def load_config(defaults, path):
    """
    Load a configuration from file `path`, merge it with the default
    configuration and return a ConfigObj instance. So if any config option is
    missing, it is filled in with a default.

    Raises a :exc:`ConfigError` when the specified file doesn't exist.
    """
    if not os.path.isfile(path):
        raise ConfigError("config file '{0}' doesn't exist".format(path))

    config = ConfigObj(defaults)
    custom_config = ConfigObj(path)
    config.merge(custom_config)

    return config
