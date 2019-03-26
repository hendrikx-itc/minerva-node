# -*- coding: utf-8 -*-
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

consumer_settings = {
    "url": "amqp://guest:guest@rabbit:5672/%2F?connection_attempts=3&heartbeat_interval=3600",
    "queue": "aireas",
    "routing_key": "minerva",
    "logger": None
}

database_settings = {
    'dbname': 'minerva',
    'user': 'postgres',
    'host': 'database',
    'password': 'postgres'
}
