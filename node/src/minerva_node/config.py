# -*- coding: utf-8 -*-
import os
import json


class ConfigError(Exception):
    """
    Base class for any configuration loading related exceptions.
    """
    pass


def load_config(path):
    """
    Load a configuration from file `path`
    """
    if not os.path.isfile(path):
        raise ConfigError("config file '{0}' doesn't exist".format(path))

    with open(path) as config_file:
        config = json.load(config_file)

    return config
