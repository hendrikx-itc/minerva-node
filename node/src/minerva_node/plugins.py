# -*- coding: utf-8 -*-
import pkg_resources

ENTRY_POINT = "node.plugins"


def load_plugins():
    """
    Load and return a list with plugins.
    """
    return [
        entry_point.load()
        for entry_point in pkg_resources.iter_entry_points(group=ENTRY_POINT)
    ]
