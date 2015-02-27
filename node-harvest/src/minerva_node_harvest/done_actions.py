# -*- coding: utf-8 -*-
__docformat__ = "restructuredtext en"

__copyright__ = """
Copyright (C) 2012-2015 Hendrikx-ITC B.V.

Distributed under the terms of the GNU General Public License as published by
the Free Software Foundation; either version 3, or (at your option) any later
version.  The full license is in the file COPYING, distributed as part of
this software.
"""
import logging
import os


def remove():
    def f(path):
        try:
            os.remove(path)
        except Exception as exc:
            logging.warning(str(exc))

    return f


def move_to(target):
    def f(path):
        directory, filename = os.path.split(path)
        new = os.path.join(target, filename)

        try:
            os.rename(path, new)
        except Exception as exc:
            logging.warning(str(exc))

    return f


def do_nothing():
    def f(path):
        pass

    return f


action_map = {
    "remove": remove,
    "move_to": move_to,
    "do_nothing": do_nothing
}


def execute_action(uri, action):
    if action:
        name, args = action[0], action[1:]

        action_map[name](*args)(uri)
