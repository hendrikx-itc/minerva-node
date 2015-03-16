# -*- coding: utf-8 -*-
__docformat__ = "restructuredtext en"

__copyright__ = """
Copyright (C) 2008-2015 Hendrikx-ITC B.V.

Distributed under the terms of the GNU General Public License as published by
the Free Software Foundation; either version 3, or (at your option) any later
version.  The full license is in the file COPYING, distributed as part of
this software.
"""


class HarvestParser():
    @staticmethod
    def store_command():
        raise NotImplementedError()

    @staticmethod
    def packages(stream, file_name):
        """
        Return iterable of DataPackage objects.

        :param stream: A file-like object to read the data from
        :param file_name:
        :return: iterable(DataPackage)
        """
        raise NotImplementedError()


class HarvestPlugin():
    @staticmethod
    def create_parser(config):
        """
        Create and return new parser instance.

        A parser instance is a callable object that returns an iterator of
        data packages.

        :returns: A new parser object
        :rtype: HarvestParser
        """
        raise NotImplementedError()
