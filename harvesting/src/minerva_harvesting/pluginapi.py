# -*- coding: utf-8 -*-


class HarvestParser:
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


class HarvestPlugin:
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
