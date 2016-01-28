# -*- coding: utf-8 -*-


class DataExtractor(object):
    def from_record(self, record):
        raise NotImplementedError()

    def header_check(self):
        return None

    def record_check(self):
        return None
