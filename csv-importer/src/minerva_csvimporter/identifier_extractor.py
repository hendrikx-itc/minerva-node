# -*- coding: utf-8 -*-
import re
from functools import partial
import operator

from minerva.util import compose

from minerva_csvimporter.util import expand_kwargs
from minerva_csvimporter.data_requirements import any_field_empty
from minerva_csvimporter.data_extractor import DataExtractor


class IdentifierExtractor(DataExtractor):
    """
    Extracts/constructs the identifier (distinguised name) from records based
    on a template.
    """
    def __init__(self, template, regex):
        self.template = template
        self.regex = regex

        self.fields = re.findall("{([^}]+)}", template)

        # composed identifier (e.g. '{fld1}-{fld2}, {fld1}:{fld2}')
        get_identifier = expand_kwargs(template.format)

        extract_ident = partial(extract_identifier, regex)

        self.record_to_dn = compose(extract_ident, get_identifier)

    def __str__(self):
        return self.template

    def from_record(self, record):
        """
        Return a Distinguished Name constructed from the provided record.
        """
        return self.record_to_dn(record)

    def record_check(self):
        return compose(operator.not_, any_field_empty(self.fields))


def extract_identifier(pattern, value):
    """
    Return all groups matched by pattern, from value, joined as one string.
    """
    regex = re.compile(pattern)

    m = regex.match(value)

    return "".join(m.groups())
