from minerva_csvimporter.values_extractor import ValuesExtractor


class FieldSelector(object):
    def check_header(self, header):
        raise NotImplementedError()

    def values_extractor(self, header):
        raise NotImplementedError()


class SpecificFieldSelector(FieldSelector):
    def __init__(self, fields):
        self.fields = fields

    def check_header(self, header):
        missing_fields = [
            name for name in self.fields if name not in header
        ]

        if missing_fields:
            raise Exception("Missing fields: {}".format(missing_fields))

    def values_extractor(self, header):
        return ValuesExtractor([
            name for name in self.fields if name in header
        ])


def create_field_selector(f):
    type_name = f["type"]

    if type_name == "select":
        return select_fields(f["config"]["names"])

    elif type_name == "exclude":
        return exclude_fields(f["config"]["names"])

    elif type_name == "all":
        return identity

    else:
        raise Exception("No such field selector: {}".format(type_name))


def select_fields(names):
    def fn(all_names):
        return [name for name in names if name in all_names]

    return fn


def exclude_fields(exclude_names):
    def fn(all_names):
        return [name for name in all_names if not name in exclude_names]

    return fn
