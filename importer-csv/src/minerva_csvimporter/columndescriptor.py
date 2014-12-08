from minerva_csvimporter import datatype


class ColumnDescriptor(object):
    def __init__(self, name, data_type, string_format):
        self.name = name
        self.data_type = data_type
        self.string_format = string_format

    def string_parser(self):
        return self.data_type.string_parser(
            self.data_type.string_parser_config(self.string_format)
        )


def create_column_descriptor(name, conf):
    data_type = datatype.type_map[conf["datatype"]]

    return ColumnDescriptor(name, data_type, conf["string_format"])
