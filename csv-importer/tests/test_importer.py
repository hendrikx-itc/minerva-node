from nose.tools import assert_true, assert_false, raises, eq_

from minerva_csvimporter.importer import is_field_empty, check_header, \
    DataError, remove_nul


def test_is_field_empty():
    record = {
        "cic": 10,
        "name": ""
    }

    name_empty = is_field_empty("name", record)

    assert_true(name_empty)

    cic_empty = is_field_empty("cic", record)

    assert_false(cic_empty)


@raises(DataError)
def test_check_header_exc():
    header1 = ["first", "", ""]

    check_header(header1, ["first"])


def test_check_header_no_exc():
    header1 = ["first", "second", "third"]

    check_header(header1, ["first", "third"])


def test_remove_nul():
    lines = [
        "some\0 text\0"
    ]

    lines_without_nul = list(remove_nul(lines))

    eq_(lines_without_nul[0], "some text")
