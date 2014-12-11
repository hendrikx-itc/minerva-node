from subprocess import Popen, PIPE
from contextlib import closing

from nose.tools import eq_

from minerva.directory.helpers_v4 import DataSource, EntityType

from minerva_db import connect


def test_cmd_1():
    file_path = "/tmp/test.csv"
    datasource_name = "test-src"

    with open(file_path, "wt") as csv_file:
        csv_file.write(
            "type, name, a, b, c\n"
            "Node, 001, 12, 232, high\n"
            "Node, 002, 43, 334, medium\n")

    with closing(connect()) as conn:
        with closing(conn.cursor()) as cursor:
            DataSource.from_name(cursor, datasource_name)
            EntityType.from_name(cursor, 'Node')

        conn.commit()

    cmd = [
        "import-csv", "--timestamp", "now",
        "--identifier", "Node={name}", "--datasource", datasource_name,
        file_path
    ]
    p = Popen(cmd, stdout=PIPE, stdin=PIPE, stderr=PIPE)

    stdout_data, stderr_data = p.communicate(input=None)

    eq_('', stderr_data)

    eq_('', stdout_data)


def test_cmd_2():
    file_path = "/tmp/test.csv"
    datasource_name = "test-src"

    with open(file_path, "wt") as csv_file:
        csv_file.write(
            "type, name, a, b, c\n"
            "Node, 001, 12, 232, high\n"
            "Node, 002, 43, 334, medium\n")

    cmd = [
        "import-csv", "--timestamp", "now",
        "--identifier", "Node={name}", "--datasource", datasource_name,
        file_path
    ]
    p = Popen(cmd, stdout=PIPE, stdin=PIPE, stderr=PIPE)

    stdout_data, stderr_data = p.communicate(input=None)

    eq_('', stderr_data)

    eq_('', stdout_data)


def test_cmd_identifier_regex():
    file_path = "/tmp/test.csv"
    datasource_name = "test-src"

    with open(file_path, "wt") as csv_file:
        csv_file.write(
            "type, name, a, b, c\n"
            "Node, node0001, 12, 232, high\n"
            "Node, node0102, 43, 334, medium\n")

    cmd = [
        "import-csv", "--timestamp", "now",
        "--identifier", "Node={name}", "--identifier-regex",
        "(Node=)node[0]+(\\d+)", "--datasource", datasource_name, file_path
    ]

    p = Popen(cmd, stdout=PIPE, stdin=PIPE, stderr=PIPE)

    stdout_data, stderr_data = p.communicate(input=None)

    eq_('', stderr_data)

    eq_('', stdout_data)


def test_cmd_fixed_identifier():
    datasource_name = "test-src"

    data = (
        u"a, b, c\n"
        "12, 232, high\n"
        "43, 334, medium\n"
    )

    cmd = [
        "import-csv", "--timestamp", "now",
        "--identifier", "Node=001",
        "--storage-type", "notification",
        "--datasource", datasource_name
    ]

    p = Popen(cmd, stdout=PIPE, stdin=PIPE, stderr=PIPE)

    stdout_data, stderr_data = p.communicate(input=data)

    eq_('', stderr_data)

    eq_('', stdout_data)
