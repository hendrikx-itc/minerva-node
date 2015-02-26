from subprocess import Popen, PIPE
from contextlib import closing

from minerva.directory import DataSource, EntityType
from minerva.test import eq_, with_conn, clear_database


@with_conn(clear_database)
def test_cmd_1(conn):
    file_path = "/tmp/test.csv"
    data_source_name = "test-src"

    with open(file_path, "wt") as csv_file:
        csv_file.write(
            "type, name, a, b, c\n"
            "Node, 001, 12, 232, high\n"
            "Node, 002, 43, 334, medium\n")

    with closing(conn.cursor()) as cursor:
        DataSource.from_name(data_source_name)(cursor)
        EntityType.from_name('Node')(cursor)

    conn.commit()

    cmd = [
        "import-csv", "--timestamp", "now",
        "--identifier", "Node={name}", "--data-source", data_source_name,
        file_path
    ]
    p = Popen(cmd, stdout=PIPE, stdin=PIPE, stderr=PIPE)

    stdout_data, stderr_data = p.communicate(input=None)

    eq_('', stderr_data)

    eq_('', stdout_data)


@with_conn(clear_database)
def test_cmd_2(conn):
    file_path = "/tmp/test.csv"
    with closing(conn.cursor()) as cursor:
        data_source = DataSource.create("test-src", "")(cursor)
        entity_type = EntityType.create("Node", "")(cursor)

    conn.commit()

    with open(file_path, "wt") as csv_file:
        csv_file.write(
            "type, name, a, b, c\n"
            "Node, 001, 12, 232, high\n"
            "Node, 002, 43, 334, medium\n")

    cmd = [
        "import-csv", "--timestamp", "now",
        "--identifier", "Node={name}", "--data-source", data_source.name,
        file_path
    ]
    p = Popen(cmd, stdout=PIPE, stdin=PIPE, stderr=PIPE)

    stdout_data, stderr_data = p.communicate(input=None)

    eq_('', stderr_data)

    eq_('', stdout_data)


@with_conn(clear_database)
def test_cmd_identifier_regex(conn):
    with closing(conn.cursor()) as cursor:
        data_source = DataSource.create("test-src", "")(cursor)

    test_file = (
        "type, name, a, b, c\n"
        "Node, node0001, 12, 232, high\n"
        "Node, node0102, 43, 334, medium\n"
    ).encode()

    cmd = [
        "import-csv", "--timestamp", "now",
        "--identifier", "Node={name}", "--identifier-regex",
        "(Node=)node[0]+(\\d+)", "--data-source", data_source.name
    ]

    p = Popen(cmd, stdout=PIPE, stdin=PIPE, stderr=PIPE)

    stdout_data, stderr_data = p.communicate(input=test_file)

    eq_('', stderr_data)

    eq_('', stdout_data)


@with_conn(clear_database)
def test_cmd_fixed_identifier(conn):
    with closing(conn.cursor()) as cursor:
        data_source = DataSource.create("test-src", "")(cursor)

    data = (
        "a, b, c\n"
        "12, 232, high\n"
        "43, 334, medium\n"
    ).encode()

    cmd = [
        "import-csv", "--timestamp", "now",
        "--identifier", "Node=001",
        #"--storage-type", "notification",
        "--data-source", data_source.name
    ]

    p = Popen(cmd, stdout=PIPE, stdin=PIPE, stderr=PIPE)

    stdout_data, stderr_data = p.communicate(input=data)

    eq_('', stderr_data)

    eq_('', stdout_data)
