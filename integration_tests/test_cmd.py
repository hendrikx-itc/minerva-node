import os
from subprocess import Popen, PIPE

from nose.tools import eq_

TEST_CONFIG = """\
db_uri = {}
profile_directory = /etc/minerva/csv-importer/profiles/
log_level = INFO
log_filename = import-csv.log
log_directory = /tmp/
log_rotation_size = 10MB
""".format(os.getenv("TEST_DB_URL"))


def test_cmd_1():
    file_path = "/tmp/test.csv"
    datasource_name = "test-src"
    config_file_path = "/tmp/import-csv.conf"

    with open(config_file_path, "wt") as config_file:
        config_file.write(TEST_CONFIG)

    with open(file_path, "wt") as csv_file:
        csv_file.write(
            "type, name, a, b, c\n"
            "Node, 001, 12, 232, high\n"
            "Node, 002, 43, 334, medium\n")

    cmd = ["import-csv", "-c", config_file_path, "--timestamp", "now", "--identifier", "Node={name}", datasource_name, file_path]
    p = Popen(cmd, stdout=PIPE, stdin=PIPE, stderr=PIPE)

    stdout_data, stderr_data = p.communicate(input=None)

    print(stderr_data)

    #eq_('', stderr_data)

    eq_('', stdout_data)


def test_cmd_2():
    file_path = "/tmp/test.csv"
    datasource_name = "test-src"
    config_file_path = "/tmp/import-csv.conf"

    with open(config_file_path, "wt") as config_file:
        config_file.write(TEST_CONFIG)

    with open(file_path, "wt") as csv_file:
        csv_file.write(
            "type, name, a, b, c\n"
            "Node, 001, 12, 232, high\n"
            "Node, 002, 43, 334, medium\n")

    cmd = ["import-csv", "-c", config_file_path, "--timestamp", "now", "--identifier", "Node={name}", datasource_name, file_path]
    p = Popen(cmd, stdout=PIPE, stdin=PIPE, stderr=PIPE)

    stdout_data, stderr_data = p.communicate(input=None)

    print(stderr_data)

    #eq_('', stderr_data)

    eq_('', stdout_data)


def test_cmd_identifier_regex():
    file_path = "/tmp/test.csv"
    datasource_name = "test-src"
    config_file_path = "/tmp/import-csv.conf"

    with open(config_file_path, "wt") as config_file:
        config_file.write(TEST_CONFIG)

    with open(file_path, "wt") as csv_file:
        csv_file.write(
            "type, name, a, b, c\n"
            "Node, node0001, 12, 232, high\n"
            "Node, node0102, 43, 334, medium\n")

    cmd = ["import-csv", "-c", config_file_path, "--timestamp", "now", "--identifier", "Node={name}", "--identifier-regex", "(Node=)node[0]+(\\d+)", datasource_name, file_path]
    p = Popen(cmd, stdout=PIPE, stdin=PIPE, stderr=PIPE)

    stdout_data, stderr_data = p.communicate(input=None)

    print(stderr_data)

    #eq_('', stderr_data)

    eq_('', stdout_data)
