import os
import logging
from contextlib import closing
import psycopg2.extras

from minerva.db import parse_db_url

QUERY_SEP = "\n"


def connect():
    db_url = os.getenv("TEST_DB_URL")

    if db_url is None:
        raise Exception("Environment variable TEST_DB_URL not set")

    scheme, user, password, host, port, database = parse_db_url(db_url)

    if scheme != "postgresql":
        raise Exception("Only PostgreSQL connections are supported")

    conn = psycopg2.connect(database=database, user=user, password=password,
         host=host, port=port, connection_factory=psycopg2.extras.LoggingConnection)

    logging.info("connected to database {0}/{1}".format(host, database))

    conn.initialize(logging.getLogger(""))

    return conn

