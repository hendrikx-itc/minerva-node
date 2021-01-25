import logging
from operator import not_

import psycopg2

from minerva.util import compose, retry_while


class ConnectionProvider:
    def __init__(self, connect_fn, stop_event):
        self._connection = None
        self.connect_fn = connect_fn
        self.stop_event = stop_event

    def reset(self):
        self._connection = None

    def _connect(self):
        handler_map = {
            psycopg2.OperationalError: lambda exc: logging.error(
                "could not connect to database ({}), waiting".format(exc)
            )
        }

        retry_condition = compose(not_, self.stop_event.is_set)

        return retry_while(
            self.connect_fn, handler_map, retry_condition
        )

    def get_connection(self):
        if self._connection is not None:
            return self._connection
        else:
            self._connection = self._connect()

            return self._connection
