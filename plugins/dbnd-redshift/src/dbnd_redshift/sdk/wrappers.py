from typing import Optional

from psycopg2.extensions import (
    connection as psycopg2_connection,
    cursor as psycopg2_cursor,
)


class DbndCursorWrapper:
    def __init__(self, original_cursor: psycopg2_cursor):
        self.original_cursor = original_cursor

    def execute(self, query, vars=None):
        return self.original_cursor.execute(query, vars)

    @property
    def connection(self):
        return self.original_cursor.connection

    # Delegate attribute lookup to internal obj
    def __getattr__(self, name):
        return getattr(self.original_cursor, name)

    # psycopg2 inspects the type of object, sqla_unwrap used to get the underlying object and pass it to psycopg.
    # source: https://gist.github.com/mjallday/3d4c92e7e6805af1e024
    @property
    def _sqla_unwrap(self):
        return self.original_cursor


class DbndConnectionWrapper:
    def __init__(self, original_connection: psycopg2_connection):
        self.connection = original_connection

    def cursor(self):
        return DbndCursorWrapper(self.connection.cursor())

    def close(self):
        return self.connection.close()

    def __enter__(self):
        return self

    def __exit__(self, exc_type, exc_val, exc_tb):
        return self.connection.__exit__(exc_type, exc_val, exc_tb)

    # Delegate attribute lookup to internal obj
    def __getattr__(self, name):
        return getattr(self.connection, name)

    @property
    def _sqla_unwrap(self):
        return self.connection


class PostgresConnectionWrapper:
    def __init__(self, connection: psycopg2_connection):
        self.connection = connection
        self._schema = None

    @property
    def host(self) -> str:
        return self.connection.info.host

    @property
    def port(self) -> Optional[int]:
        return self.connection.info.port

    @property
    def database(self) -> Optional[str]:
        return self.connection.info.dbname

    @property
    def schema(self) -> Optional[str]:
        return self._schema

    @schema.setter
    def schema(self, schema):
        self._schema = schema

    def cursor(self):
        return self.connection.cursor()
