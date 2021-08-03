# This code was taken from SQLAlchemy-Utils version 0.36.1
# License is BSD

import os

from copy import copy

import sqlalchemy as sa

from sqlalchemy.engine.url import make_url
from sqlalchemy.exc import OperationalError, ProgrammingError


def database_exists(url):
    """Check if a database exists.

    :param url: A SQLAlchemy engine URL.

    Performs backend-specific testing to quickly determine if a database
    exists on the server. ::

        database_exists('postgresql://postgres@localhost/name')  #=> False
        create_database('postgresql://postgres@localhost/name')
        database_exists('postgresql://postgres@localhost/name')  #=> True

    Supports checking against a constructed URL as well. ::

        engine = create_engine('postgresql://postgres@localhost/name')
        database_exists(engine.url)  #=> False
        create_database(engine.url)
        database_exists(engine.url)  #=> True

    """

    def get_scalar_result(engine, sql):
        result_proxy = engine.execute(sql)
        result = result_proxy.scalar()
        result_proxy.close()
        engine.dispose()
        return result

    def sqlite_file_exists(database):
        if not os.path.isfile(database) or os.path.getsize(database) < 100:
            return False

        with open(database, "rb") as f:
            header = f.read(100)

        return header[:16] == b"SQLite format 3\x00"

    url = copy(make_url(url))
    database = url.database
    if url.drivername.startswith("postgres"):
        url.database = "postgres"
    else:
        url.database = None

    engine = sa.create_engine(url)

    if engine.dialect.name == "postgresql":
        text = "SELECT 1 FROM pg_database WHERE datname='%s'" % database
        return bool(get_scalar_result(engine, text))

    elif engine.dialect.name == "mysql":
        text = (
            "SELECT SCHEMA_NAME FROM INFORMATION_SCHEMA.SCHEMATA "
            "WHERE SCHEMA_NAME = '%s'" % database
        )
        return bool(get_scalar_result(engine, text))

    elif engine.dialect.name == "sqlite":
        if database:
            return database == ":memory:" or sqlite_file_exists(database)
        else:
            # The default SQLAlchemy database is in memory,
            # and :memory is not required, thus we should support that use-case
            return True

    else:
        engine.dispose()
        engine = None
        text = "SELECT 1"
        try:
            url.database = database
            engine = sa.create_engine(url)
            result = engine.execute(text)
            result.close()
            return True

        except (ProgrammingError, OperationalError):
            return False
        finally:
            if engine is not None:
                engine.dispose()
