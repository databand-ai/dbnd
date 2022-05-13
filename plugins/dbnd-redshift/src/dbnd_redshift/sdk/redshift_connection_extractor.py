import re

import attr

from psycopg2.extensions import connection


DEFAULT_REDSHIFT_SCHEMA = "public"


@attr.s
class RedshiftDataset:
    host: str = attr.ib()
    database: str = attr.ib()
    schema: str = attr.ib()
    table: str = attr.ib()


def get_redshift_dataset(conn: connection, query: str) -> RedshiftDataset:
    table, schema = get_target_table_and_schema(conn, query)
    host = get_connection_parameter(conn, "host")
    database = get_connection_parameter(conn, "dbname")
    return RedshiftDataset(host=host, database=database, schema=schema, table=table)


def get_target_table_and_schema(con: connection, query):
    table, schema = _extract_table_and_schema_from_query(query)
    # Schema can also be extracted from search_path provided as a connection option or
    # using `SHOW search_path` query. It can contain more than one schema,
    # hence requires more complex logic.
    if not schema:
        schema = DEFAULT_REDSHIFT_SCHEMA
    return table.lower(), schema.lower()


def _extract_table_and_schema_from_query(query):
    table_and_schema_regex = r"""COPY ["']?((?P<schema>\w+)\.)?(?P<table>\w+)["']?"""
    match = re.search(table_and_schema_regex, query, flags=re.IGNORECASE)
    table = match.group("table")
    schema = match.group("schema")
    return table, schema


def get_connection_parameter(con: connection, parameter: str) -> str:
    return con.info.dsn_parameters[parameter]
