# © Copyright Databand.ai, an IBM Company 2022

import logging
import re
import sys
import traceback

from psycopg2.extensions import connection as psycopg2_connection
from psycopg2.extras import DictCursor

from dbnd._core.log.external_exception_logging import log_exception_to_server


logger = logging.getLogger(__name__)


COPY_ROWS_COUNT_QUERY = "select pg_last_copy_count();"

TEMP_TABLE_NAME = "DBND_TEMP"


def redshift_query(
    connection: psycopg2_connection, query: str, params=None, fetch_all=True
):
    if connection is not None and not connection.closed:
        with connection.cursor(cursor_factory=DictCursor) as cursor:
            try:
                cursor.execute(query, params)
                if fetch_all:
                    return cursor.fetchall()

            except Exception as e:
                cursor.execute("ROLLBACK")
                traceback.print_exc(file=sys.stdout)
                logger.exception(
                    f"Error occurred during querying redshift, query: {query}"
                )
                log_exception_to_server(e)
    else:
        logger.exception("Error with redshift connection")


def get_last_query_records_count(connection: psycopg2_connection):
    """
    Returns the number of rows that were loaded by the last COPY command run in the current session.
    """
    result_set = redshift_query(connection, COPY_ROWS_COUNT_QUERY)
    if result_set and len(result_set) == 1:
        dimensions = result_set[0]
        if dimensions[0]:
            return dimensions[0]


def build_schema_from_dataframe(dataframe):
    if dataframe is not None:
        try:
            df_schema = dataframe.dtypes.to_dict()
            df_schema = dict((k, str(v)) for k, v in df_schema.items())
        except Exception as e:
            df_schema = None
            logger.exception(
                "Error occurred during build schema from dataframe: %s", dataframe
            )
            log_exception_to_server(e)
    else:
        df_schema = None
    return df_schema


def copy_to_temp_table(redshift_connection, schema_name, table_name, query):
    full_table_name = (
        f"{schema_name}.{table_name}"
        if schema_name is not None and schema_name != "public"
        else table_name
    )
    if table_name is not None:
        redshift_query(
            redshift_connection,
            f"CREATE TEMP TABLE {TEMP_TABLE_NAME} (LIKE {full_table_name});",
            fetch_all=False,
        )
        copy_tmp_table_query = re.sub(
            re.escape(full_table_name),
            TEMP_TABLE_NAME,
            query,
            flags=re.IGNORECASE,
            count=1,
        )
        redshift_query(redshift_connection, copy_tmp_table_query, fetch_all=False)
        redshift_query(redshift_connection, "COMMIT", fetch_all=False)
    else:
        logger.error("Couldn't extract table name")
