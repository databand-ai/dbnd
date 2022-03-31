import logging
import re

from itertools import takewhile
from typing import Optional

from psycopg2.extensions import connection as psycopg2_connection
from psycopg2.extras import DictCursor

from dbnd._core.log.external_exception_logging import log_exception_to_server
from dbnd._core.sql_tracker_common.sql_operation import DTypes


logger = logging.getLogger(__name__)


COPY_ROWS_COUNT_QUERY = "select pg_last_copy_count();"

TEMP_TABLE_NAME = "DBND_TEMP"


def redshift_query(
    connection: psycopg2_connection, query: str, params=None, fetch_all=True
):
    try:
        with connection.cursor(cursor_factory=DictCursor) as cursor:
            cursor.execute(query, params)
            if fetch_all:
                return cursor.fetchall()

    except Exception as e:
        logger.exception("Error occurred during querying redshift, query: %s", query)
        log_exception_to_server(e)


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


def get_redshift_table_schema(connection, table) -> Optional[DTypes]:
    desc_results = redshift_query(
        connection,
        f"select * from pg_table_def where tablename='{table.lower().split('.')[-1]}'",
    )

    schema = {}
    if desc_results:
        for col_desc in desc_results:
            if len(col_desc) > 2:
                # extract host to connection
                connection.schema = col_desc[0]
                normalized_col_type = "".join(
                    takewhile(lambda c: c != "(", col_desc[3])
                )
                normalized_col_name = col_desc[2].lower()
                schema[normalized_col_name] = normalized_col_type
    return schema


def copy_to_temp_table(redshift_connection, target_name, query):
    if target_name is not None:
        table_name = target_name.lower()
        redshift_query(
            redshift_connection,
            f"CREATE TEMP TABLE {TEMP_TABLE_NAME} (LIKE {table_name});",
            fetch_all=False,
        )
        copy_tmp_table_query = re.sub(
            re.escape(table_name), TEMP_TABLE_NAME, query, flags=re.IGNORECASE
        )
        redshift_query(redshift_connection, copy_tmp_table_query, fetch_all=False)
    else:
        logger.error("Couldn't extract table name")
