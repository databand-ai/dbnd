import logging

from decimal import Decimal
from textwrap import dedent
from typing import Optional

from dbnd import log_duration, log_metrics
from dbnd_snowflake.snowflake_values import SnowflakeController


logger = logging.getLogger(__name__)

# TODO: Add support for QUERY_TAG
# I.e. Subclass SnowflakeOperator and set session param QUERY_TAG to "dbnd.{dag/task_name/task_id}"
# Then use pass this QUERY_TAG to UI for easier navigation between
# See https://community.snowflake.com/s/article/How-We-Controlled-and-Reduced-Snowflake-Compute-Cost
# https://github.com/snowflakedb/snowflake-connector-python/issues/203


def log_snowflake_resource_usage(
    query_text,  # type: str
    database,  # type: str
    user,  # type: str
    connection_string,  # type: str
    session_id=None,  # type: Optional[str]
    key="snowflake_query",  # type: str
):
    """
    get and log cpu time, run time, disk read, and processed rows.
    connection or connection_string is required. supports only psycopg2 connections.
    """
    try:
        with log_duration("log_snowflake_resource_usage__time_seconds", "system"):
            _log_snowflake_resource_usage(
                query_text, database, user, connection_string, session_id, key
            )
    except Exception as exc:
        conn_without_pass = _censor_password(connection_string)
        logger.exception(
            "Failed to log_snowflake_resource_usage (query_text=%s, connection_string=%s)",
            query_text,
            conn_without_pass,
        )


def _log_snowflake_resource_usage(
    query_text, database, user, connection_string, session_id=None, key=None,
):
    # Quick and dirty way to handle optional clause element.
    # Might be better to use SQLAlchemy expression language here
    if session_id:
        query_history = dedent(
            """\
            select *
            from table({}.information_schema.query_history(dateadd('minutes',-15,current_timestamp()),current_timestamp()))
            where LOWER(query_text)=LOWER(%s) and LOWER(user_name)=LOWER(%s) and session_id=%s
            order by start_time desc limit 1;"""
        ).format(database, session_id)
        query_params = (query_text, user, session_id)
    else:
        query_history = dedent(
            """\
            select *
            from table({}.information_schema.query_history(dateadd('minutes',-15,current_timestamp()),current_timestamp()))
            where LOWER(query_text)=LOWER(%s) and LOWER(user_name)=LOWER(%s)
            order by start_time desc limit 1;"""
        ).format(database)
        query_params = (query_text, user)

    result = _connect_and_query(connection_string, query_history, *query_params)
    if not result:
        logger.info(
            "resource metrics were not found for query '%s', query_params=%s",
            query_text,
            query_params,
        )
        log_metrics(
            {
                "snowflake_query_warning": "No resources info found",
                "snowflake_query_text": query_text,
            },
            source="user",
        )
        return

    metrics = result[0]
    key = key or "snowflake_query"
    snowflake_metric_to_ui_name = {
        "BYTES_SCANNED": "bytes_scanned",
        "COMPILATION_TIME": "compilation_time_milliseconds",
        "CREDITS_USED_CLOUD_SERVICES": "credits_used_cloud_services",
        "EXECUTION_TIME": "execution_time_milliseconds",
        "QUERY_ID": "query_id",
        "QUERY_TEXT": "query_text",
        "ROWS_PRODUCED": "rows_produced",
        "TOTAL_ELAPSED_TIME": "total_elapsed_time_milliseconds",
    }

    metrics_to_log = {}
    for metric, ui_name in snowflake_metric_to_ui_name.items():
        if metric in metrics:
            value = metrics[metric]
            # Quick hack to track decimal values. probably should be handled on a serialization level
            if isinstance(value, Decimal):
                value = float(value)
            metrics_to_log[key + "." + ui_name] = value
    log_metrics(metrics_to_log, source="user")


def _connect_and_query(connection_string, query, *params):
    """ connect if needed, then query. """
    # if (connection is None) and (connection_string is None):
    if connection_string is None:
        logger.error(
            "connection and connection string are None, one of them is required to query redshift"
        )
        return

    with SnowflakeController(connection_string) as snowflake:
        return snowflake._query(query, params)


def _censor_password(connection_string):
    """
    example connection string:
        postgres://user:password@host.com:5439/dev
    returns:
        postgres://user:*****@host.com:5439/dev
    """
    if (not connection_string) or ("@" not in connection_string):
        return connection_string

    split1 = connection_string.split("@")
    split2 = split1[0].split(":")

    if len(split2) != 3:
        return connection_string

    split2[-1] = "*****"
    split2_join = ":".join(split2)
    split1[0] = split2_join
    split1_join = "@".join(split1)
    return split1_join
