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

SNOWFLAKE_METRIC_TO_UI_NAME = {
    "BYTES_SCANNED": "bytes_scanned",
    "COMPILATION_TIME": "compilation_time_milliseconds",
    "CREDITS_USED_CLOUD_SERVICES": "credits_used_cloud_services",
    "EXECUTION_TIME": "execution_time_milliseconds",
    "QUERY_ID": "query_id",
    "QUERY_TEXT": "query_text",
    "ROWS_PRODUCED": "rows_produced",
    "TOTAL_ELAPSED_TIME": "total_elapsed_time_milliseconds",
}
RESOURCE_METRICS = ",".join(
    '"{}"'.format(m) for m in SNOWFLAKE_METRIC_TO_UI_NAME.keys()
)


def log_snowflake_resource_usage(
    query_text,  # type: str
    database,  # type: str
    user,  # type: str
    connection_string,  # type: str
    session_id=None,  # type: Optional[str]
    key="snowflake_query",  # type: str
    history_window=15,  # type: float
):
    """
    get and log cpu time, run time, disk read, and processed rows.
    connection or connection_string is required. supports only psycopg2 connections.
    """
    try:
        with log_duration("log_snowflake_resource_usage__time_seconds", "system"):
            _log_snowflake_resource_usage(
                query_text,
                database,
                user,
                connection_string,
                session_id,
                key,
                history_window,
            )
    except Exception as exc:
        conn_without_pass = _censor_password(connection_string)
        logger.exception(
            "Failed to log_snowflake_resource_usage (query_text=%s, connection_string=%s)",
            query_text,
            conn_without_pass,
        )


def _log_snowflake_resource_usage(
    query_text,  # type: str
    database,  # type: str
    user,  # type: str
    connection_string,  # type: str
    session_id=None,  # type: Optional[int]
    key=None,  # type: Optional[str]
    history_window=15,  # type: float
):  # type: (...) -> None
    # Quick and dirty way to handle optional clause element.
    # Might be better to use SQLAlchemy expression language here
    if session_id:
        query_history = dedent(
            """\
            select {metrics}
            from table({database}.information_schema.query_history_by_session(
                SESSION_ID => {session_id},
                END_TIME_RANGE_START => dateadd('minutes', -{minutes}, current_timestamp()),
                END_TIME_RANGE_END => current_timestamp()
            ))
            where LOWER(user_name)=LOWER(%s) and LOWER(REGEXP_REPLACE(TRIM(query_text), ' |\t|\n'))=LOWER(REGEXP_REPLACE(TRIM(%s), ' |\t|\n'))
            order by start_time desc limit 1;"""
        ).format(
            metrics=RESOURCE_METRICS,
            database=database,
            minutes=history_window,
            session_id=session_id,
        )
        query_params = (user, query_text)
    else:
        query_history = dedent(
            """\
            select {metrics}
            from table({database}.information_schema.query_history_by_user(
                USER_NAME => %s,
                END_TIME_RANGE_START => dateadd('minutes', -{minutes}, current_timestamp()),
                END_TIME_RANGE_END => current_timestamp())
            )
            where LOWER(REGEXP_REPLACE(TRIM(query_text), ' |\t|\n'))=LOWER(REGEXP_REPLACE(TRIM(%s), ' |\t|\n'))
            order by start_time desc limit 1;"""
        ).format(metrics=RESOURCE_METRICS, database=database, minutes=history_window)
        query_params = (user, query_text)

    result = _connect_and_query(connection_string, query_history, *query_params)
    if not result:
        logger.error(
            "Resource metrics were not found for query '%s', query_params=%s",
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

    metrics_to_log = {}
    for metric, ui_name in SNOWFLAKE_METRIC_TO_UI_NAME.items():
        if metric in metrics:
            value = metrics[metric]
            # Quick hack to track decimal values. probably should be handled on a serialization level
            if isinstance(value, Decimal):
                value = float(value)
            metrics_to_log[key + "." + ui_name] = value
    log_metrics(metrics_to_log, source="user")


def _connect_and_query(connection_string, query, *params):
    """ connect if needed, then query. """
    if not connection_string:
        logger.error("Connection string is empty, don't know where to connect")
        return

    with SnowflakeController(connection_string) as snowflake:
        return snowflake._query(query, params)


def _censor_password(connection_string):
    """
    example connection string:
        snowflake://user:password@account.europe-west4.gcp/database
    returns:
        snowflake://user:*****@account.europe-west4.gcp/database
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
