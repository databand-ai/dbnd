import logging

import psycopg2

from psycopg2.extras import RealDictCursor

from dbnd import log_metric


logger = logging.getLogger(__name__)


def log_redshift_resource_usage(query_id, connection=None, connection_string=None):
    """
    get and log cpu time, run time, disk read, and processed rows.
    connection or connection_string is required. supports only psycopg2 connections.
    """
    try:
        _log_redshift_resource_usage(query_id, connection, connection_string)
    except Exception:
        conn_without_pass = _censor_password(connection_string)
        logger.exception(
            "Failed to log_redshift_resource_usage (query_id=%s, connection=%s, connection_string=%s)",
            query_id,
            connection,
            conn_without_pass,
        )


def _log_redshift_resource_usage(query_id, connection, connection_string):
    query = "select * from STL_QUERY_METRICS where segment=-1 and query=%s"
    result = _connect_and_query(connection, connection_string, query, query_id)
    if result is None:
        logger.info("resource metrics were not found for query %s", query_id)
        return

    key = "redshift_query_" + str(query_id)
    redshift_metric_to_ui_name = dict(
        cpu_time="cpu_time_microseconds",
        run_time="run_time_microseconds",
        rows="processed_rows",
        blocks_read="disk_read_mb",
    )

    for metric, ui_name in redshift_metric_to_ui_name.items():
        if result[metric] != -1:
            log_metric(key + "." + ui_name, result[metric])


def get_redshift_query_id(statement, connection=None, connection_string=None):
    """
    get last completed query id with given query statement.
    connection or connection_string is required. supports only psycopg2 connections.
    """
    statement = statement[:60]
    query = "select * from SVL_QLOG where substring=%s order by endtime desc limit 1"
    result = _connect_and_query(connection, connection_string, query, statement)
    return result["query"] if (result is not None) else None


def _connect_and_query(connection, connection_string, query, *args):
    """ connect if needed, then query. """
    if (connection is None) and (connection_string is None):
        logger.error(
            "connection and connection string are None, one of them is required to query redshift"
        )
        return

    if connection:
        return _query(connection, query, *args)

    with psycopg2.connect(connection_string) as connection:
        return _query(connection, query, *args)


def _query(connection, query, *args):
    cursor = connection.cursor(cursor_factory=RealDictCursor)
    cursor.execute(query, args)
    result = cursor.fetchone()
    return result


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
