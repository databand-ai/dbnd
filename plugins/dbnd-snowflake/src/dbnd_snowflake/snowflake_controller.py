import logging

from datetime import timedelta
from decimal import Decimal
from textwrap import dedent
from time import sleep
from typing import Dict, Optional, Tuple, Union

from six.moves.urllib_parse import urlparse
from snowflake.connector import DictCursor, SnowflakeConnection

from dbnd import __version__
from dbnd._core.errors import DatabandConfigError, DatabandRuntimeError
from dbnd._core.utils.timezone import utcnow
from dbnd._vendor.tabulate import tabulate
from dbnd_snowflake.snowflake_config import SnowflakeConfig
from dbnd_snowflake.snowflake_values import SnowflakeTable


logger = logging.getLogger(__name__)


class SnowflakeError(DatabandRuntimeError):
    pass


def conn_str_to_conn_params(conn_str):
    # type: (str) -> dict
    # TODO: Drop in favor of sqlalchemy.engine.url
    conn = urlparse(conn_str)
    if conn.scheme != "snowflake":
        raise DatabandConfigError(
            "Unsupported connection string scheme '{}'. snowflake is required".format(
                conn.scheme
            )
        )

    return {
        "account": conn.hostname,
        "user": conn.username,
        "password": conn.password,
    }


SNOWFLAKE_METRIC_TO_UI_NAME = {
    "BYTES_SCANNED": "bytes_scanned",
    "COMPILATION_TIME": "compilation_time_milliseconds",
    "CREDITS_USED_CLOUD_SERVICES": "credits_used_cloud_services",
    "ERROR_MESSAGE": "error_message",
    "EXECUTION_STATUS": "execution_status",
    "EXECUTION_TIME": "execution_time_milliseconds",
    "QUERY_ID": "query_id",
    "QUERY_TAG": "query_tag",
    "QUERY_TEXT": "query_text",
    "ROWS_PRODUCED": "rows_produced",
    "SESSION_ID": "session_id",
    "TOTAL_ELAPSED_TIME": "total_elapsed_time_milliseconds",
}
RESOURCE_METRICS = ",".join(
    '"{}"'.format(m) for m in SNOWFLAKE_METRIC_TO_UI_NAME.keys()
)
RESULT_LIMIT_INC = 10


class SnowflakeController:
    """ Interacts with Snowflake, queries it"""

    def __init__(self, connection_or_connection_string):
        # type: (Union[str, SnowflakeConnection]) -> SnowflakeController
        if isinstance(connection_or_connection_string, SnowflakeConnection):
            self._connection = connection_or_connection_string

            self.account = self._connection.account
            self.user = self._connection.user
            self.password = "***"
        else:
            conn_params = conn_str_to_conn_params(connection_or_connection_string)

            self.account = conn_params["account"]
            self.user = conn_params["user"]
            self.password = conn_params["password"]

            self._connection = None

        self._should_close = False
        self._query_tag = "dbnd-snowflake"
        self._column_types = None

    def __enter__(self):
        if self._connection is None:
            self._connection = SnowflakeConnection(
                user=self.user,
                password=self.password,
                account=self.account,
                session_parameters={"QUERY_TAG": self._query_tag},
                application="DBND Snowflake plugin {}".format(__version__),
            )
            # if and only if we opened connection - we should close it
            self._should_close = True

        return self

    def __exit__(self, exc_type, exc_val, exc_tb):
        if self._should_close and self._connection is not None:
            self._connection.close()

    def to_preview(self, table):
        # type: (SnowflakeTable) -> str
        # snowflake.connector does not handle incorrect utf-8 data fetched from db,
        # hence this fillding with encode/decode
        column_types = self.get_column_types(table)
        columns = ",".join(
            'TRY_HEX_DECODE_STRING(HEX_ENCODE("{0}")) AS {0}'.format(column)
            for column in column_types.keys()
        )

        rows = self.query(
            "select {0} from {1.db_with_schema}.{1.table_name} limit {1.preview_rows}".format(
                columns, table
            )
        )
        preview_table = tabulate(rows, headers="keys") + "\n..."
        return preview_table

    def get_dimensions(self, table):
        # type: (SnowflakeTable) -> Dict
        if table.schema:
            in_cond = "in schema {0.db_with_schema}".format(table)
        else:
            in_cond = "in database {0.database}".format(table)
        table_meta = self.query(
            "SHOW TABLES LIKE '{0}' {1}".format(table.table_name, in_cond)
        )
        if len(table_meta) != 1:
            raise SnowflakeError(
                "Snowflake table not found: '{0.table_name}', DB: '{0.database}'".format(
                    table
                )
            )

        cols = len(self.get_column_types(table))
        return {
            "rows": table_meta[0]["rows"],
            "cols": cols,
            "bytes": table_meta[0]["bytes"],
        }

    def get_column_types(self, table):
        # type: (SnowflakeTable) -> Dict[str, str]
        if self._column_types is not None:
            return self._column_types

        query = dedent(
            """\
            SELECT column_name, data_type
            FROM {0.database}.information_schema.columns
            WHERE LOWER(table_name) = LOWER('{0.table_name}')"""
        ).format(table)
        if table.schema:
            query += " and LOWER(table_schema) = LOWER('{0.schema}')".format(table)
        results = self.query(query)
        if not results:
            raise SnowflakeError(
                "Table columns not found. Snowflake DB: '{0.database}', "
                "schema: {0.schema} table: '{0.table_name}'\n"
                "Query used: {1}".format(table, query)
            )

        self._column_types = {row["COLUMN_NAME"]: row["DATA_TYPE"] for row in results}
        return self._column_types

    def query(self, query, params=None):
        try:
            with self._connection.cursor(DictCursor) as cursor:
                cursor.execute(query, params)
                result = cursor.fetchall()
                return result
        except Exception:
            logger.exception(
                "Error occurred during querying Snowflake, query: %s", query
            )
            raise

    def __str__(self):
        return "snowflake://{user}:***@{account}".format(
            account=self.account, user=self.user
        )

    def get_resource_usage(
        self,
        database: str,
        query_id: str,
        session_id: Optional[int],
        key: str,
        history_window: float,
        query_history_result_limit: int,
        delay: int,
        retries: int,
        retry_pause: float,
        raise_on_error: bool,
        config: SnowflakeConfig,
    ) -> Dict:
        if delay > 0:
            logger.info("Delaying search in QUERY_HISTORY for %s seconds", delay)
            sleep(delay)
        result_limit = min(
            query_history_result_limit, config.query_history_result_limit_max_value
        )
        tries, sf_query = 0, ""
        try:
            while (
                result_limit <= config.query_history_result_limit_max_value
                and tries <= retries
            ):
                resource_metrics, sf_query = self._query_resource_usage(
                    database,
                    query_id=query_id,
                    session_id=session_id,
                    key=key,
                    history_window=history_window,
                    query_history_result_limit=result_limit,
                    config=config,
                )
                if resource_metrics:
                    return resource_metrics
                logger.warning(
                    "Metadata not found for session_id '{}', query_id '{}'\n"
                    "Query used to search for resource usage: '{}'".format(
                        session_id, query_id, sf_query
                    )
                )
                result_limit = min(
                    result_limit * RESULT_LIMIT_INC,
                    config.query_history_result_limit_max_value,
                )
                logger.info(
                    "Extending QUERY_HISTORY() search window: RESULT_LIMIT={}".format(
                        result_limit
                    )
                )
                tries += 1
                if retry_pause and retry_pause > 0:
                    logger.info("Sleeping for %s seconds", retry_pause)
                    sleep(retry_pause)
            else:
                logger.info(
                    "No more retries left to fetch Snoflake query resources. Giving up."
                )

        except Exception as exc:
            logger.exception(
                "Failed to log_snowflake_resource_usage (query_text=%s, connection_string=%s)\n"
                "Last query params used to search for resource usage: query_id - '%s', "
                "sesion_id = '%s', database - '%s', connection - '%s', query - '%s'",
                query_id,
                session_id,
                database,
                self,
                sf_query,
            )
            if raise_on_error:
                raise

        logger.error(
            "Resource metrics were not found for query_id '%s'.\n Query used: %s",
            query_id,
            sf_query,
        )
        return {
            f"{key}.warning": "No resources info found",
            # converting to str, since can be too large for DB int
            f"{key}.session_id": str(session_id),
            f"{key}.query_id": query_id,
        }

    def _query_resource_usage(
        self,
        database,  # type: str
        query_id,  # type: str
        session_id,  # type: Optional[int]
        key,  # type: Optional[str]
        history_window,  # type: float
        query_history_result_limit,  # type: int
        config,  # type: SnowflakeConfig
    ):  # type: (...) -> Tuple[Dict, str]
        key = key or "snowflake_query"
        query_history = self._build_resource_usage_query(
            database,
            query_id=query_id,
            session_id=session_id,
            history_window=history_window,
            query_history_result_limit=query_history_result_limit,
            config=config,
        )

        result = self.query(query_history)
        if not result:
            return {}, query_history

        metrics = result[0]

        metrics_to_log = {}
        for metric, ui_name in SNOWFLAKE_METRIC_TO_UI_NAME.items():
            if metric in metrics:
                value = metrics[metric]
                # Quick hack to track decimal values. probably should be handled on a serialization level
                if isinstance(value, Decimal):
                    value = float(value)
                metrics_to_log[key + "." + ui_name] = value
        return metrics_to_log, query_history

    def _build_resource_usage_query(
        self,
        database: str,
        query_id: str,
        session_id: int,
        history_window: float,
        query_history_result_limit: int,
        config: SnowflakeConfig,
    ) -> str:
        time_end = utcnow() - timedelta(minutes=config.query_history_end_time_range_end)
        time_start = time_end - timedelta(
            minutes=history_window or config.query_history_end_time_range_start
        )
        if session_id:
            query_history = dedent(
                """\
                select {metrics}
                from table({database}.information_schema.query_history_by_session(
                    SESSION_ID => {session_id},
                    END_TIME_RANGE_START => '{time_start}'::timestamp_ltz,
                    END_TIME_RANGE_END => '{time_end}'::timestamp_ltz,
                    RESULT_LIMIT => {result_limit}
                ))
                where query_id='{query_id}'
                order by start_time desc limit 1;"""
            ).format(
                metrics=RESOURCE_METRICS,
                database=database,
                minutes=history_window,
                session_id=session_id,
                result_limit=query_history_result_limit,
                time_start=time_start,
                time_end=time_end,
                query_id=query_id,
            )
            return query_history

        else:
            query_history = dedent(
                """\
                select {metrics}
                from table({database}.information_schema.query_history(
                    END_TIME_RANGE_START => '{time_start}'::timestamp_ltz,
                    END_TIME_RANGE_END => '{time_end}'::timestamp_ltz,
                    RESULT_LIMIT => {result_limit}
                ))
                where query_id='{query_id}'
                order by start_time desc limit 1;"""
            ).format(
                metrics=RESOURCE_METRICS,
                database=database,
                minutes=history_window,
                result_limit=query_history_result_limit,
                time_start=time_start,
                time_end=time_end,
                query_id=query_id,
            )
            return query_history
