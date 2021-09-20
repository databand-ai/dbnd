import functools
import logging

from contextlib import contextmanager

import attr

from dbnd._core.settings import TrackingConfig
from dbnd._core.settings.tracking_config import ValueTrackingLevel
from dbnd._core.tracking.metrics import log_dataset_op
from dbnd_airflow.tracking.airflow_connections import get_conn_path
from dbnd_airflow.tracking.config import AirflowTrackingConfig
from dbnd_snowflake.extract_sql_query import extract_from_sql
from dbnd_snowflake.snowflake_table import log_snowflake_table_targets


logger = logging.getLogger(__name__)


@contextmanager
def log_sql_target_wrapper(conn_str, conn_path, sql_query):
    # type: (str, str, str) -> None
    """
    Context manager to handle the tracking of sql query execution.

    @param conn_str:
    @param conn_path: connection string format as path. use to format a the target's path
    @param sql_query: a sql query to extract the table operations from
    """
    try:
        yield
    except Exception:
        log_sql_targets(conn_str, conn_path, sql_query, False)
        raise
    else:
        log_sql_targets(conn_str, conn_path, sql_query, True)


def log_sql_targets(conn_str, path, sql_query, is_succeed):
    try:
        for target_op in extract_from_sql(path, sql_query):
            target_op = attr.evolve(target_op, success=is_succeed)

            if (
                target_op.path.startswith("snowflake")
                and TrackingConfig.current().value_reporting_strategy
                == ValueTrackingLevel.ALL
                and not target_op.name.startswith("@")
            ):
                # 1) snowflake tables are lazy evaluated types
                # we support they been logs only if the the strategy is All
                # 2) staging is not supported yet
                log_snowflake_table_targets(
                    table_op=target_op, connection_string=conn_str
                )

            else:
                # the target can actually be a non table like S3 file that used
                # as part of the sql query
                log_dataset_op(
                    op_path=target_op.path,
                    op_type=target_op.operation,
                    success=target_op.success,
                )
    except Exception:
        logger.exception("Caught exception will trying to log target for sql_query")


def patch_airflow_db_hook(operator_cls, wrapper):
    def wrap_sql_executor(name):
        original_run = getattr(operator_cls, name)
        setattr(operator_cls, name, wrapper(original_run))

    from airflow.hooks.dbapi_hook import DbApiHook

    if issubclass(operator_cls, DbApiHook):
        wrap_sql_executor("run")  # :run(self, sql, autocommit=False, parameters=None)
        wrap_sql_executor("get_records")  # :get_records(self, sql, parameters=None)
        wrap_sql_executor("get_pandas_df")  # :get_pandas_df(self, sql, parameters=None)
        wrap_sql_executor("get_first")  # :get_first(self, sql, parameters=None)


def config_base_target_reporter(original_run):
    @functools.wraps(original_run)
    def _run_sql(self, sql, *args, **kwargs):
        config = AirflowTrackingConfig.current()

        if config.sql_reporting:
            # each instance of DbApiHook has `conn_name_attr` attribute that map to the actual name the attribute
            # with the connection id
            conn_id = getattr(self, self.conn_name_attr)
            conn_path = get_conn_path(conn_id)

            with log_sql_target_wrapper(self.get_uri(), conn_path, sql):
                # calling the original method to behave as the user expect
                return original_run(self, sql, *args, **kwargs)

        return original_run(self, sql, *args, **kwargs)

    return _run_sql
