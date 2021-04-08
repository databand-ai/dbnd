import functools
import logging

from contextlib import contextmanager

from dbnd._core.constants import DbndTargetOperationStatus
from dbnd._core.tracking.metrics import log_target_operation
from dbnd_airflow.tracking.airflow_connections import get_conn_path
from dbnd_snowflake.extract_sql_query import extract_from_sql


logger = logging.getLogger(__name__)


@contextmanager
def log_sql_target_wrapper(conn_path, sql_query):
    # type: (str, str) -> None
    """
    Context manager to handle the tracking of sql query execution.

    @param conn_path: connection string format as path. use to format a the target's path
    @param sql_query: a sql query to extract the table operations from
    """
    try:
        yield
    except:
        log_sql_targets(conn_path, sql_query, DbndTargetOperationStatus.NOK)
        raise
    else:
        log_sql_targets(conn_path, sql_query, DbndTargetOperationStatus.OK)


def log_sql_targets(path, sql_query, operation_status):
    # type: (str, str, DbndTargetOperationStatus) -> None
    """
    Extracts and log any table operation from the sql_query
    """
    try:
        for path, name, operation in extract_from_sql(path, sql_query):
            log_target_operation(
                name=name,
                target=path,
                operation_type=operation,
                operation_status=operation_status,
            )
    except Exception:
        logger.exception("Caught exception will trying to log target for sql_query")


def patch_airflow_db_hook(operator_cls):
    def wrap_sql_executor(name):
        original_run = getattr(operator_cls, name)

        @functools.wraps(original_run)
        def _run_sql(self, sql, *args, **kwargs):
            """Patch function to replace a method for airflow's DbApiHook"""
            # each instance of DbApiHook has `conn_name_attr` attribute that map to the actual name the attribute
            # with the connection id
            conn_id = getattr(self, self.conn_name_attr)
            conn_path = get_conn_path(conn_id)

            with log_sql_target_wrapper(conn_path, sql):
                # calling the original method to behave as the user expect
                return original_run(self, sql, *args, **kwargs)

        setattr(operator_cls, name, _run_sql)

    from airflow.hooks.dbapi_hook import DbApiHook

    if issubclass(operator_cls, DbApiHook):
        # signature >  run(self, sql, autocommit=False, parameters=None)
        wrap_sql_executor("run")

        # signature >  get_records(self, sql, parameters=None)
        wrap_sql_executor("get_records")

        # signature >  get_pandas_df(self, sql, parameters=None)
        wrap_sql_executor("get_pandas_df")

        # signature >  get_first(self, sql, parameters=None)
        wrap_sql_executor("get_first")
