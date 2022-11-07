# © Copyright Databand.ai, an IBM Company 2022

import contextlib
import functools
import logging

from typing import List

import attr
import psycopg2
import sqlparse

from dbnd import log_dataset_op
from dbnd._core.log.external_exception_logging import log_exception_to_server
from dbnd._core.utils.sql_tracker_common.sql_extract import (
    READ,
    WRITE,
    SqlQueryExtractor,
)
from dbnd_redshift.sdk.redshift_connection_collection import (
    PostgresConnectionRuntime,
    RedshiftConnectionCollection,
)
from dbnd_redshift.sdk.redshift_connection_extractor import get_redshift_dataset
from dbnd_redshift.sdk.redshift_utils import (
    copy_to_temp_table,
    get_last_query_records_count,
)
from dbnd_redshift.sdk.redshift_values import RedshiftOperation
from dbnd_redshift.sdk.wrappers import (
    DbndConnectionWrapper,
    DbndCursorWrapper,
    PostgresConnectionWrapper,
)


REDSHIFT_TRACKER_OP_SOURCE = "redshift_tracker"

logger = logging.getLogger(__name__)


@attr.s
class RedshiftTrackerConfig:
    """
    Config class for redshift tracker, copycats log_dataset_op args

    Attributes:
        with_histograms(bool): Should calculate histogram of the given data - relevant only with data param.
            - Boolean to calculate or not on all the data columns.
        with_stats(bool): Should extract schema of the data as meta-data of the target - relevant only with data param.
            - Boolean to calculate or not on all the data columns.
        with_partition(bool): If True, the webserver tries to detect partitions of our datasets and extract them from the path,
                        otherwise not manipulating the dataset path at all.
        with_preview(bool): Should extract preview of the data as meta-data of the target - relevant only with data param.
        with_schema(bool): Should extract schema of the data as meta-data of the target - relevant only with data param.
        send_metrics(bool): Should report preview, schemas and histograms as metrics.
    """

    with_preview: bool = attr.ib(default=False)
    with_size: bool = attr.ib(default=None)
    with_schema: bool = attr.ib(default=True)
    with_stats: bool = attr.ib(default=False)
    with_histograms: bool = attr.ib(default=None)
    send_metrics: bool = attr.ib(default=True)
    with_partition: bool = attr.ib(default=None)
    with_percentiles: bool = attr.ib(default=True)

    def __attrs_post_init__(self):
        if self.with_stats and not self.with_schema:
            logger.warning(
                "Column level stats require schema extraction, ignoring with_schema=False"
            )
            self.with_schema = True


class RedshiftTracker:
    def __init__(
        self,
        calculate_file_path=None,
        conf: RedshiftTrackerConfig = RedshiftTrackerConfig(),
    ):

        self.conf = conf

        self.connections: RedshiftConnectionCollection[
            int, PostgresConnectionRuntime
        ] = RedshiftConnectionCollection(lambda: PostgresConnectionRuntime(None, []))

        # custom function for file path calculation
        self.calculate_file_path = calculate_file_path
        self.dataframe = None  # type: Pandas.DF

    def __enter__(self):
        if not hasattr(psycopg2.connect, "__dbnd_patched__"):
            connect_original = psycopg2.connect

            @functools.wraps(connect_original)
            def redshift_connect(*args, **kwargs):
                original_connection = connect_original(*args, **kwargs)
                return DbndConnectionWrapper(original_connection)

            redshift_connect.__dbnd_patched__ = connect_original
            psycopg2.connect = redshift_connect

        if not hasattr(psycopg2.extensions.register_type, "__dbnd_patched__"):
            register_type_original = psycopg2.extensions.register_type

            @functools.wraps(register_type_original)
            def psycopg_register_type(*args, **kwargs):
                try:
                    if isinstance(args[1], DbndConnectionWrapper):
                        connection = args[1]._sqla_unwrap
                    else:
                        connection = args[1]
                    return register_type_original(args[0], connection, **kwargs)
                except Exception:
                    logger.exception(
                        "Error patching psycopg register_type, using original method"
                    )
                    return register_type_original(*args, **kwargs)

            psycopg_register_type.__dbnd_patched__ = register_type_original
            psycopg2.extensions.register_type = psycopg_register_type

        if not hasattr(DbndCursorWrapper.execute, "__dbnd_patched__"):
            execute_original = DbndCursorWrapper.execute

            @functools.wraps(execute_original)
            def redshift_cursor_execute(cursor_self, *args, **kwargs):
                with self.track_execute(cursor_self, *args, **kwargs):
                    return execute_original(cursor_self, *args, **kwargs)

            redshift_cursor_execute.__dbnd_patched__ = execute_original
            DbndCursorWrapper.execute = redshift_cursor_execute

        if not hasattr(DbndConnectionWrapper.close, "__dbnd_patched__"):
            close_original = DbndConnectionWrapper.close

            @functools.wraps(close_original)
            def redshift_connection_close(connection_self, *args, **kwargs):
                conn = self.connections.get_connection(
                    connection_self, PostgresConnectionWrapper(connection_self)
                )
                self.flush_operations(conn)

                return close_original(connection_self, *args, **kwargs)

            redshift_connection_close.__dbnd_patched__ = close_original
            DbndConnectionWrapper.close = redshift_connection_close

        return self

    @staticmethod
    def unpatch_method(obj, original_attr, patched_attr="__dbnd_patched__"):
        method = getattr(obj, original_attr)
        if hasattr(method, patched_attr):
            setattr(obj, original_attr, getattr(method, patched_attr))

    def __exit__(self, exc_type, exc_val, exc_tb):
        self.unpatch_method(DbndCursorWrapper, "execute")
        self.unpatch_method(DbndConnectionWrapper, "close")
        self.unpatch_method(psycopg2, "connect")
        self.unpatch_method(psycopg2.extensions, "register_type")

        for connection in self.connections.values():
            self.flush_operations(connection.connection)

    def flush_operations(self, connection: PostgresConnectionWrapper):
        if connection in self.connections:
            for op in self.connections.get_operations(connection):

                if self.conf.with_schema:
                    op.extract_schema(connection)
                    if self.conf.with_stats:
                        op.extract_stats(
                            connection, with_percentiles=self.conf.with_percentiles
                        )
                if self.conf.with_preview:
                    op.extract_preview(connection)

                log_dataset_op(
                    op_path=op.render_connection_path(connection),
                    op_type=op.op_type,
                    success=op.success,
                    data=op,
                    error=op.error,
                    with_preview=self.conf.with_preview,
                    send_metrics=self.conf.send_metrics,
                    with_schema=self.conf.with_schema,
                    with_partition=self.conf.with_partition,
                    with_stats=self.conf.with_stats,
                    with_histograms=self.conf.with_histograms,
                    operation_source=REDSHIFT_TRACKER_OP_SOURCE,
                )
            # we clean all the batch of operations we reported so we don't report twice
            self.connections.clear_operations(connection)

    def set_dataframe(self, dataframe):
        """
        set dataframe
        Args:
            dataframe(pandas.DF): data structure with columns Which represents the information we have read
        """
        try:
            import pandas as pd

            if isinstance(dataframe, pd.DataFrame):
                self.dataframe = dataframe
            else:
                logger.exception(
                    "Error occurred during set dataframe. provided dataframe is not valid"
                )
        except Exception as e:
            logger.exception("Error occurred during set dataframe: %s", self.dataframe)
            log_exception_to_server(e)

    @contextlib.contextmanager
    def track_execute(self, cursor, command, *args, **kwargs):
        if cursor not in self.connections:
            self.connections.new_connection(
                cursor,
                PostgresConnectionRuntime(
                    PostgresConnectionWrapper(cursor.connection), []
                ),
            )
        success = True
        error = None
        try:
            yield
        except Exception as e:
            success = False
            error = str(e)
            raise
        finally:
            try:
                operations = build_redshift_operations(
                    cursor,
                    command,
                    success,
                    self.calculate_file_path,
                    error,
                    self.dataframe,
                )
                if operations:
                    first_op = operations[0]
                    if first_op.expect_tmp_table(self.conf):
                        copy_to_temp_table(
                            cursor.connection,
                            first_op.schema_name,
                            first_op.table_name,
                            command,
                        )
                    # Only extend self.connections obj operations
                    # if read or write operation occurred in command
                    self.connections.add_operations(cursor, operations)

            except Exception as e:
                logging.exception("Error parsing redshift query")
                log_exception_to_server(e)


def build_redshift_operations(
    cursor: DbndCursorWrapper,
    command: str,
    success: bool,
    calculate_file_path,
    error: str,
    dataframe=None,  # type: pd.DataFrame
) -> List[RedshiftOperation]:
    operations = []
    if calculate_file_path:
        sql_query_extractor = SqlQueryExtractor(calculate_file_path)
    else:
        sql_query_extractor = SqlQueryExtractor()
    clean_command = sql_query_extractor.clean_query(command)
    # find the relevant operations schemas from the command
    parsed_query = sqlparse.parse(clean_command)[0]
    extracted = sql_query_extractor.extract_operations_schemas(parsed_query)

    if not extracted:
        # This is DML statement and no read or write occurred
        return operations

    redshift_dataset = get_redshift_dataset(cursor.connection, clean_command)

    source_name = None
    if READ in extracted:
        source_name = list(extracted[READ].values())[0][0].dataset_name

    for op_type, schema in extracted.items():
        if op_type in [READ, WRITE]:
            operation = RedshiftOperation(
                # redshift_connection=cursor.connection,
                records_count=get_last_query_records_count(cursor.connection),
                query=command,
                # TODO: extract query id
                query_id=None,
                success=success,
                error=error,
                dataframe=dataframe,
                schema_name=redshift_dataset.schema,
                database=redshift_dataset.database,
                table_name=redshift_dataset.table,
                host=redshift_dataset.host,
                source_name=source_name,
                extracted_schema=schema,
                dtypes=None,
                op_type=op_type,
            )

            operations.append(operation)

    return operations
