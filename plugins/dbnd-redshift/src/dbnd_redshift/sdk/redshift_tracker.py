import contextlib
import functools
import logging

from itertools import chain, takewhile
from typing import Dict, List, Optional

import sqlparse

from psycopg2.extras import DictConnection as DatabaseConnection, DictCursor as Cursor

from dbnd import log_dataset_op
from dbnd._core.log.external_exception_logging import log_exception_to_server
from dbnd._core.sql_tracker_common.sql_extract import READ, WRITE, SqlQueryExtractor
from dbnd._core.sql_tracker_common.sql_operation import (
    DTypes,
    SqlOperation,
    render_connection_path,
)


logger = logging.getLogger(__name__)

COPY_ROWS_COUNT_QUERY = "select pg_last_copy_count();"


class RedshiftTracker:
    def __init__(self, calculate_file_path=None):
        self.operations = []
        self._connection = None
        # custom function for file path calculation
        self.calculate_file_path = calculate_file_path

    def __enter__(self):
        if not hasattr(Cursor.execute, "__dbnd_patched__"):
            execute_original = Cursor.execute

            @functools.wraps(execute_original)
            def redshift_cursor_execute(cursor_self, *args, **kwargs):
                with self.track_execute(cursor_self, *args, **kwargs):
                    return execute_original(cursor_self, *args, **kwargs)

            redshift_cursor_execute.__dbnd_patched__ = execute_original
            Cursor.execute = redshift_cursor_execute

        if not hasattr(DatabaseConnection.close, "__dbnd_patched__"):
            close_original = DatabaseConnection.close

            @functools.wraps(close_original)
            def redshift_connection_close(connection_self, *args, **kwargs):
                # track connection before closing it (Example 1)
                self.unpatch_method(Cursor, "execute")
                if self._connection:
                    conn = self._connection
                else:
                    conn = connection_self
                self.flush_operations(conn)

                return close_original(connection_self, *args, **kwargs)

            redshift_connection_close.__dbnd_patched__ = close_original
            DatabaseConnection.close = redshift_connection_close

        return self

    @staticmethod
    def unpatch_method(obj, original_attr, patched_attr="__dbnd_patched__"):
        method = getattr(obj, original_attr)
        if hasattr(method, patched_attr):
            setattr(
                Cursor, original_attr, getattr(method, patched_attr),
            )

    def __exit__(self, exc_type, exc_val, exc_tb):
        self.unpatch_method(Cursor, "execute")
        self.unpatch_method(DatabaseConnection, "close")
        if self._connection:
            self.flush_operations(self._connection)

    def flush_operations(self, connection: DatabaseConnection):
        self.report_operations(connection, self.operations)
        # we clean all the batch of operations we reported so we don't report twice
        self.operations = []

    @contextlib.contextmanager
    def track_execute(self, cursor, command, *args, **kwargs):
        self._connection = PostgresConnectionWrapper(cursor.connection)
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
                    cursor, command, success, self.calculate_file_path, error
                )
                if operations:
                    # Only extend self.operations if read or write operation occurred in command
                    self.operations.extend(operations)
            except Exception as e:
                logging.exception("Error parsing redshift query")
                log_exception_to_server(e)

    def report_operations(
        self, connection: DatabaseConnection, operations: List[SqlOperation]
    ):
        # update the tables names
        operations = [op.evolve_table_name(connection) for op in operations]

        # looks for tables schemas
        tables = chain.from_iterable(op.tables for op in operations if not op.is_file)

        tables_schemas: Dict[str, DTypes] = {}

        for table in tables:
            table_schema = get_redshift_table_schema(connection, table)
            if table_schema:
                tables_schemas[table] = table_schema

        operations: List[SqlOperation] = [
            op.evolve_schema(tables_schemas) for op in operations
        ]

        for op in operations:
            log_dataset_op(
                op_path=render_connection_path(connection, op, "redshift"),
                op_type=op.op_type,
                success=op.success,
                data=op,
                with_schema=True,
                send_metrics=True,
                error=op.error,
                with_partition=True,
            )


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


def get_last_query_records_count(connection: DatabaseConnection):
    """
    Returns the number of rows that were loaded by the last COPY command run in the current session.
    """
    # TODO: handle array extraction of rows num , handle NONE
    result_set = redshift_query(connection, COPY_ROWS_COUNT_QUERY)
    if result_set:
        return result_set[0][0]


def redshift_query(connection: DatabaseConnection, query: str, params=None):
    try:
        with connection.cursor() as cursor:
            cursor.execute(query, params)
            result = cursor.fetchall()
            return result
    except Exception as e:
        logger.exception("Error occurred during querying redshift, query: %s", query)
        log_exception_to_server(e)


def build_redshift_operations(
    cursor: Cursor, command: str, success: bool, calculate_file_path, error: str
) -> List[SqlOperation]:
    operations = []
    sql_query_extractor = SqlQueryExtractor(calculate_file_path)
    command = sql_query_extractor.clean_query(command)
    # find the relevant operations schemas from the command
    parsed_query = sqlparse.parse(command)[0]
    extracted = sql_query_extractor.extract_operations_schemas(parsed_query)

    if not extracted:
        # This is DML statement and no read or write occurred
        return operations

    # helper method for building an operation from common values
    build_operation = functools.partial(
        SqlOperation,
        records_count=get_last_query_records_count(cursor.connection),
        query=command,
        # TODO: extract query id
        query_id=0,
        success=success,
        error=error,
    )

    if READ in extracted and WRITE not in extracted:
        # In this case this is a read only operation which means the cursor holds the actual result and the
        # description contains the schema of the operation.
        schema = None
        read = build_operation(
            extracted_schema=extracted[READ], dtypes=schema, op_type=READ
        )
        operations.append(read)
    else:
        # This is write operation which means the cursor holds only the `effected_rows` result which holds no
        # schema.
        if READ in extracted:
            read = build_operation(
                extracted_schema=extracted[READ], dtypes=None, op_type=READ
            )
            operations.append(read)

        write = build_operation(
            extracted_schema=extracted[WRITE], dtypes=None, op_type=WRITE
        )
        operations.append(write)
    return operations


class PostgresConnectionWrapper:
    def __init__(self, connection: DatabaseConnection):
        self.connection = connection
        self._schema = None

    @property
    def host(self) -> str:
        return self.connection.info.host

    @property
    def port(self) -> Optional[int]:
        return self.connection.info.port

    @property
    def database(self) -> Optional[str]:
        return self.connection.info.dbname

    @property
    def schema(self) -> Optional[str]:
        return self._schema

    @schema.setter
    def schema(self, schema):
        self._schema = schema

    def cursor(self):
        return self.connection.cursor()
