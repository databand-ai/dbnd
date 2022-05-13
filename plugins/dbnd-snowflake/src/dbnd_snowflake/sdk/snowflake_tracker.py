import contextlib
import functools
import logging

from collections import namedtuple
from itertools import chain, takewhile
from typing import Dict, List, Optional

import sqlparse

from snowflake.connector import SnowflakeConnection
from snowflake.connector.cursor import DictCursor, SnowflakeCursor

from dbnd import log_dataset_op
from dbnd._core.log.external_exception_logging import log_exception_to_server
from dbnd._core.utils.sql_tracker_common.sql_extract import (
    READ,
    WRITE,
    SqlQueryExtractor,
)
from dbnd._core.utils.sql_tracker_common.sql_operation import (
    DTypes,
    SqlOperation,
    render_connection_path,
)


logger = logging.getLogger(__name__)

SNOWFLAKE_TYPE_CODE_MAP = {
    0: "NUMBER",
    1: "FLOAT",
    2: "VARCHAR",
    3: "DATE",
    4: "TIMESTAMP",
    5: "VARIANT",
    6: "TIMESTAMP_LTZ",
    7: "TIMESTAMP_TZ",
    8: "TIMESTAMP_TZ",
    9: "OBJECT",
    10: "ARRAY",
    11: "BINARY",
    12: "TIME",
    13: "BOOLEAN",
}
ParsedResultMetadata = namedtuple(
    "ParsedResultMetadata",
    [
        "name",
        "type_code",
        "display_size",
        "internal_size",
        "precision",
        "scale",
        "is_nullable",
    ],
)


def extract_schema_from_sf_desc(description):
    # todo: support ResultMetadata in version 2.4.6
    dtypes = {}
    for column in map(lambda col: ParsedResultMetadata(*col), description):
        dtypes[column.name] = SNOWFLAKE_TYPE_CODE_MAP[column.type_code]
    return dtypes


class SnowflakeTracker(object):
    def __init__(self):
        self.operations = []
        self._connection = None
        # result set of executed snowflake statement
        self.result_set: dict = None

    def __enter__(self):
        if not hasattr(SnowflakeCursor.execute, "__dbnd_patched__"):
            execute_original = SnowflakeCursor.execute
            execute_helper_original = SnowflakeCursor._execute_helper

            @functools.wraps(execute_original)
            def snowflake_cursor_execute(cursor_self, *args, **kwargs):
                with self.track_execute(cursor_self, *args, **kwargs):
                    return execute_original(cursor_self, *args, **kwargs)

            snowflake_cursor_execute.__dbnd_patched__ = execute_original
            SnowflakeCursor.execute = snowflake_cursor_execute

            @functools.wraps(execute_helper_original)
            def snowflake_cursor_execute_helper(query, *args, **kwargs):
                self.result_set = execute_helper_original(query, *args, **kwargs)
                return self.result_set

            snowflake_cursor_execute_helper.__dbnd_patched__ = execute_helper_original
            SnowflakeCursor._execute_helper = snowflake_cursor_execute_helper

        if not hasattr(SnowflakeConnection.close, "__dbnd_patched__"):
            close_original = SnowflakeConnection.close

            @functools.wraps(close_original)
            def snowflake_connection_close(connection_self, *args, **kwargs):
                # track connection before closing it (Example 1)
                self.unpatch_method(SnowflakeCursor, "execute")
                self.unpatch_method(SnowflakeCursor, "_execute_helper")
                self.flush_operations(connection_self)

                return close_original(connection_self, *args, **kwargs)

            snowflake_connection_close.__dbnd_patched__ = close_original
            SnowflakeConnection.close = snowflake_connection_close

        return self

    @staticmethod
    def unpatch_method(obj, original_attr, patched_attr="__dbnd_patched__"):
        method = getattr(obj, original_attr)
        if hasattr(method, patched_attr):
            setattr(SnowflakeCursor, original_attr, getattr(method, patched_attr))

    def __exit__(self, exc_type, exc_val, exc_tb):
        self.unpatch_method(SnowflakeCursor, "execute")
        self.unpatch_method(SnowflakeCursor, "_execute_helper")
        self.unpatch_method(SnowflakeConnection, "close")
        if self._connection:
            self.flush_operations(self._connection)

    def flush_operations(self, connection: SnowflakeConnection):
        self.report_operations(connection, self.operations)
        # we clean all the batch of operations we reported so we don't report twice
        self.operations = []

    def report_operations(
        self, connection: SnowflakeConnection, operations: List[SqlOperation]
    ):
        # update the tables names
        operations = [op.evolve_table_name(connection) for op in operations]

        # looks for tables schemas
        tables = chain.from_iterable(
            op.tables for op in operations if not (op.is_file or op.is_stage)
        )

        tables_schemas: Dict[str, DTypes] = {}
        if not connection.is_closed():
            # connection already closed, cannot get table schema
            for table in tables:
                table_schema = get_snowflake_table_schema(connection, table)
                if table_schema:
                    tables_schemas[table] = table_schema

            operations: List[SqlOperation] = [
                op.evolve_schema(tables_schemas) for op in operations
            ]

        for op in operations:
            log_dataset_op(
                op_path=render_connection_path(connection, op, "snowflake"),
                op_type=op.op_type,
                success=op.success,
                data=op,
                with_schema=True,
                send_metrics=True,
                error=op.error,
                with_partition=True,
            )

    @contextlib.contextmanager
    def track_execute(self, cursor, command, *args, **kwargs):
        self._connection = cursor.connection
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
                operations = build_snowflake_operations(
                    cursor, command, success, self.result_set, error
                )
                if operations:
                    # Only extend self.operations if read or write operation occurred in command
                    self.operations.extend(operations)
                else:
                    logger.warning(
                        "Query %s is not supported by snowflake tracker", command
                    )
            except Exception:
                logging.exception("Error parsing snowflake query")


def get_snowflake_table_schema(connection, table) -> Optional[DTypes]:
    try:
        desc_results = snowflake_query(connection, f"desc table {table}")
    except Exception as e:
        logger.warning(
            "Failed to fetch table description for table %s", table, exc_info=True
        )
        log_exception_to_server(e)
    else:
        schema = {}
        for col_desc in desc_results:
            # remove the trailing part of the column type that's inside brackets
            # `NUMBER(123,0) -> NUMBER`, `VARCHAR(23,2) -> VARCHAR` and so on
            normalized_col_type = "".join(
                takewhile(lambda c: c != "(", col_desc["type"])
            )
            normalized_col_name = col_desc["name"].lower()
            schema[normalized_col_name] = normalized_col_type
        return schema


def snowflake_query(connection: SnowflakeConnection, query: str, params=None):
    try:
        with connection.cursor(DictCursor) as cursor:
            cursor.execute(query, params)
            result = cursor.fetchall()
            return result
    except Exception:
        logger.exception("Error occurred during querying Snowflake, query: %s", query)
        raise


def extract_inserted_rows(result_set: Dict) -> int:
    return (
        result_set.get("data", {}).get("stats", {}).get("numRowsInserted", 0)
        if result_set is not None
        else 0
    )


def build_snowflake_operations(
    cursor: SnowflakeCursor, command: str, success: bool, result_set: Dict, error: str
) -> List[SqlOperation]:
    operations = []
    sql_query_extractor = SqlQueryExtractor()
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
        records_count=extract_inserted_rows(result_set),
        query=command,
        query_id=cursor.sfqid,
        success=success,
        error=error,
    )

    if READ in extracted and WRITE not in extracted:
        # In this case this is a read only operation which means the cursor holds the actual result and the
        # description contains the schema of the operation.
        schema = extract_schema_from_sf_desc(cursor.description)
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
