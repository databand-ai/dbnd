import contextlib
import functools

from collections import namedtuple
from itertools import chain, takewhile
from typing import Dict, List

import sqlparse

from snowflake.connector import SnowflakeConnection
from snowflake.connector.cursor import DictCursor, SnowflakeCursor

from dbnd import log_dataset_op
from dbnd_snowflake.POC.sql_extract import READ, WRITE, SqlQueryExtractor
from dbnd_snowflake.POC.sql_operation import (
    DTypes,
    SqlOperation,
    render_connection_path,
)


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


class PocSnowflakeTracker(object):
    def __init__(self):
        self.operations = []

    def __enter__(self):
        if not hasattr(SnowflakeCursor.execute, "__dbnd_patched__"):
            execute_original = SnowflakeCursor.execute

            @functools.wraps(execute_original)
            def snowflake_cursor_execute(cursor_self, *args, **kwargs):
                with self.track_execute(cursor_self, *args, **kwargs):
                    return execute_original(cursor_self, *args, **kwargs)

            snowflake_cursor_execute.__dbnd_patched__ = execute_original
            SnowflakeCursor.execute = snowflake_cursor_execute

        if not hasattr(SnowflakeConnection.close, "__dbnd_patched__"):
            close_original = SnowflakeConnection.close

            @functools.wraps(close_original)
            def snowflake_connection_close(connection_self, *args, **kwargs):
                # track connection before closing it (Example 1)
                self.unpatch_method(SnowflakeCursor, "execute")

                self.track_connection_data(connection_self)
                return close_original(connection_self, *args, **kwargs)

            snowflake_connection_close.__dbnd_patched__ = close_original
            SnowflakeConnection.close = snowflake_connection_close

        return self

    def unpatch_method(self, obj, original_attr, patched_attr="__dbnd_patched__"):
        method = getattr(obj, original_attr)
        if hasattr(method, patched_attr):
            setattr(
                SnowflakeCursor, original_attr, getattr(method, patched_attr),
            )

    def __exit__(self, exc_type, exc_val, exc_tb):
        self.unpatch_method(SnowflakeCursor, "execute")
        self.unpatch_method(SnowflakeConnection, "close")

    def track_connection_data(self, connection_self: SnowflakeConnection):
        # update the tables names
        operations = [op.evolve_table_name(connection_self) for op in self.operations]

        # looks for tables schemas
        tables = chain.from_iterable(op.tables for op in operations)
        tables_schemas: Dict[str, DTypes] = {
            table: get_snowflake_table_schema(connection_self, table)
            for table in tables
        }

        operations: List[SqlOperation] = [
            op.evolve_schema(tables_schemas) for op in operations
        ]

        for op in operations:
            log_dataset_op(
                op_path=render_connection_path(connection_self, op, "snowflake"),
                op_type=op.op_type,
                success=op.success,
                data=op,
                with_schema=True,
                send_metrics=True,
            )

    @contextlib.contextmanager
    def track_execute(self, cursor, command, *args, **kwargs):
        success = True
        try:
            yield
        except Exception as e:
            success = False
            raise
        finally:
            operations = build_snowflake_operations(cursor, command, success)
            self.operations.extend(operations)


def get_snowflake_table_schema(connection, table):
    desc_results = snowflake_query(connection, f"desc table {table}")
    schema = {}
    for col_desc in desc_results:
        # remove the trailing part of the column type that's inside brackets
        # `NUMBER(123,0) -> NUMBER`, `VARCHAR(23,2) -> VARCHAR` and so on
        normalized_col_type = "".join(takewhile(lambda c: c != "(", col_desc["type"]))
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
        print("Error occurred during querying Snowflake, query: %s", query)
        raise


def build_snowflake_operations(
    cursor: SnowflakeCursor, command: str, success: bool
) -> List[SqlOperation]:
    operations = []

    # find the relevant operations schemas from the command
    parsed_query = sqlparse.parse(command)[0]
    extracted = SqlQueryExtractor().extract_operations_schemas(parsed_query)

    # helper method for building an operation from common values
    build_operation = functools.partial(
        SqlOperation,
        records_count=cursor.rowcount,
        query=command,
        query_id=cursor.sfqid,
        success=success,
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
