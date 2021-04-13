from typing import Optional, Union

from snowflake.connector import SnowflakeConnection

from dbnd import log_duration
from dbnd._core.plugin.dbnd_plugins import is_plugin_enabled
from dbnd._core.tracking.metrics import log_data, log_target_operation
from dbnd_snowflake.extract_sql_query import TableTargetOperation
from dbnd_snowflake.snowflake_config import SnowflakeConfig
from dbnd_snowflake.snowflake_controller import SnowflakeController


def log_snowflake_table(
    table_name: str,
    connection_string: Union[str, SnowflakeConnection],
    database: str,
    schema: str,
    key: Optional[str] = None,
    with_preview: Optional[bool] = None,
    with_schema: Optional[bool] = None,
    raise_on_error: bool = False,
):
    """

    :param table_name: table name
    :param connection_string: either connection_string or actual connection
    :param database:
    :param schema:
    :param key:
    :param with_preview:
    :param with_schema:
    :param raise_on_error:
    :return:
    """
    if not is_plugin_enabled("dbnd-snowflake", module_import="dbnd_snowflake"):
        return
    from dbnd_snowflake import snowflake_values

    with log_duration(
        "log_snowflake_table__time_seconds", source="system"
    ), SnowflakeController(connection_string) as snowflake_ctrl:
        config = SnowflakeConfig()
        snowflake_table = snowflake_values.SnowflakeTable(
            snowflake_ctrl, database, schema, table_name, config.table_preview_rows,
        )
        log_data(
            key or "snowflake_table.{}".format(snowflake_table),
            snowflake_table,
            with_preview=with_preview,
            with_schema=with_schema,
            with_size=with_schema,
            with_histograms=False,
            raise_on_error=raise_on_error,
        )


def log_snowflake_table_targets(
    table_op: TableTargetOperation,
    connection_string: Union[str, SnowflakeConnection],
    with_preview: Optional[bool] = None,
    with_schema: Optional[bool] = None,
):
    if not is_plugin_enabled("dbnd-snowflake", module_import="dbnd_snowflake"):
        return

    from dbnd_snowflake.snowflake_values import SnowflakeTable

    with SnowflakeController(connection_string) as snowflake_ctrl:
        snowflake_table = SnowflakeTable.from_table(snowflake_ctrl, table_op.name)
        log_target_operation(
            name=table_op.name,
            target=table_op.path,
            operation_type=table_op.operation,
            success=table_op.success,
            data=snowflake_table,
            with_preview=with_preview,
            with_schema=with_schema,
        )
