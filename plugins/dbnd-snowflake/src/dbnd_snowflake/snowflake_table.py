from typing import Optional, Union

from snowflake.connector import SnowflakeConnection

from dbnd import log_duration
from dbnd._core.commands.metrics import log_data
from dbnd._core.plugin.dbnd_plugins import is_plugin_enabled
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
            key or table_name,
            snowflake_table,
            with_preview=with_preview,
            with_schema=with_schema,
            with_size=with_schema,
            with_histograms=False,
            raise_on_error=raise_on_error,
        )
