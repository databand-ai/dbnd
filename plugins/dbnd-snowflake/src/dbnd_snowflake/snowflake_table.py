from typing import Optional

from dbnd import log_duration
from dbnd._core.commands.metrics import log_data
from dbnd._core.plugin.dbnd_plugins import is_plugin_enabled
from dbnd_snowflake.snowflake_config import SnowflakeConfig


def log_snowflake_table(
    table_name: str,
    connection_string: str,
    database: str,
    schema: str,
    key: Optional[str] = None,
    with_preview: Optional[bool] = None,
    with_schema: Optional[bool] = None,
    raise_on_error: bool = False,
):

    if not is_plugin_enabled("dbnd-snowflake", module_import="dbnd_snowflake"):
        return
    from dbnd_snowflake import snowflake_values

    with log_duration("log_snowflake_table__time_seconds", source="system"):
        conn_params = snowflake_values.conn_str_to_conn_params(connection_string)
        account = conn_params["account"]
        user = conn_params["user"]
        password = conn_params["password"]

        config = SnowflakeConfig()
        snowflake_table = snowflake_values.SnowflakeTable(
            account,
            user,
            password,
            database,
            schema,
            table_name,
            config.table_preview_rows,
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
