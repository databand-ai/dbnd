import logging


logger = logging.getLogger(__name__)


def set_airflow_sql_conn_from_dbnd_config():
    from airflow.settings import configure_vars, configure_orm, conf as airflow_conf
    from dbnd._core.configuration.dbnd_config import config as dbnd_config

    sql_alchemy_conn = airflow_conf.get("core", "sql_alchemy_conn")
    # airflow puts unittests into it db, regardles what we have in unittest.cfg
    if not sql_alchemy_conn or (
        "configured_at_databand_system_config_file" in sql_alchemy_conn
        or "unittests" in sql_alchemy_conn
    ):
        logger.debug("Set airflow sql_alchemy_conn from dbnd")
        sql_alchemy_conn = dbnd_config.get("core", "sql_alchemy_conn")
        airflow_conf.set("core", "sql_alchemy_conn", sql_alchemy_conn)

    fernet_key = airflow_conf.get("core", "fernet_key", fallback=None)
    if not fernet_key:
        logger.debug("Could not find fernet key, set from dbnd configuration")
        fernet_key = dbnd_config.get("airflow", "fernet_key")
        airflow_conf.set("core", "fernet_key", fernet_key)

    configure_vars()
    # The webservers import this file from models.py with the default settings.
    configure_orm()

    # add query handler before every execute
    # this will print query, code line and stack trace
    if dbnd_config.getboolean("log", "sqlalchemy_trace"):
        from sqlalchemy import event
        from airflow import settings as airflow_settings
        from dbnd_airflow.db_utils import trace_sqlalchemy_query

        event.listen(
            airflow_settings.engine, "before_cursor_execute", trace_sqlalchemy_query
        )

    # this will print query, code line and stack trace
    if dbnd_config.getboolean("log", "sqlalchemy_profile"):
        from sqlalchemy import event
        from airflow import settings as airflow_settings
        from dbnd_airflow.db_utils import (
            profile_before_cursor_execute,
            profile_after_cursor_execute,
        )

        event.listen(
            airflow_settings.engine,
            "before_cursor_execute",
            profile_before_cursor_execute,
        )
        event.listen(
            airflow_settings.engine,
            "after_cursor_execute",
            profile_after_cursor_execute,
        )
