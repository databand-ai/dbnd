import logging
import os


logger = logging.getLogger(__name__)


def set_airflow_sql_conn_from_dbnd_config():
    logging.debug("updating airflow config from dbnd config")
    from dbnd._core.configuration.dbnd_config import config as dbnd_config

    sql_alchemy_conn = dbnd_config.get("airflow", "sql_alchemy_conn")
    if sql_alchemy_conn == "dbnd":
        logging.debug("updating airflow sql from dbnd core.sql_alchemy_conn")
        sql_alchemy_conn = dbnd_config.get("core", "sql_alchemy_conn")

    if sql_alchemy_conn and "AIRFLOW__CORE__SQL_ALCHEMY_CONN" not in os.environ:
        os.environ["AIRFLOW__CORE__SQL_ALCHEMY_CONN"] = sql_alchemy_conn

    fernet_key = dbnd_config.get("airflow", "fernet_key")
    if fernet_key == "dbnd":
        fernet_key = dbnd_config.get("core", "fernet_key")
    if fernet_key and "AIRFLOW__CORE__FERNET_KEY" not in os.environ:
        os.environ["AIRFLOW__CORE__FERNET_KEY"] = fernet_key


def init_airflow_sqlconn_by_dbnd():
    set_airflow_sql_conn_from_dbnd_config()
    from airflow.settings import configure_vars, configure_orm
    from dbnd._core.configuration.dbnd_config import config as dbnd_config

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

    # this will print query execution time
    from sqlalchemy import event
    from airflow import settings as airflow_settings
    from dbnd_airflow.db_utils import (
        profile_before_cursor_execute,
        profile_after_cursor_execute,
    )

    event.listen(
        airflow_settings.engine, "before_cursor_execute", profile_before_cursor_execute
    )
    event.listen(
        airflow_settings.engine, "after_cursor_execute", profile_after_cursor_execute
    )
