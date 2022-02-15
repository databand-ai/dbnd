import logging


logger = logging.getLogger(__name__)


def reinit_airflow_sql_conn():
    from airflow.settings import configure_orm, configure_vars

    from dbnd._core.configuration.dbnd_config import config as dbnd_config

    configure_vars()
    # The webservers import this file from models.py with the default settings.
    configure_orm()
    # add query handler before every execute
    # this will print query, code line and stack trace
    if dbnd_config.getboolean("log", "sqlalchemy_trace"):
        from airflow import settings as airflow_settings
        from sqlalchemy import event

        from dbnd_airflow.db_utils import trace_sqlalchemy_query

        event.listen(
            airflow_settings.engine, "before_cursor_execute", trace_sqlalchemy_query
        )

    # this will print query execution time
    from airflow import settings as airflow_settings
    from sqlalchemy import event

    from dbnd_airflow.db_utils import (
        profile_after_cursor_execute,
        profile_before_cursor_execute,
    )

    event.listen(
        airflow_settings.engine, "before_cursor_execute", profile_before_cursor_execute
    )
    event.listen(
        airflow_settings.engine, "after_cursor_execute", profile_after_cursor_execute
    )
