# © Copyright Databand.ai, an IBM Company 2022

import logging


logger = logging.getLogger(__name__)


def reinit_airflow_sql_conn():
    from airflow.settings import configure_orm, configure_vars

    configure_vars()
    # The webservers import this file from models.py with the default settings.
    configure_orm()
    # add query handler before every execute
    # this will print query, code line and stack trace
