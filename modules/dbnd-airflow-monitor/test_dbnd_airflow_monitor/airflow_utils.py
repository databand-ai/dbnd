# © Copyright Databand.ai, an IBM Company 2022

import logging
import os

from functools import wraps

import sh


def airflow_init_db(db_path):
    airflow = sh.Command("airflow")
    try:
        airflow(
            "initdb",
            _env={
                "AIRFLOW__CORE__SQL_ALCHEMY_CONN": db_path,
                "AIRFLOW_HOME": os.environ["AIRFLOW_HOME"],
            },
            _truncate_exc=False,
        )
    except sh.ErrorReturnCode as e:
        logging.exception("Failed to populate db. Exception: %s", e.stderr)
        raise


class TestConnectionError(ConnectionError):
    pass


def can_be_dead(f):
    @wraps(f)
    def wrapped(*args, **kwargs):
        if not args[0].alive:
            raise TestConnectionError()
        return f(*args, **kwargs)

    return wrapped
