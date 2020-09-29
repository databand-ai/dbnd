import logging
import os

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
        )
    except sh.ErrorReturnCode as e:
        logging.exception("Failed to populate db. Exception: {}".format(e.stderr))
        raise
