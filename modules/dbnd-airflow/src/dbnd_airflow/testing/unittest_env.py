# © Copyright Databand.ai, an IBM Company 2022

import logging
import os
import subprocess
import sys


AIRFLOW_LEGACY_URL_KEY = "airflow"
logger = logging.getLogger(__name__)


def subprocess_airflow(args):
    """Forward arguments to airflow command line"""

    from airflow.configuration import conf
    from sqlalchemy.engine.url import make_url

    # let's make sure that we user correct connection string
    airflow_sql_conn = conf.get("core", "SQL_ALCHEMY_CONN")
    env = os.environ.copy()
    env["AIRFLOW__CORE__SQL_ALCHEMY_CONN"] = airflow_sql_conn
    env["AIRFLOW__CORE__FERNET_KEY"] = conf.get("core", "FERNET_KEY")

    # if we use airflow, we can get airflow from external env
    args = ["airflow"] + args
    logging.info(
        "Running airflow command at subprocess: '%s" " with DB=%s",
        subprocess.list2cmdline(args),
        repr(make_url(airflow_sql_conn)),
    )
    try:
        subprocess.check_call(args=args, env=env)
    except Exception:
        logging.exception(
            "Failed to run airflow command %s with path=%s",
            subprocess.list2cmdline(args),
            sys.path,
        )
        raise
    logging.info("Airflow command has been successfully executed")


def subprocess_airflow_initdb():
    logging.info("Initializing Airflow DB")
    from dbnd_airflow.compat import AIRFLOW_VERSION_2

    if AIRFLOW_VERSION_2:
        return subprocess_airflow(args=["db", "init"])

    return subprocess_airflow(args=["initdb"])


def setup_unittest_airflow():
    os.environ["AIRFLOW__CORE__UNIT_TEST_MODE"] = "True"
    from airflow import configuration as airflow_configuration
    from airflow.configuration import TEST_CONFIG_FILE

    # we can't call load_test_config, as it override airflow.cfg
    # we want to keep it as base
    logger.info("Reading Airflow test config at %s" % TEST_CONFIG_FILE)
    airflow_configuration.conf.read(TEST_CONFIG_FILE)

    # init db first
    subprocess_airflow_initdb()
    logger.info("Airflow DB has been initialized")

    # now reconnnect
    from dbnd_airflow.testing.airflow_config import reinit_airflow_sql_conn

    reinit_airflow_sql_conn()
