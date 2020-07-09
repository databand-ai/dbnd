import logging
import os


AIRFLOW_LEGACY_URL_KEY = "airflow"
logger = logging.getLogger(__name__)


def setup_unittest_airflow():
    os.environ["AIRFLOW__CORE__UNIT_TEST_MODE"] = "True"
    from airflow import configuration as airflow_configuration
    from airflow.configuration import TEST_CONFIG_FILE

    # we can't call load_test_config, as it override airflow.cfg
    # we want to keep it as base
    logger.info("Reading Airflow test config at %s" % TEST_CONFIG_FILE)
    airflow_configuration.conf.read(TEST_CONFIG_FILE)

    # init db first
    from dbnd_airflow.dbnd_airflow_main import subprocess_airflow_initdb

    subprocess_airflow_initdb()

    # now reconnnect
    from dbnd_airflow.airflow_extensions.airflow_config import reinit_airflow_sql_conn

    reinit_airflow_sql_conn()
