import logging

from logging.config import dictConfig

import dbnd

from dbnd.testing.helpers import run_dbnd_subprocess__with_home
from dbnd_airflow_contrib.dbnd_airflow_default_logger import DEFAULT_LOGGING_CONFIG


class TestDbndAirflowLogging(object):
    def test_dbnd_airflow_logging_conifg(self):
        # we implement it as a separte test, as we don't want to affect current logging system
        dbnd_config = DEFAULT_LOGGING_CONFIG
        assert dbnd_config

    def test_can_be_loaded(self):
        # we can't just load config, it will affect all future tests
        output = run_dbnd_subprocess__with_home([__file__.replace(".pyc", ".py")])
        assert "test_can_be_loaded OK" in output
        logging.error("Done")


if __name__ == "__main__":
    print(
        dbnd.__version__
    )  # we need it first to import, before we import any airflow code
    dbnd_config = DEFAULT_LOGGING_CONFIG

    dictConfig(dbnd_config)
    logging.info("test_can_be_loaded OK")
