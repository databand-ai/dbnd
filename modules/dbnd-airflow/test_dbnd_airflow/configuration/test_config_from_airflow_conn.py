import json
import logging

import pytest

from dbnd import config
from dbnd._core.configuration.environ_config import DATABAND_AIRFLOW_CONN_ID
from dbnd_airflow.tracking.dbnd_airflow_conf import (
    set_dbnd_config_from_airflow_connections,
)


DATABAND_URL = "http://databand_test_webserver"
# TODO: Add tests for error handling
CONN_FAILED_INFO_MSG = "The conn_id `{0}` isn't defined".format(
    DATABAND_AIRFLOW_CONN_ID
)
CONN_FAILED_WARNING_MSG = "No extra config provided to {0} connection.".format(
    DATABAND_AIRFLOW_CONN_ID
)
CONN_FAILED_ERROR_MSG = "Extra config for {0} connection, should be formated as a valid json.".format(
    DATABAND_AIRFLOW_CONN_ID
)
#

logger = logging.getLogger(__name__)


class TestConfigFromConnection(object):
    """ Check that setting dbnd_connection in airflow configures dbnd global config correctly."""

    @property
    def valid_extra_json_for_connection(self):
        return json.dumps({"core": {"databand_url": DATABAND_URL}})

    @pytest.fixture(scope="function", autouse=False)
    def add_valid_dbnd_airflow_connection(self, af_session):
        from airflow.models.connection import Connection

        # TODO: Handle as fixture
        existing_dbnd_conn = (
            af_session.query(Connection)
            .filter(Connection.conn_id == "dbnd_config")
            .one_or_none()
        )
        if existing_dbnd_conn:
            existing_dbnd_conn.extra = self.valid_extra_json_for_connection
        else:
            dbnd_config_connection = Connection(
                conn_id="dbnd_config",
                conn_type="HTTP",
                extra=self.valid_extra_json_for_connection,
            )
            af_session.add(dbnd_config_connection)

        af_session.commit()
        #

        airflow_db_url = af_session.bind.engine.url
        logging.info(
            "dbnd_config connection added to: {airflow_db_url}".format(
                airflow_db_url=airflow_db_url
            )
        )

    @pytest.mark.usefixtures("add_valid_dbnd_airflow_connection")
    def test_set_dbnd_config_from_valid_connection(self):

        logging.info("Setting config from Airflow dbnd_config connection")
        is_config_configured = set_dbnd_config_from_airflow_connections()

        assert is_config_configured
        assert config.get("core", "databand_url") == DATABAND_URL
