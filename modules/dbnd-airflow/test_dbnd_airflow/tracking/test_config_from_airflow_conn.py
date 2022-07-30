# © Copyright Databand.ai, an IBM Company 2022

import json
import logging

import pytest

from dbnd import config
from dbnd._core.configuration.environ_config import DATABAND_AIRFLOW_CONN_ID
from dbnd_airflow.tracking.dbnd_airflow_conf import (
    set_dbnd_config_from_airflow_connections,
)


DATABAND_URL = "http://databand_test_webserver"

# Log Messages
CONN_FAILED_INFO_MSG = "The conn_id `{0}` isn't defined".format(
    DATABAND_AIRFLOW_CONN_ID
)
CONN_FAILED_WARNING_MSG = "No extra config provided to {0} connection.".format(
    DATABAND_AIRFLOW_CONN_ID
)
CONN_FAILED_ERROR_MSG = (
    "Extra config for {0} connection, should be formated as a valid json.".format(
        DATABAND_AIRFLOW_CONN_ID
    )
)

# JSONS for Extra section in DBND Airflow connection
VALID_EXTRA_JSON_FOR_CONNECTION = {
    "name": "valid_dbnd_config_conn",
    "value": json.dumps({"core": {"databand_url": DATABAND_URL}}),
}

BAD_JSONS_FOR_AIRFLOW_CONNECTION = [
    {
        "name": "invalid_dbnd_config_conn",
        "value": "{NOT,{} A VALID} JSON",
        "log_msg": CONN_FAILED_ERROR_MSG,
    },
    {"name": "empty_dbnd_config_conn", "value": "", "log_msg": CONN_FAILED_WARNING_MSG},
    {"name": "no_dbnd_conn", "value": "", "log_msg": CONN_FAILED_INFO_MSG},
]


logger = logging.getLogger(__name__)


class TestConfigFromConnection(object):
    """Check that setting dbnd_connection in airflow configures dbnd global config correctly."""

    def set_dbnd_airflow_connection(self, af_session, json_for_connection):
        from airflow.models.connection import Connection

        dbnd_config_connection = (
            af_session.query(Connection)
            .filter(Connection.conn_id == DATABAND_AIRFLOW_CONN_ID)
            .delete()
        )
        if json_for_connection["name"] != "no_dbnd_conn":
            dbnd_config_connection = Connection(
                conn_id=DATABAND_AIRFLOW_CONN_ID,
                conn_type="HTTP",
                extra=json_for_connection["value"],
            )
            af_session.add(dbnd_config_connection)

        af_session.commit()

        airflow_db_url = af_session.bind.engine.url
        logging.info(
            "{config_name} added to: {airflow_db_url}".format(
                config_name=json_for_connection["name"], airflow_db_url=airflow_db_url
            )
        )

    def test_setting_dbnd_config_from_valid_connection(self, af_session):
        self.set_dbnd_airflow_connection(
            af_session, json_for_connection=VALID_EXTRA_JSON_FOR_CONNECTION
        )

        is_config_configured = set_dbnd_config_from_airflow_connections()

        assert is_config_configured
        assert config.get("core", "databand_url") == DATABAND_URL

    @pytest.mark.parametrize("json_for_connection", BAD_JSONS_FOR_AIRFLOW_CONNECTION)
    def test_exceptions_when_setting_config_from_airflow_connection(
        self, json_for_connection, af_session, caplog
    ):
        self.set_dbnd_airflow_connection(af_session, json_for_connection)
        logger.info("Set config based on  {0}  ".format(json_for_connection["name"]))
        is_config_configured = set_dbnd_config_from_airflow_connections()

        assert not is_config_configured
        assert json_for_connection["log_msg"] in caplog.text

        logger.info("Test Succeeded")
