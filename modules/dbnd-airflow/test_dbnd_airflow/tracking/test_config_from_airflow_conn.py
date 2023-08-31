# © Copyright Databand.ai, an IBM Company 2022

import json
import logging

from typing import Union

import pytest

from dbnd import config
from dbnd._core.configuration.environ_config import DATABAND_AIRFLOW_CONN_ID
from dbnd._core.settings import TrackingConfig
from dbnd_airflow.tracking.dbnd_airflow_conf import (
    get_dbnd_config_dict_from_airflow_connections,
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


logger = logging.getLogger(__name__)


def set_dbnd_airflow_connection(
    af_session,
    dbnd_config_value: Union[dict, str],
    password: str = None,
    conn_id=DATABAND_AIRFLOW_CONN_ID,
):
    from airflow.models.connection import Connection

    # delete from previous test
    dbnd_config_connection = (
        af_session.query(Connection).filter(Connection.conn_id == conn_id).delete()
    )

    dbnd_config_connection = Connection(
        conn_id=conn_id,
        conn_type="HTTP",
        password=password,
        extra=json.dumps(dbnd_config_value)
        if isinstance(dbnd_config_value, dict)
        else dbnd_config_value,
    )
    af_session.add(dbnd_config_connection)

    af_session.commit()

    airflow_db_url = af_session.bind.engine.url
    logging.info("%s added to: %s", dbnd_config_value, airflow_db_url)


def set_and_assert_config_configured():
    dbnd_config_from_connection = get_dbnd_config_dict_from_airflow_connections()
    actual = set_dbnd_config_from_airflow_connections(
        dbnd_config_from_connection=dbnd_config_from_connection
    )
    assert actual


class TestConfigFromConnection(object):
    """Check that setting dbnd_connection in airflow configures dbnd global config correctly."""

    def test_setting_dbnd_config_from_valid_connection(self, af_session):
        set_dbnd_airflow_connection(
            af_session,
            dbnd_config_value={
                "core": {
                    "databand_url": DATABAND_URL,
                    "databand_access_token": "some-token",
                }
            },
        )

        set_and_assert_config_configured()
        assert config.get("core", "databand_url") == DATABAND_URL

    @pytest.mark.parametrize(
        "token, expected_token",
        [(None, "some-token"), ("override-token", "override-token")],
    )
    def test_setting_dbnd_config_with_custom_password(
        self, af_session, token, expected_token
    ):
        set_dbnd_airflow_connection(
            af_session,
            dbnd_config_value={
                "core": {
                    "databand_url": DATABAND_URL,
                    "databand_access_token": "some-token",
                }
            },
            password=token,
        )

        set_and_assert_config_configured()
        assert config.get("core", "databand_access_token") == expected_token

    @pytest.mark.parametrize(
        "json_for_connection",
        [
            {
                "name": "invalid_dbnd_config_conn",
                "value": "{NOT,{} A VALID} JSON",
                "log_msg": CONN_FAILED_ERROR_MSG,
            },
            {
                "name": "empty_dbnd_config_conn",
                "value": "",
                "log_msg": CONN_FAILED_WARNING_MSG,
            },
        ],
    )
    def test_exceptions_when_setting_config_from_airflow_connection(
        self, json_for_connection, af_session, caplog
    ):
        # in the case of no_dbnd_conn we create connection with different name (or we might not create at all)
        conn_id = (
            DATABAND_AIRFLOW_CONN_ID
            if json_for_connection["name"] != "no_dbnd_conn"
            else "no_dbnd_conn"
        )

        set_dbnd_airflow_connection(
            af_session, dbnd_config_value=json_for_connection["value"], conn_id=conn_id
        )
        logger.info("Set config based on  {0}  ".format(json_for_connection["name"]))

        set_and_assert_config_configured()
        logger.info("INFO: %s", caplog.text)
        assert json_for_connection["log_msg"] in caplog.text, caplog.text

        logger.info("Test Succeeded")

    def test_settings_airflow_operarator_handler_custom(self, af_session):
        dbnd_config = {
            "core": {
                "databand_url": DATABAND_URL,
                "databand_access_token": "some-token",
            },
            "tracking": {
                "airflow_operator_handlers": {
                    "some_company.package.YourCompanyOperator": "dbnd_airflow.tracking.dbnd_conf.track_spark_operator_with_steps_and_jars_attrs"
                }
            },
        }

        actual = TrackingConfig().airflow_operator_handlers
        assert not actual

        set_dbnd_airflow_connection(af_session, dbnd_config_value=dbnd_config)

        set_and_assert_config_configured()

        actual = TrackingConfig().airflow_operator_handlers
        assert actual
        assert "some_company.package.YourCompanyOperator" in actual
