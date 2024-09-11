# © Copyright Databand.ai, an IBM Company 2022

import json
import logging

from typing import Optional, Union

import pytest

from dbnd import config
from dbnd._core.configuration.environ_config import DATABAND_AIRFLOW_CONN_ID
from dbnd._core.settings import TrackingConfig
from dbnd_airflow.compat import AIRFLOW_VERSION_1
from dbnd_airflow.tracking.dbnd_airflow_conf import (
    AIRFLOW_MONITOR_CONFIG_NAME,
    DAG_IDS_FOR_TRACKING_CONFIG_NAME,
    EXCLUDED_DAG_IDS_FOR_TRACKING_FLAG_CONFIG_NAME,
    get_dbnd_config_dict_from_airflow_connections,
    get_sync_status_and_tracking_dag_ids_from_dbnd_conf,
    set_dbnd_config_from_airflow_connections,
)


DATABAND_URL = "http://databand_test_webserver"

# Log Messages
CONN_FAILED_INFO_MSG = f"The conn_id `{DATABAND_AIRFLOW_CONN_ID}` isn't defined"
CONN_FAILED_WARNING_MSG = (
    f"No extra config provided to {DATABAND_AIRFLOW_CONN_ID} connection."
)
CONN_FAILED_ERROR_MSG = f"Extra config for {DATABAND_AIRFLOW_CONN_ID} connection, should be formated as a valid json."

# JSONS for Extra section in DBND Airflow connection


logger = logging.getLogger(__name__)


def set_dbnd_airflow_connection(
    af_session,
    dbnd_config_value: Union[dict, str],
    password: Optional[str] = None,
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

    # we can't print to log, as caplog is active in tests
    print(f"CONFIG FROM CONNECTION: {str(dbnd_config_from_connection)}")
    actual = set_dbnd_config_from_airflow_connections(
        dbnd_config_from_connection=dbnd_config_from_connection
    )
    assert actual


class TestConfigFromConnection:
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
        logger.info("Set config based on  %s  ", json_for_connection["name"])

        set_and_assert_config_configured()
        print(f"CAPTURED LOG: {caplog.text}")

        # exceptions are different in airflow 1, so we can not compare log_msg
        if not AIRFLOW_VERSION_1:
            assert json_for_connection["log_msg"] in caplog.text, caplog.text

        print("Test Succeeded")

    def test_settings_airflow_operator_handler_custom(self, af_session):
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

    def test_get_sync_status_and_tracking_dag_ids_from_dbnd_conf_empty(self):
        (
            sync_enabled,
            tracking_list,
            excluded_tracking_list,
        ) = get_sync_status_and_tracking_dag_ids_from_dbnd_conf(None)
        assert sync_enabled
        assert tracking_list is None

    @pytest.mark.parametrize(
        "dag_ids, expected_tracking_list, excluded_dag_ids, expected_excluded_tracking_list",
        [
            (None, None, None, None),
            ("", None, "", None),
            ("a", ["a"], None, None),
            ("a,b  ", ["a", "b"], None, None),
            (None, None, "a,b, c ", ["a", "b", "c"]),
            ("a,b", ["a", "b"], " c, d ", ["c", "d"]),
        ],
    )
    def test_get_sync_status_and_tracking_dag_ids_from_dbnd_conf_parsing(
        self,
        dag_ids,
        excluded_dag_ids,
        expected_tracking_list,
        expected_excluded_tracking_list,
    ):
        (
            sync_enabled,
            tracking_list,
            excluded_tracking_list,
        ) = get_sync_status_and_tracking_dag_ids_from_dbnd_conf(
            {
                AIRFLOW_MONITOR_CONFIG_NAME: {
                    DAG_IDS_FOR_TRACKING_CONFIG_NAME: dag_ids,
                    EXCLUDED_DAG_IDS_FOR_TRACKING_FLAG_CONFIG_NAME: excluded_dag_ids,
                }
            }
        )
        assert sync_enabled
        assert tracking_list == expected_tracking_list
        assert excluded_tracking_list == expected_excluded_tracking_list
