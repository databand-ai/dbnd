# © Copyright Databand.ai, an IBM Company 2024

import os

from mock import patch

from airflow_monitor.config import AirflowMonitorConfig


def test_airflow_monitor_config_from_env():
    env = {
        "DBND__AIRFLOW_MONITOR__SYNCER_NAME": "amazing_syncer",
        "DBND__AIRFLOW_MONITOR__IS_SYNC_ENABLED": "true",
    }
    with patch.dict(os.environ, env):
        airflow_config = AirflowMonitorConfig.from_env()
        assert airflow_config.syncer_name == "amazing_syncer"
        assert airflow_config.is_sync_enabled is True

    env = {
        "DBND__AIRFLOW_MONITOR__SYNCER_NAME": "awful_syncer",
        "DBND__AIRFLOW_MONITOR__IS_SYNC_ENABLED": "False",
    }
    with patch.dict(os.environ, env):
        airflow_config = AirflowMonitorConfig.from_env()
        assert airflow_config.syncer_name == "awful_syncer"
        assert airflow_config.is_sync_enabled is False
