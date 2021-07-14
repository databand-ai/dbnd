import mock

from airflow_monitor.airflow_servers_fetching import (
    DEFAULT_FETCH_QUANTITY,
    DEFAULT_OLDEST_INCOMPLETE_DATA_IN_DAYS,
    AirflowServersGetter,
)
from airflow_monitor.config import AirflowMonitorConfig
from dbnd._core.errors import DatabandConfigError


servers_fetch_response = {
    "data": [
        {
            "is_sync_enabled": True,
            "base_url": "localhost:8082",
            "api_mode": "rbac",
            "fetcher": "web",
            "composer_client_id": None,
            "fetch_quantity": None,
            "oldest_incomplete_data_in_days": None,
            "include_logs": None,
            "include_task_args": None,
            "include_xcom": None,
            "dag_ids": None,
        },
    ]
}


def test_airflow_servers_fetching_syncer_name():
    with mock.patch(
        "airflow_monitor.airflow_servers_fetching.AirflowServersGetter.fetch_airflow_servers_list",
        return_value=servers_fetch_response,
    ), mock.patch(
        "airflow_monitor.airflow_servers_fetching.logger_format_for_databand_error",
        return_value="",
    ) as log_error:
        config = AirflowMonitorConfig()
        config.syncer_name = "test_123"
        config.sql_alchemy_conn = None
        result = AirflowServersGetter().get_fetching_configurations()
        assert result is None
        log_error.assert_called_once()
        assert isinstance(log_error.call_args.args[0], DatabandConfigError)


def test_airflow_servers_fetching_direct_must_have_syncer_name():
    servers_fetch_data = {
        "data": [
            {
                "is_sync_enabled": True,
                "base_url": "localhost:8082",
                "api_mode": "rbac",
                "fetcher": "web",
                "composer_client_id": None,
                "fetch_quantity": None,
                "oldest_incomplete_data_in_days": None,
                "include_logs": None,
                "include_task_args": None,
                "include_xcom": None,
                "dag_ids": None,
            },
        ]
    }
    with mock.patch(
        "airflow_monitor.airflow_servers_fetching.AirflowServersGetter.fetch_airflow_servers_list",
        return_value=servers_fetch_data,
    ) as servers_fetch, mock.patch(
        "airflow_monitor.airflow_servers_fetching.logger_format_for_databand_error",
        return_value="",
    ) as log_error:
        config = AirflowMonitorConfig()
        config.sql_alchemy_conn = "test_321123asdasd connnection"
        config.syncer_name = None
        result = AirflowServersGetter().get_fetching_configurations()
        assert result is None
        servers_fetch.assert_not_called()
        log_error.assert_called_once()
        assert isinstance(log_error.call_args.args[0], DatabandConfigError)
        log_error.reset_mock()

        config.syncer_name = "test_321"
        result = AirflowServersGetter().get_fetching_configurations()
        assert result is None
        servers_fetch.assert_called_once()
        log_error.assert_called_once()
        assert isinstance(log_error.call_args.args[0], DatabandConfigError)
        servers_fetch.reset_mock()
        log_error.reset_mock()

        servers_fetch_data["data"][0]["name"] = "test_321"
        result = AirflowServersGetter().get_fetching_configurations()
        assert result is not None
        servers_fetch.assert_called_once()
        log_error.assert_not_called()


def test_airflow_servers_fetching():
    with mock.patch(
        "airflow_monitor.airflow_servers_fetching.AirflowServersGetter.fetch_airflow_servers_list",
        return_value=servers_fetch_response,
    ):
        config = AirflowMonitorConfig()
        config.syncer_name = None
        config.sql_alchemy_conn = None
        result = AirflowServersGetter().get_fetching_configurations()
        assert result
        assert len(result) == 1
        assert result[0].base_url == "localhost:8082"
        assert result[0].url == "localhost:8082/exportdataviewappbuilder"
        assert result[0].api_mode == "rbac"
        assert result[0].fetcher == "web"
        assert result[0].composer_client_id is None
        assert result[0].fetch_quantity == DEFAULT_FETCH_QUANTITY
        assert (
            result[0].oldest_incomplete_data_in_days
            == DEFAULT_OLDEST_INCOMPLETE_DATA_IN_DAYS
        )
        assert result[0].include_logs is False
        assert result[0].include_task_args is False
        assert result[0].include_xcom is False
        assert result[0].dag_ids is None
