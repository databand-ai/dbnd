import mock

from airflow_monitor.airflow_servers_fetching import (
    DEFAULT_FETCH_QUANTITY,
    DEFAULT_OLDEST_INCOMPLETE_DATA_IN_DAYS,
    AirflowServersGetter,
)


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
        }
    ]
}


def test_airflow_servers_fetching():
    with mock.patch(
        "airflow_monitor.airflow_servers_fetching.AirflowServersGetter.fetch_airflow_servers_list",
        return_value=servers_fetch_response,
    ):
        result = AirflowServersGetter().get_fetching_configurations()
        assert result
        assert len(result) == 1
        assert result[0].base_url == "localhost:8082"
        assert result[0].url == "localhost:8082/exportdataviewappbuilder/export_data"
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
