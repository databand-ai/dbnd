import mock

from click.testing import CliRunner

from airflow_monitor.airflow_servers_fetching import AirflowFetchingConfiguration
from airflow_monitor.cmd_airflow_monitor import airflow_monitor
from airflow_monitor.data_fetchers import WebFetcher
from dbnd._core.configuration.dbnd_config import config
from test_dbnd_airflow_monitor.incomplete_fetching.plugin_simulator import (
    PluginSimulator,
)


"""
Those results assume that we have 3 runs, each with 10 task instances.
The end date of the first run is 2021-02-08T06:57:47.125706+00:00 and there is
a difference of exactly 10 minutes between run end dates.
This dict maps between the fetch quantity used to the expected parameters that the monitor uses for fetching.
The parameters are since and incomplete_offset.
"""

expected_results_by_fetch_quantity = {
    10: [
        ("2021-01-01T00:00:00+00:00", 0),
        ("2021-01-01T00:00:00+00:00", 10),
        ("2021-02-08T06:57:47.125706+00:00", 10),
        ("2021-02-08T07:07:47.125706+00:00", 10),
    ],
    9: [
        ("2021-01-01T00:00:00+00:00", 0),
        ("2021-01-01T00:00:00+00:00", 9),
        ("2021-02-08T06:57:47.125706+00:00", 8),
        ("2021-02-08T07:07:47.125706+00:00", 7),
    ],
    11: [
        ("2021-01-01T00:00:00+00:00", 0),
        ("2021-02-08T06:57:47.125706+00:00", 1),
        ("2021-02-08T07:07:47.125706+00:00", 2),
    ],
}


def get_fetching_configuration(fetch_quantity):
    return AirflowFetchingConfiguration(
        url="http://localhost:8082",
        fetcher="web",
        composer_client_id=None,
        sql_alchemy_conn=None,
        local_dag_folder=None,
        api_mode="rbac",
        fetch_quantity=fetch_quantity,
        oldest_incomplete_data_in_days=14,
        include_logs=False,
        include_task_args=False,
        include_xcom=False,
        dag_ids=None,
    )


plugin_simulator = PluginSimulator("dummy_dag.json", 3)
results = []


def fake_get_data(
    self,
    since,
    include_logs,
    include_task_args,
    include_xcom,
    dag_ids,
    quantity,
    fetch_type,
    incomplete_offset,
):
    global plugin_simulator
    global results
    if fetch_type != "incomplete_type1":
        return None

    results.append((str(since), incomplete_offset))
    return plugin_simulator.get_plugin_return(since, quantity, incomplete_offset)


def test_incomplete_type1():
    """
    The purpose of those tests is to make coverage of the code of sync_all_incomplete_data_type1 function
    from airflow_monitor_main.py and make sure since and offset are updated properly between fetches
    """
    global results
    for fetch_quantity in expected_results_by_fetch_quantity:
        results = []
        run_tests_on_fetch_quantity(fetch_quantity)


def run_tests_on_fetch_quantity(fetch_quantity):
    with mock.patch(
        "airflow_monitor.airflow_monitor_main.save_airflow_monitor_data"
    ) as p1, mock.patch(
        "airflow_monitor.airflow_monitor_main.save_airflow_server_info"
    ) as p2, mock.patch(
        "airflow_monitor.airflow_servers_fetching.AirflowServersGetter.get_fetching_configurations",
        return_value=[get_fetching_configuration(fetch_quantity)],
    ) as p3, mock.patch.object(
        WebFetcher, "get_data", new=fake_get_data
    ) as p4:
        runner = CliRunner()
        result = runner.invoke(
            airflow_monitor, ["--number-of-iterations", 1, "--since", "2021-01-01"],
        )

    assert result.exit_code == 0
    assert len(results) == len(expected_results_by_fetch_quantity[fetch_quantity])

    for i in range(len(results)):
        assert results[i] == expected_results_by_fetch_quantity[fetch_quantity][i]
