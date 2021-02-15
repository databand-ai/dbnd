import os

import mock

# Do not remove this line
from airflow.configuration import conf
from airflow_monitor.airflow_servers_fetching import AirflowFetchingConfiguration
from airflow_monitor.cmd_airflow_monitor import airflow_monitor
from airflow_monitor.config import AirflowMonitorConfig
from click.testing import CliRunner

from dbnd._core.configuration.dbnd_config import config
from test_dbnd_airflow_monitor.airflow_utils import airflow_init_db


def test_command_running():
    db_path = "sqlite:///" + os.path.abspath(
        os.path.normpath(
            os.path.join(os.path.join(os.path.dirname(__file__), "fetch-unittests.db"))
        )
    )

    airflow_init_db(db_path)

    airflow_config = AirflowMonitorConfig()
    airflow_config.fetcher = "db"
    airflow_config.sql_alchemy_conn = db_path

    fetching_configuration = AirflowFetchingConfiguration(
        url="http://localhost:8082",
        fetcher="db",
        composer_client_id=None,
        sql_alchemy_conn=airflow_config.sql_alchemy_conn,
        local_dag_folder=airflow_config.local_dag_folder,
        api_mode="rbac",
        fetch_quantity=100,
        oldest_incomplete_data_in_days=14,
        include_logs=False,
        include_task_args=False,
        include_xcom=False,
        dag_ids=None,
    )

    # We need this mock, because otherwise we are going to enter an infinite loop in CI/CD
    with mock.patch("airflow_monitor.airflow_monitor_main.save_airflow_monitor_data"):
        with mock.patch(
            "airflow_monitor.airflow_monitor_main.save_airflow_server_info"
        ):
            with mock.patch(
                "airflow_monitor.airflow_servers_fetching.AirflowServersGetter.get_fetching_configurations",
                return_value=[fetching_configuration],
            ):
                runner = CliRunner()
                with config({"core": {"tracker": "console"}}):
                    result = runner.invoke(
                        airflow_monitor,
                        [
                            "--since",
                            "01/09/2020 10:00:00",
                            "--number-of-iterations",
                            1,
                        ],
                    )

    assert result.exit_code == 0
