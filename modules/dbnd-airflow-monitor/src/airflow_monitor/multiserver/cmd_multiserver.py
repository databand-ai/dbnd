# © Copyright Databand.ai, an IBM Company 2022

import logging

from airflow_monitor.adapter.validations import run_validations
from airflow_monitor.config import AirflowMonitorConfig
from airflow_monitor.multiserver.airflow_integration import AirflowIntegration
from dbnd import dbnd_config
from dbnd._core.errors.base import DatabandConfigError
from dbnd._vendor import click
from dbnd_monitor.integration_management_service import IntegrationManagementService
from dbnd_monitor.multiserver import MultiServerMonitor


logger = logging.getLogger(__name__)


def assert_valid_config(monitor_config):
    if monitor_config.sql_alchemy_conn and not monitor_config.syncer_name:
        raise DatabandConfigError(
            "Syncer name should be specified when using direct sql connection",
            help_msg="Please provide correct syncer name (using --syncer-name parameter,"
            " env variable DBND__AIRFLOW_MONITOR__SYNCER_NAME, or any other suitable way)",
        )


def start_multi_server_monitor(monitor_config: AirflowMonitorConfig):
    assert_valid_config(monitor_config)

    MultiServerMonitor(
        monitor_config=monitor_config,
        integration_management_service=IntegrationManagementService(),
        integration_types=[AirflowIntegration],
    ).run()


@click.command()
@click.option("--interval", type=click.INT, help="Interval between iterations")
@click.option(
    "--number-of-iterations",
    type=click.INT,
    help="Limit the number of periodic monitor runs",
)
@click.option(
    "--stop-after", type=click.INT, help="Limit time for monitor to run, in seconds"
)
@click.option(
    "--runner-type",
    type=click.Choice(["seq", "mp"]),
    help="Runner type. Options: seq for sequential, mp for multi-process",
)
@click.option("--syncer-name", type=click.STRING, help="Sync only specified instance")
def airflow_monitor_v2(*args, **kwargs):
    # remove all None values to not override defaults/env configured params

    # we need to load configs,
    # we have multiple get_databand_context().databand_api_client calls
    dbnd_config.load_system_configs()

    monitor_config_kwargs = {k: v for k, v in kwargs.items() if v is not None}
    monitor_config = AirflowMonitorConfig.from_env(**monitor_config_kwargs)
    run_validations()
    start_multi_server_monitor(monitor_config)


if __name__ == "__main__":
    airflow_monitor_v2()
