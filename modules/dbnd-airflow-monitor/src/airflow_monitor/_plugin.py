# © Copyright Databand.ai, an IBM Company 2022

import logging

import dbnd


logger = logging.getLogger(__name__)


@dbnd.hookimpl
def dbnd_get_commands():
    from airflow_monitor.multiserver.cmd_liveness_probe import airflow_monitor_v2_alive
    from airflow_monitor.multiserver.cmd_multiserver import airflow_monitor_v2

    return [airflow_monitor_v2, airflow_monitor_v2_alive]
