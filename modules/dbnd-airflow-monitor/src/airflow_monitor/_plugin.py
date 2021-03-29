import logging

import dbnd


logger = logging.getLogger(__name__)


@dbnd.hookimpl
def dbnd_get_commands():
    from airflow_monitor.cmd_airflow_monitor import airflow_monitor
    from airflow_monitor.syncer.cmd_runtime_syncer import runtime_syncer
    from airflow_monitor.multiserver.cmd_multiserver import multi_server_syncer

    return [airflow_monitor, runtime_syncer, multi_server_syncer]
