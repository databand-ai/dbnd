import logging

import dbnd


logger = logging.getLogger(__name__)


@dbnd.hookimpl
def dbnd_get_commands():
    from airflow_monitor.cmd_airflow_monitor import airflow_monitor
    from airflow_monitor.multiserver.cmd_multiserver import airflow_monitor_v2

    return [airflow_monitor, airflow_monitor_v2]
