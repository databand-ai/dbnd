import logging

import dbnd


logger = logging.getLogger(__name__)


@dbnd.hookimpl
def dbnd_get_commands():
    from airflow_monitor.cmd_airflow_monitor import airflow_monitor

    return [airflow_monitor]
