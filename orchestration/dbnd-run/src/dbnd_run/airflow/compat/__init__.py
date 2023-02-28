# © Copyright Databand.ai, an IBM Company 2022
from distutils.version import LooseVersion

import airflow

from airflow.configuration import conf
from airflow.exceptions import AirflowConfigException


AIRFLOW_VERSION_2 = LooseVersion(airflow.version.version) >= LooseVersion("2.0.0")
AIRFLOW_VERSION_1 = LooseVersion(airflow.version.version) < LooseVersion("2.0.0")

AIRFLOW_VERSION_BEFORE_2_2 = LooseVersion(airflow.version.version) < LooseVersion(
    "2.2.0"
)
AIRFLOW_VERSION_AFTER_2_2 = LooseVersion(airflow.version.version) >= LooseVersion(
    "2.3.0"
)

AIRFLOW_ABOVE_13 = LooseVersion(airflow.version.version) > LooseVersion("1.10.13")
AIRFLOW_ABOVE_10 = LooseVersion(airflow.version.version) > LooseVersion("1.10.10")
AIRFLOW_ABOVE_9 = LooseVersion(airflow.version.version) > LooseVersion("1.10.9")
AIRFLOW_ABOVE_6 = LooseVersion(airflow.version.version) > LooseVersion("1.10.6")
AIRFLOW_BELOW_10 = LooseVersion(airflow.version.version) < LooseVersion("1.10.10")
if AIRFLOW_VERSION_1:
    from airflow.hooks.base_hook import BaseHook

elif AIRFLOW_VERSION_2:
    from airflow.hooks.base import BaseHook


def get_task_log_reader():
    try:
        return conf.get("core", "task_log_reader")
    except AirflowConfigException:
        return conf.get("logging", "task_log_reader")


__all__ = ["BaseHook"]
