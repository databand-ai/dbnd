# © Copyright Databand.ai, an IBM Company 2022


import airflow

from dbnd._vendor import version


AIRFLOW_VERSION_BEFORE_2_2 = version.parse(airflow.version.version) < version.parse(
    "2.2.0"
)
AIRFLOW_VERSION_2 = version.parse(airflow.version.version) >= version.parse("2.0.0")

AIRFLOW_VERSION_1 = version.parse(airflow.version.version) < version.parse("2.0.0")

if AIRFLOW_VERSION_1:
    from airflow.hooks.base_hook import BaseHook

elif AIRFLOW_VERSION_2:
    from airflow.hooks.base import BaseHook

else:
    raise NotImplementedError

__all__ = [
    "BaseHook",
    "AIRFLOW_VERSION_1",
    "AIRFLOW_VERSION_2",
    "AIRFLOW_VERSION_BEFORE_2_2",
]
