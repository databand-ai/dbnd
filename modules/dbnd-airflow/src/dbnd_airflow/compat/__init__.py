from dbnd_airflow.constants import AIRFLOW_VERSION_1, AIRFLOW_VERSION_2


if AIRFLOW_VERSION_1:
    from airflow.hooks.base_hook import BaseHook

elif AIRFLOW_VERSION_2:
    from airflow.hooks.base import BaseHook

else:
    raise NotImplementedError

__all__ = [
    "BaseHook",
]
