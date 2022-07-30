# © Copyright Databand.ai, an IBM Company 2022

from dbnd_airflow.constants import AIRFLOW_ABOVE_10


if AIRFLOW_ABOVE_10:
    from airflow.kubernetes.pod_launcher import PodStatus
else:
    from airflow.contrib.kubernetes.pod_launcher import PodStatus


__all__ = ["PodStatus"]
