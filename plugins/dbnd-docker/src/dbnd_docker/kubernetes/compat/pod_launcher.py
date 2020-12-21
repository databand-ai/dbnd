from dbnd_airflow.constants import AIRFLOW_ABOVE_10


if AIRFLOW_ABOVE_10:
    from airflow.kubernetes.pod_launcher import PodStatus as PodStatus
else:
    from airflow.contrib.kubernetes.pod_launcher import PodStatus as PodStatus
