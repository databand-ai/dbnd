from dbnd_airflow.contants import AIRFLOW_BELOW_2


if AIRFLOW_BELOW_2:
    from airflow.contrib import kubernetes
else:
    from airflow import kubernetes
