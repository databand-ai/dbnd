from dbnd_airflow.contants import AIRFLOW_BELOW_2


if AIRFLOW_BELOW_2:
    from airflow.contrib.kubernetes.secret import Secret
else:
    from airflow.kubernetes.secret import Secret
