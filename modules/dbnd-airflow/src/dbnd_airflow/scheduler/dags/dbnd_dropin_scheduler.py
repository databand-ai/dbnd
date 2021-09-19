import logging

from dbnd_airflow.scheduler.dags_provider_from_databand import get_dags_from_databand


logger = logging.getLogger("dbnd-scheduler")


# airflow will only scan files containing the text DAG or airflow. This comment performs this function
dags = get_dags_from_databand()
if dags:
    globals().update(dags)
