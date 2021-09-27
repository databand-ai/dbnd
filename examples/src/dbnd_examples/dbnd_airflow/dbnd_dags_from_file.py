from dbnd_airflow.scheduler.dags_provider_from_file import get_dags_from_file


# airflow will only scan files containing the text DAG or airflow. This comment performs this function

dags = get_dags_from_file()
if dags:
    globals().update(dags)
