import logging


logger = logging.getLogger("dbnd-scheduler")


try:
    from dbnd_airflow.scheduler.scheduler_dags_provider import get_dags

    # airflow will only scan files containing the text DAG or airflow. This comment performs this function
    dags = get_dags()
    if dags:
        for dag in dags:
            globals()[dag.dag_id] = dag
except Exception as e:
    logging.exception("Failed to get dags form databand server")
    raise e
