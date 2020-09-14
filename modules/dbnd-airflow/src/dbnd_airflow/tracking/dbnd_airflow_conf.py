from dbnd._core.settings import CoreConfig


def get_airflow_conf(
    dag_id="{{dag.dag_id}}",
    task_id="{{task.task_id}}",
    execution_date="{{ts}}",
    try_number="{{task_instance._try_number}}",
):
    """
    These properties are
        AIRFLOW_CTX_DAG_ID - name of the Airflow DAG to associate a run with
        AIRFLOW_CTX_EXECUTION_DATE - execution_date to associate a run with
        AIRFLOW_CTX_TASK_ID - name of the Airflow Task to associate a run with
        AIRFLOW_CTX_TRY_NUMBER - try number of the Airflow Task to associate a run with
    """
    airflow_conf = [
        ("AIRFLOW_CTX_DAG_ID", dag_id),
        ("AIRFLOW_CTX_EXECUTION_DATE", execution_date),
        ("AIRFLOW_CTX_TASK_ID", task_id),
        ("AIRFLOW_CTX_TRY_NUMBER", try_number),
    ]

    databand_url = _get_databand_url()
    if databand_url:
        return airflow_conf + [("DBND__CORE__DATABAND_URL", databand_url)]

    return airflow_conf


def _get_databand_url():
    try:
        return CoreConfig().databand_external_url
    except Exception:
        pass
