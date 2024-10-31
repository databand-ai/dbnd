# © Copyright Databand.ai, an IBM Company 2022

from uuid import NAMESPACE_URL, uuid5

from dbnd._core.utils.uid_utils import get_job_run_uid, get_task_run_attempt_uid


AIRFLOW_INSTANCE_UUID_VAR_NAME = "DBND_AIRFLOW_INSTANCE_UUID"


def get_task_run_attempt_uid_from_af_ti(ti):
    airflow_instance_uid = get_or_create_airflow_instance_uid()
    run_uid = get_job_run_uid(
        airflow_instance_uid=airflow_instance_uid,
        dag_id=ti.dag_id,
        execution_date=ti.execution_date,
    )
    return get_task_run_attempt_uid(run_uid, ti.dag_id, ti.task_id, ti.try_number)


def get_or_create_airflow_instance_uid() -> str:
    """
    Used to distinguish between jobs of different airflow instances.
    airflow_instance_uid is stored in "DBND_AIRFLOW_INSTANCE_UUID" Airflow Variable.
    It's generated from DB connection string for backwards compatibility.
    """
    import airflow  # pylint: disable=import-error

    from airflow.models import Variable  # pylint: disable=import-error

    airflow_instance_uid = Variable.get(
        AIRFLOW_INSTANCE_UUID_VAR_NAME, default_var=None
    )
    if airflow_instance_uid:
        return airflow_instance_uid

    db_url = airflow.settings.Session.bind.engine.url
    db_str = f"{db_url.host}:{db_url.port}/{db_url.database}"
    new_airflow_instance_uid = str(uuid5(NAMESPACE_URL, db_str))

    Variable.set(AIRFLOW_INSTANCE_UUID_VAR_NAME, new_airflow_instance_uid)
    return new_airflow_instance_uid
