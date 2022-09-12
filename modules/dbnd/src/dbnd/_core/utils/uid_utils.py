# © Copyright Databand.ai, an IBM Company 2022

import datetime
import hashlib
import uuid

import pytz
import six

from dbnd._core.configuration.environ_config import get_dbnd_project_config
from dbnd._core.utils.basics.memoized import cached
from dbnd._vendor import pendulum


def source_md5(source_code):
    if source_code:
        try:
            return hashlib.md5(source_code.encode("utf-8")).hexdigest()  # nosec B324
        except UnicodeDecodeError:
            return hashlib.md5(source_code).hexdigest()  # nosec B324


def get_uuid():
    # TODO: obfuscate getnode() - mac address part
    return uuid.uuid1()


NAMESPACE_DBND = uuid.uuid5(uuid.NAMESPACE_DNS, "databand.ai")
NAMESPACE_DBND_JOB = uuid.uuid5(NAMESPACE_DBND, "job")
NAMESPACE_DBND_RUN = uuid.uuid5(NAMESPACE_DBND, "run")
NAMESPACE_DBND_TASK_DEF = uuid.uuid5(NAMESPACE_DBND, "task_definition")


def get_stable_uid(payload: str, namespace=uuid.NAMESPACE_DNS):
    return uuid.uuid5(namespace, payload)


def get_task_def_uid(dag_id, task_id, code_hash):
    return uuid.uuid5(
        NAMESPACE_DBND_TASK_DEF, "{}.{}.{}".format(dag_id, task_id, code_hash)
    )


def get_task_run_uid(run_uid, dag_id, task_id):
    return uuid.uuid5(run_uid, "{}.{}".format(dag_id, task_id))


def get_task_run_attempt_uid(run_uid, dag_id, task_id, try_number):
    return uuid.uuid5(run_uid, "{}.{}:{}".format(dag_id, task_id, try_number))


def get_task_run_attempt_uid_for_subrun(task_run):
    return uuid.uuid5(
        task_run.run.run_uid,
        "{}.{}:{}-subrun".format(
            task_run.run.dag_id, task_run.task_af_id, task_run.attempt_number
        ),
    )


def get_task_run_attempt_uid_by_task_run(task_run):
    is_sub_rub = get_dbnd_project_config().resubmit_run
    if is_sub_rub:
        return get_task_run_attempt_uid_for_subrun(task_run)

    return get_task_run_attempt_uid(
        task_run.run.run_uid,
        task_run.run.dag_id,
        task_run.task_af_id,
        task_run.attempt_number,
    )


def get_task_run_attempt_uid_from_af_ti(ti):
    airflow_instance_uid = get_airflow_instance_uid()
    run_uid = get_job_run_uid(
        airflow_instance_uid=airflow_instance_uid,
        dag_id=ti.dag_id,
        execution_date=ti.execution_date,
    )
    return get_task_run_attempt_uid(run_uid, ti.dag_id, ti.task_id, ti.try_number)


def get_job_run_uid(airflow_instance_uid, dag_id, execution_date):
    # TODO_CORE: change to source_instance_uid
    if isinstance(execution_date, six.string_types):
        execution_date = pendulum.parse(execution_date)
    if isinstance(execution_date, datetime.datetime):
        execution_date = execution_date.astimezone(pytz.utc).isoformat()
    if airflow_instance_uid is None:
        return uuid.uuid5(NAMESPACE_DBND_RUN, "{}:{}".format(dag_id, execution_date))
    else:
        return uuid.uuid5(
            NAMESPACE_DBND_RUN,
            "{}:{}:{}".format(airflow_instance_uid, dag_id, execution_date),
        )


def get_job_uid(airflow_server_info_uid, dag_id):
    # TODO_CORE: change to source_instance_uid

    if airflow_server_info_uid:
        return uuid.uuid5(
            NAMESPACE_DBND_JOB, "{}:{}".format(airflow_server_info_uid, dag_id)
        )
    else:
        return uuid.uuid5(NAMESPACE_DBND_JOB, dag_id)


@cached()
def get_airflow_instance_uid():
    """used to distinguish between jobs of different airflow instances"""
    import airflow

    db_url = airflow.settings.Session.bind.engine.url
    db_str = "{}:{}/{}".format(db_url.host, db_url.port, db_url.database)
    airflow_instance_uid = uuid.uuid5(uuid.NAMESPACE_URL, db_str)
    return str(airflow_instance_uid)


def get_dataset_op_uid(dataset_uid, task_run_attempt_id, operation_type):
    return uuid.uuid5(dataset_uid, "{}.{}".format(task_run_attempt_id, operation_type))
