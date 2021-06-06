import datetime
import uuid

import pytz
import six

from dbnd._core.configuration.dbnd_config import config
from dbnd._core.utils.basics.memoized import cached
from dbnd._vendor import pendulum


def get_uuid():
    # TODO: obfuscate getnode() - mac address part
    return uuid.uuid1()


NAMESPACE_DBND = uuid.uuid5(uuid.NAMESPACE_DNS, "databand.ai")
NAMESPACE_DBND_JOB = uuid.uuid5(NAMESPACE_DBND, "job")
NAMESPACE_DBND_RUN = uuid.uuid5(NAMESPACE_DBND, "run")
NAMESPACE_DBND_TASK_DEF = uuid.uuid5(NAMESPACE_DBND, "task_definition")


def get_task_def_uid(dag_id, task_id, code_hash):
    return uuid.uuid5(
        NAMESPACE_DBND_TASK_DEF, "{}.{}.{}".format(dag_id, task_id, code_hash)
    )


def get_task_run_uid(run_uid, dag_id, task_id):
    return uuid.uuid5(run_uid, "{}.{}".format(dag_id, task_id))


def get_task_run_attempt_uid(run_uid, dag_id, task_id, try_number):
    return uuid.uuid5(run_uid, "{}.{}:{}".format(dag_id, task_id, try_number))


def get_job_run_uid(airflow_instance_uid, dag_id, execution_date):
    # TODO_CORE: change to source_instance_uid
    if isinstance(execution_date, six.string_types):
        execution_date = pendulum.parse(execution_date)
    if isinstance(execution_date, datetime.datetime):
        # Temporary fix for existing databases with uids without microseconds
        algo_threshold = config.get("webserver", "run_uid_execution_date_threshold")
        if algo_threshold and execution_date <= pendulum.parse(algo_threshold):
            execution_date = execution_date.replace(microsecond=0)
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
    """ used to distinguish between jobs of different airflow instances """
    import airflow

    db_url = airflow.settings.Session.bind.engine.url
    db_str = "{}:{}/{}".format(db_url.host, db_url.port, db_url.database)
    airflow_instance_uid = uuid.uuid5(uuid.NAMESPACE_URL, db_str)
    return str(airflow_instance_uid)


def get_dataset_op_uid(dataset_uid, task_run_attempt_id, operation_type):
    return uuid.uuid5(dataset_uid, "{}.{}".format(task_run_attempt_id, operation_type))
