import datetime
import uuid

import pytz
import six

from dbnd._vendor import pendulum


def get_uuid():
    # TODO: obfuscate getnode() - mac address part
    return uuid.uuid1()


NAMESPACE_DBND = uuid.uuid5(uuid.NAMESPACE_DNS, "databand.ai")
NAMESPACE_DBND_JOB = uuid.uuid5(NAMESPACE_DBND, "job")
NAMESPACE_DBND_RUN = uuid.uuid5(NAMESPACE_DBND, "run")
NAMESPACE_DBND_TASK_DEF = uuid.uuid5(NAMESPACE_DBND, "task_definition")


def get_task_def_uid(dag_id, task_id):
    return uuid.uuid5(NAMESPACE_DBND_TASK_DEF, "{}.{}".format(dag_id, task_id))


def get_task_run_uid(run_uid, dag_id, task_id):
    return uuid.uuid5(run_uid, "{}.{}".format(dag_id, task_id))


def get_task_run_attempt_uid(run_uid, dag_id, task_id, try_number):
    return uuid.uuid5(run_uid, "{}.{}:{}".format(dag_id, task_id, try_number))


def get_job_run_uid(dag_id, execution_date):
    if isinstance(execution_date, six.string_types):
        execution_date = pendulum.parse(execution_date)
    if isinstance(execution_date, datetime.datetime):
        execution_date = (
            execution_date.replace(microsecond=0).astimezone(pytz.utc).isoformat()
        )
    return uuid.uuid5(NAMESPACE_DBND_RUN, "{}:{}".format(dag_id, execution_date))


def get_job_uid(dag_id):
    return uuid.uuid5(NAMESPACE_DBND_JOB, dag_id)
