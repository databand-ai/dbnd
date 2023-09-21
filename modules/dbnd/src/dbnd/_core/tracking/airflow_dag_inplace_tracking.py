# © Copyright Databand.ai, an IBM Company 2022

"""
Context: airflow operator/task is running a function with @task
Here we create dbnd objects to represent them and send to webserver through tracking api.
"""
import logging
import os
import uuid

from collections.abc import Mapping
from typing import Any, Dict, Iterable, Optional, Tuple
from uuid import UUID

from dbnd._core.configuration.environ_config import (
    DBND_ROOT_RUN_UID,
    ENV_DBND_TRACKING_ATTEMPT_UID,
)
from dbnd._core.constants import UpdateSource
from dbnd._core.current import get_settings
from dbnd._core.log import dbnd_log_debug
from dbnd._core.task.tracking_task import TrackingTask
from dbnd._core.task_build.task_passport import TaskPassport
from dbnd._core.task_build.task_source_code import NO_SOURCE_CODE, TaskSourceCode
from dbnd._core.tracking.airflow_task_context import AirflowTaskContext
from dbnd._core.utils.type_check_utils import is_instance_by_class_name
from dbnd._core.utils.uid_utils import (
    get_job_run_uid,
    get_task_def_uid,
    get_task_run_attempt_uid,
    get_task_run_uid,
    source_md5,
)
from dbnd.providers.spark.spark_airflow_context import (
    try_get_airflow_context_from_spark_conf,
)
from dbnd.utils.helpers import get_callable_name


logger = logging.getLogger(__name__)

DAG_SPECIAL_TASK_ID = "DAG"


def try_get_airflow_context():
    # type: ()-> Optional[AirflowTaskContext]
    # first try to get from spark, then from call stack and then from airflow env
    for func in [try_get_airflow_context_from_spark_conf, try_get_airflow_context_env]:
        try:
            context = func()
            if context:
                dbnd_log_debug(
                    f"Found airflow context via { func.__name__ }  : {context} "
                )
                return context
        except Exception as ex:
            dbnd_log_debug(f"Failed to find context  { func.__name__ }  : {ex} ")


def try_get_airflow_context_env():
    # type: ()-> Optional[AirflowTaskContext]
    # Those env vars are set by airflow before running the operator
    dag_id = os.environ.get("AIRFLOW_CTX_DAG_ID")
    execution_date = os.environ.get("AIRFLOW_CTX_EXECUTION_DATE")
    task_id = os.environ.get("AIRFLOW_CTX_TASK_ID")
    try_number = os.environ.get("AIRFLOW_CTX_TRY_NUMBER")
    airflow_instance_uid = os.environ.get("AIRFLOW_CTX_UID")

    if dag_id and task_id and execution_date:
        return AirflowTaskContext(
            dag_id=dag_id,
            execution_date=execution_date,
            task_id=task_id,
            try_number=int(try_number) if try_number else None,
            airflow_instance_uid=airflow_instance_uid,
        )

    logger.debug(
        "airflow context from env, at least one of those environment var is missing"
        "dag_id: {}, execution_date: {}, task_id: {}".format(
            dag_id, execution_date, task_id
        )
    )
    return None


def build_run_time_airflow_task(
    af_context: AirflowTaskContext, root_task_name: Optional[str]
) -> Tuple[TrackingTask, str, UpdateSource, UUID]:
    if af_context.context:
        # we are in the execute entry point and therefore that task name is <task>__execute
        task_family = af_context.task_id

        airflow_operator = af_context.context["task_instance"].task

        # find the template fields of the operators
        user_params = get_flatten_operator_params(airflow_operator)

        source_code = NO_SOURCE_CODE
        if is_instance_by_class_name(airflow_operator, "PythonOperator"):
            tracked_function = airflow_operator.python_callable
            user_params["function_name"] = get_callable_name(tracked_function)
            source_code = TaskSourceCode.from_callable(tracked_function)
    else:
        # if this is an inline run-time task, we name it after the script which ran it
        # This will enable us to have unique task names (airflow id + internal task id
        task_family = get_task_family_for_inline_script(
            af_context.task_id, root_task_name
        )
        source_code = TaskSourceCode.from_callstack()
        user_params = {}

    user_params.update(
        dag_id=af_context.dag_id,
        execution_date=af_context.execution_date,
        task_version="%s:%s" % (af_context.task_id, af_context.execution_date),
    )

    # just a placeholder name
    task_passport = TaskPassport.from_module(task_family)
    task_definition_uid = get_task_def_uid(
        af_context.dag_id,
        task_family,
        "{}{}".format(
            source_md5(source_code.task_source_code),
            source_md5(source_code.task_module_code),
        ),
    )
    root_task = TrackingTask.for_user_params(
        task_definition_uid=task_definition_uid,
        task_name=task_family,
        task_passport=task_passport,
        source_code=source_code,
        user_params=user_params,
    )  # type: TrackingTask

    root_run_uid = get_job_run_uid(
        airflow_instance_uid=af_context.airflow_instance_uid,
        dag_id=af_context.dag_id,
        execution_date=af_context.execution_date,
    )
    root_task.ctrl.force_task_run_uid = get_task_run_uid(
        run_uid=root_run_uid, dag_id=af_context.dag_id, task_id=task_family
    )

    job_name = af_context.dag_id
    source = UpdateSource.airflow_tracking
    return root_task, job_name, source, root_run_uid


def should_flatten(flatten_config, operator, attr_name):
    for op_name in flatten_config:
        if is_instance_by_class_name(operator, op_name):
            return attr_name in flatten_config[op_name]
    return False


def flatten_param(attr_name, value):
    # type: (str, Any) -> Iterable[Tuple[str, Any]]
    """
    Flatten means we take out the first layer of nested value and split it to elements with relevant name.
    examples:
    >>> flatten_param("name", [0,1, [1,2,3,4]]) == [("name.0", 0),  ("name.1", 1), ("name.2", [1,2,3,4]),]
    >>> flatten_param("other", {"a": 1, "b": {"inner": "value"}}) == [("other.a", 1),  ("other.b", {"inner": "value"})]
    >>> flatten_param("other_name", "1") == [("other_name", "1")]
    """
    if isinstance(value, (dict, Mapping)):
        for name, value in value.items():
            yield ("%s.%s" % (attr_name, name), value)

    elif isinstance(value, list):
        for i, value in enumerate(value):
            yield ("%s.%s" % (attr_name, i), value)

    else:
        yield (attr_name, value)


def get_flatten_operator_params(operator):
    params = []
    flatten_config = get_settings().tracking.flatten_operator_fields

    for attr_name in operator.template_fields:
        value = getattr(operator, attr_name)
        if should_flatten(
            flatten_config=flatten_config, operator=operator, attr_name=attr_name
        ):
            params.extend(flatten_param(attr_name, value))

        else:
            params.append((attr_name, value))

    # we want to make sure that the value is str
    return {name: repr(value) for name, value in params}


def calc_task_key_from_af_ti(ti):
    """
    Creates a key from airflow TaskInstance, for communicating task_run_attempt_uid
    """
    return ":".join([ENV_DBND_TRACKING_ATTEMPT_UID, ti.dag_id, ti.task_id])


def get_task_family_for_inline_script(task_id, root_task_name):
    """
    Calculate task family name for inline script
    :param task_id: task_id
    :param root_task_name: root_task_name
    :return:
    """
    return "_".join([task_id, root_task_name])


def get_task_run_uid_for_inline_script(tracking_env, script_name):
    # type: (Dict[str, str], str) -> (str, UUID, UUID)
    """
    Calculate task run uid and task run attempt uid for inline script execution
    :param tracking_env: dict with airflow context env
    :param script_name: inline script name
    :return:
    """
    task_id = get_task_family_for_inline_script(
        tracking_env["AIRFLOW_CTX_TASK_ID"], script_name
    )
    run_uid = uuid.UUID(tracking_env[DBND_ROOT_RUN_UID])
    dag_id = tracking_env["AIRFLOW_CTX_DAG_ID"]
    task_run_uid = get_task_run_uid(run_uid, dag_id, task_id)
    task_run_attempt_uid = get_task_run_attempt_uid(
        run_uid, dag_id, task_id, tracking_env["AIRFLOW_CTX_TRY_NUMBER"]
    )
    return task_id, task_run_uid, task_run_attempt_uid
