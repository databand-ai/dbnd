"""
Context: airflow operator/task is running a function with @task
Here we create dbnd objects to represent them and send to webserver through tracking api.
"""
import logging
import os

from collections import Mapping
from typing import Any, Dict, Iterable, Optional, Tuple
from uuid import UUID

import attr

import dbnd._core.utils.basics.environ_utils

from dbnd._core.configuration import environ_config
from dbnd._core.configuration.environ_config import (
    ENV_DBND_TRACKING_ATTEMPT_UID,
    _debug_init_print,
    spark_tracking_enabled,
)
from dbnd._core.constants import UpdateSource
from dbnd._core.current import get_settings
from dbnd._core.task.tracking_task import TrackingTask
from dbnd._core.task_build.task_passport import TaskPassport
from dbnd._core.task_build.task_source_code import NO_SOURCE_CODE, TaskSourceCode
from dbnd._core.utils.airflow_cmd_utils import generate_airflow_cmd
from dbnd._core.utils.seven import import_errors
from dbnd._core.utils.type_check_utils import is_instance_by_class_name
from dbnd._core.utils.uid_utils import (
    get_airflow_instance_uid,
    get_job_run_uid,
    get_task_def_uid,
    get_task_run_uid,
    source_md5,
)


logger = logging.getLogger(__name__)
_SPARK_ENV_FLAG = "SPARK_ENV_LOADED"  # if set, we are in spark

DAG_SPECIAL_TASK_ID = "DAG"


def override_airflow_log_system_for_tracking():
    return dbnd._core.utils.basics.environ_utils.environ_enabled(
        environ_config.ENV_DBND__OVERRIDE_AIRFLOW_LOG_SYSTEM_FOR_TRACKING
    )


def disable_airflow_subdag_tracking():
    return dbnd._core.utils.basics.environ_utils.environ_enabled(
        environ_config.ENV_DBND__DISABLE_AIRFLOW_SUBDAG_TRACKING, False
    )


@attr.s
class AirflowTaskContext(object):
    dag_id = attr.ib()  # type: str
    execution_date = attr.ib()  # type: str
    task_id = attr.ib()  # type: str
    try_number = attr.ib(default=1)
    context = attr.ib(default=None)  # type: Optional[dict]
    airflow_instance_uid = attr.ib(default=None)  # type: Optional[UUID]

    root_dag_id = attr.ib(init=False, repr=False)  # type: str
    is_subdag = attr.ib(init=False, repr=False)  # type: bool

    def __attrs_post_init__(self):
        self.is_subdag = "." in self.dag_id and not disable_airflow_subdag_tracking()
        if self.is_subdag:
            dag_breadcrumb = self.dag_id.split(".", 1)
            self.root_dag_id = dag_breadcrumb[0]
            self.parent_dags = []
            # for subdag "root_dag.subdag1.subdag2.subdag3" it should return
            # root_dag, root_dag.subdag1, root_dag.subdag1.subdag2
            # WITHOUT the dag_id itself!
            cur_name = []
            for name_part in dag_breadcrumb[:-1]:
                cur_name.append(name_part)
                self.parent_dags.append(".".join(cur_name))
        else:
            self.root_dag_id = self.dag_id
            self.parent_dags = []


def extract_airflow_context(airflow_context):
    # type: (Dict[str, Any]) -> Optional[AirflowTaskContext]
    """Create AirflowTaskContext for airflow_context dict"""

    task_instance = airflow_context.get("task_instance")
    if task_instance is None:
        return None

    dag_id = task_instance.dag_id
    task_id = task_instance.task_id
    execution_date = str(task_instance.execution_date)
    try_number = task_instance.try_number

    if dag_id and task_id and execution_date:
        return AirflowTaskContext(
            dag_id=dag_id,
            execution_date=execution_date,
            task_id=task_id,
            try_number=try_number,
            context=airflow_context,
            airflow_instance_uid=get_airflow_instance_uid(),
        )

    logger.debug(
        "airflow context from inspect, at least one of those params is missing"
        "dag_id: {}, execution_date: {}, task_id: {}".format(
            dag_id, execution_date, task_id
        )
    )
    return None


def try_get_airflow_context():
    # type: ()-> Optional[AirflowTaskContext]
    # first try to get from spark, then from call stack and then from airflow env
    try:
        for func in [
            try_get_airflow_context_from_spark_conf,
            try_get_airflow_context_env,
        ]:
            context = func()
            if context:
                return context
            else:
                msg = func.__name__.replace("_", " ").replace("try", "couldn't")
                logger.debug(msg)

    except Exception:
        return None


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


_IS_SPARK_INSTALLED = None


def _is_dbnd_spark_installed():
    global _IS_SPARK_INSTALLED
    if _IS_SPARK_INSTALLED is not None:
        return _IS_SPARK_INSTALLED
    try:
        try:
            from pyspark import SparkContext

            from dbnd_spark import dbnd_spark_bootstrap

            dbnd_spark_bootstrap()
        except import_errors:
            _IS_SPARK_INSTALLED = False
        else:
            _IS_SPARK_INSTALLED = True
    except Exception:
        # safeguard, on any exception
        _IS_SPARK_INSTALLED = False

    return _IS_SPARK_INSTALLED


def try_get_airflow_context_from_spark_conf():
    # type: ()-> Optional[AirflowTaskContext]
    if not spark_tracking_enabled() or _SPARK_ENV_FLAG not in os.environ:
        _debug_init_print(
            "DBND__ENABLE__SPARK_CONTEXT_ENV or SPARK_ENV_LOADED are not set"
        )
        return None

    if not _is_dbnd_spark_installed():
        _debug_init_print("failed to import pyspark or dbnd-spark")
        return None
    try:
        _debug_init_print("creating spark context to get spark conf")
        from pyspark import SparkContext

        conf = SparkContext.getOrCreate().getConf()

        dag_id = conf.get("spark.env.AIRFLOW_CTX_DAG_ID")
        execution_date = conf.get("spark.env.AIRFLOW_CTX_EXECUTION_DATE")
        task_id = conf.get("spark.env.AIRFLOW_CTX_TASK_ID")
        try_number = conf.get("spark.env.AIRFLOW_CTX_TRY_NUMBER")
        airflow_instance_uid = conf.get("spark.env.AIRFLOW_CTX_UID")

        if dag_id and task_id and execution_date:
            return AirflowTaskContext(
                dag_id=dag_id,
                execution_date=execution_date,
                task_id=task_id,
                try_number=try_number,
                airflow_instance_uid=airflow_instance_uid,
            )
    except Exception as ex:
        logger.info("Failed to get airflow context info from spark job: %s", ex)

    return None


def build_run_time_airflow_task(af_context, root_task_name):
    # type: (AirflowTaskContext, Optional[str]) -> Tuple[TrackingTask, str, UpdateSource, UUID]
    if af_context.context:
        # we are in the execute entry point and therefore that task name is <task>__execute
        task_family = af_context.task_id

        airflow_operator = af_context.context["task_instance"].task

        # find the template fields of the operators
        user_params = get_flatten_operator_params(airflow_operator)

        source_code = NO_SOURCE_CODE
        if is_instance_by_class_name(airflow_operator, "PythonOperator"):
            tracked_function = airflow_operator.python_callable
            user_params["function_name"] = tracked_function.__name__
            source_code = TaskSourceCode.from_callable(tracked_function)
    else:
        # if this is an inline run-time task, we name it after the script which ran it
        task_family = "_".join([af_context.task_id, root_task_name])
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

    root_task.ctrl.task_repr.task_functional_call = ""
    root_task.ctrl.task_repr.task_command_line = generate_airflow_cmd(
        dag_id=af_context.dag_id,
        task_id=af_context.task_id,
        execution_date=af_context.execution_date,
        is_root_task=False,
    )

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


def should_flatten(operator, attr_name):
    flatten_config = get_settings().tracking.flatten_operator_fields
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
    for attr_name in operator.template_fields:
        value = getattr(operator, attr_name)
        if should_flatten(operator, attr_name):
            params.extend(flatten_param(attr_name, value))

        else:
            params.append((attr_name, value))

    # we want to make sure that the value is str
    return {name: repr(value) for name, value in params}


def calc_task_run_attempt_key_from_af_ti(ti):
    """
    Creates a key from airflow TaskInstance, for communicating task_run_attempt_uid
    """
    return ":".join([ENV_DBND_TRACKING_ATTEMPT_UID, ti.dag_id, ti.task_id])


def calc_task_run_attempt_key_from_dbnd_task(dag_id, task_family):
    """
    Creates a key from dbnd Task, for communicating task_run_attempt_uid
    """
    return ":".join([ENV_DBND_TRACKING_ATTEMPT_UID, dag_id, task_family])


def try_pop_attempt_id_from_env(task):
    """
    if the task is an airflow execute task we try to pop the attempt id from environ
    """
    if task.task_params.get_param_value("dag_id"):
        key = calc_task_run_attempt_key_from_dbnd_task(
            task.task_params.get_value("dag_id"), task.task_family
        )
        attempt_id = os.environ.get(key, None)
        if attempt_id:
            del os.environ[key]
            attempt_id = UUID(attempt_id)

        return attempt_id
