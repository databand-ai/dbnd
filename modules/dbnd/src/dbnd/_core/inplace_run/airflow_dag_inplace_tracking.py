"""
Context: airflow operator/task is running a function with @task
Here we create dbnd objects to represent them and send to webserver through tracking api.
"""
import datetime
import logging
import os
import sys

from collections import Mapping
from typing import Any, Dict, Iterable, Optional, Tuple

import attr

import dbnd._core.utils.basics.environ_utils

from dbnd._core.configuration import environ_config
from dbnd._core.configuration.environ_config import (
    _debug_init_print,
    spark_tracking_enabled,
)
from dbnd._core.constants import UpdateSource
from dbnd._core.current import get_settings
from dbnd._core.errors.errors_utils import UserCodeDetector
from dbnd._core.parameter.parameter_builder import parameter
from dbnd._core.task.task import Task
from dbnd._core.task_build.dynamic import build_dynamic_task_class
from dbnd._core.utils.airflow_cmd_utils import generate_airflow_cmd
from dbnd._core.utils.seven import import_errors
from dbnd._core.utils.type_check_utils import is_instance_by_class_name


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

    if dag_id and task_id and execution_date:
        return AirflowTaskContext(
            dag_id=dag_id,
            execution_date=execution_date,
            task_id=task_id,
            try_number=int(try_number) if try_number else None,
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

        if dag_id and task_id and execution_date:
            return AirflowTaskContext(
                dag_id=dag_id,
                execution_date=execution_date,
                task_id=task_id,
                try_number=try_number,
            )
    except Exception as ex:
        logger.info("Failed to get airflow context info from spark job: %s", ex)

    return None


class AirflowOperatorRuntimeTask(Task):
    task_is_system = False
    _conf__track_source_code = False

    dag_id = parameter[str]
    execution_date = parameter[datetime.datetime]

    def _initialize(self):
        super(AirflowOperatorRuntimeTask, self)._initialize()
        self.task_meta.task_functional_call = ""

        self.task_meta.task_command_line = generate_airflow_cmd(
            dag_id=self.dag_id,
            task_id=self.task_id,
            execution_date=self.execution_date,
            is_root_task=False,
        )


def build_run_time_airflow_task(af_context):
    # type: (AirflowTaskContext) -> (AirflowOperatorRuntimeTask, str, UpdateSource)
    if af_context.context:
        # we are in the execute entry point and therefore that task name is <task>__execute
        task_family = "%s__execute" % af_context.task_id

        airflow_operator = af_context.context["task_instance"].task

        tracked_function = None
        if is_instance_by_class_name(airflow_operator, "PythonOperator"):
            tracked_function = airflow_operator.python_callable

        # find the template fields of the operators
        user_params = get_flatten_operator_params(airflow_operator)
        if tracked_function:
            user_params["function_name"] = tracked_function.__name__

        # create params definitions for the operator's fields
        params_def = {
            name: parameter[type(value)].build_parameter("inline")
            for name, value in user_params.items()
        }

        task_class = build_dynamic_task_class(
            AirflowOperatorRuntimeTask, task_family, params_def,
        )

        try:
            task_class.airflow_log_file = get_airflow_logger_file(af_context.context)
        except Exception:
            logger.warning(
                "couldn't get the airflow's log_file for task {}".format(task_family)
            )

        if tracked_function:
            import inspect

            task_class.task_definition.task_source_code = inspect.getsource(
                tracked_function
            )
            task_class.task_definition.task_module_code = inspect.getsource(
                inspect.getmodule(tracked_function)
            )

    else:
        # if this is an inline run-time task, we name it after the script which ran it
        task_family = sys.argv[0].split(os.path.sep)[-1]
        task_class = build_dynamic_task_class(AirflowOperatorRuntimeTask, task_family)
        module_code = get_user_module_code()
        task_class.task_definition.task_module_code = module_code
        task_class.task_definition.task_source_code = module_code
        user_params = {}

    root_task = task_class(
        task_name=task_family,
        dag_id=af_context.dag_id,
        execution_date=af_context.execution_date,
        task_version="%s:%s" % (af_context.task_id, af_context.execution_date),
        **user_params
    )

    job_name = "{}.{}".format(af_context.dag_id, af_context.task_id)
    source = UpdateSource.airflow_tracking
    return root_task, job_name, source


def get_user_module_code():
    try:
        user_frame = UserCodeDetector.build_code_detector().find_user_side_frame(
            user_side_only=True
        )
        if user_frame:
            module_code = open(user_frame.filename).read()
            return module_code
    except Exception as ex:
        return


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


def get_airflow_logger_file(context):
    log_path = context["task_instance"].log_filepath
    try_number = str(context["task_instance"].try_number)
    path, extension = os.path.splitext(log_path)
    file_name = try_number + extension
    return os.path.join(path, file_name)
