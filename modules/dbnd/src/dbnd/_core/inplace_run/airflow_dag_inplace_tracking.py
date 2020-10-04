"""
Context: airflow operator/task is running a function with @task
Here we create dbnd objects to represent them and send to webserver through tracking api.
"""
import datetime
import logging
import operator
import os

from collections import Callable
from types import FrameType
from typing import Any, Dict, List, Optional, Tuple

import attr

import dbnd._core.utils.basics.environ_utils

from dbnd._core.configuration import environ_config
from dbnd._core.configuration.environ_config import (
    _debug_init_print,
    spark_tracking_enabled,
)
from dbnd._core.parameter.parameter_builder import parameter
from dbnd._core.task.task import Task
from dbnd._core.utils.airflow_cmd_utils import generate_airflow_cmd
from dbnd._core.utils.seven import import_errors


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


# after py35 inspect.stack() return a numbed tuple FrameInfo, and before that it is just a tuple
# those functions save backward compatibility and still understandable
FrameInfo = Tuple[FrameType, str, int, str, Optional[List[str]], Optional[int]]
get_frame = operator.itemgetter(0)  # type: Callable[[FrameInfo], FrameType]
get_filename = operator.itemgetter(1)  # type: Callable[[FrameInfo], str]
get_function = operator.itemgetter(3)  # type: Callable[[FrameInfo], str]


def get_frame_info(file_name, func_name):
    # type: (str, str) -> Optional[FrameInfo]
    """Find the first frame in the stack by file name and function name"""
    from more_itertools import first
    import inspect

    return first(
        filter(
            lambda frame: file_name in get_filename(frame).lower()
            and get_function(frame) == func_name,
            inspect.stack(),
        ),
        None,
    )


def try_get_airflow_context_inspect():
    # type: () -> Optional[AirflowTaskContext]
    """Trying to extract the airflow_context using `inspect` to get the frame in which the context sent to `execute`"""
    frame_info = get_frame_info(file_name="taskinstance", func_name="_run_raw_task")
    if frame_info is None:
        return None

    airflow_context = get_frame(frame_info).f_locals.get("context")
    if airflow_context is None:
        return None

    return extract_airflow_context(airflow_context)


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
            try_get_airflow_context_inspect,
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
            try_number=try_number,
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
        logger.info("Failed to get airlfow context info from spark job: %s", ex)

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

    @classmethod
    def build_from_airflow_context(self, af_context):
        # we can't actually run it, we even don't know when it's going to finish
        # current execution is inside the operator, this is the only thing we know

        # AIRFLOW OPERATOR RUNTIME
        return AirflowOperatorRuntimeTask(
            task_family="%s__execute" % (af_context.task_id),
            dag_id=af_context.dag_id,
            execution_date=af_context.execution_date,
            task_version="%s:%s" % (af_context.task_id, af_context.execution_date),
        )
