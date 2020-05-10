"""
Context: airflow operator/task is running a function with @task
Here we create dbnd objects to represent them and send to webserver through tracking api.
"""
import datetime
import logging
import os

from typing import Optional

import attr

from dbnd._core.configuration import environ_config
from dbnd._core.parameter.parameter_builder import parameter
from dbnd._core.task.task import Task
from dbnd._core.utils.airflow_cmd_utils import generate_airflow_cmd
from dbnd._core.utils.basics.memoized import cached
from dbnd._core.utils.seven import import_errors


logger = logging.getLogger(__name__)
_SPARK_ENV_FLAG = "SPARK_ENV_LOADED"  # if set, we are in spark

DAG_SPECIAL_TASK_ID = "DAG"


def override_airflow_log_system_for_tracking():
    return environ_config.environ_enabled(
        environ_config.ENV_DBND__OVERRIDE_AIRFLOW_LOG_SYSTEM_FOR_TRACKING
    )


def disable_airflow_subdag_tracking():
    return environ_config.environ_enabled(
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


def _get_try_number():
    import inspect

    is_python_operator_exec = list(
        filter(
            lambda frame: "python_operator" in frame.filename.lower()
            and frame.function == "execute",
            inspect.stack(),
        )
    )

    if is_python_operator_exec:
        # We are in python operator execution flow
        # let's get real airflow context from stack trace
        try:
            frame = is_python_operator_exec[0].frame
            if "context" in frame.f_locals and "ti" in frame.f_locals["context"]:
                return frame.f_locals["context"]["ti"].try_number
            else:
                raise Exception("Could not find airflow context inside PythonOperator.")
        except Exception as e:
            logging.error("Could not get try number from airflow context")
            raise e

    else:
        # We are in bash operator execution flow
        # TODO: generate try_number mechanism for bash operator
        try_number = os.environ.get("AIRFLOW_CTX_TRY_NUMBER")
        return try_number


def try_get_airflow_context():
    # type: ()-> Optional[AirflowTaskContext]
    # first try to get from spark
    try:
        from_spark = try_get_airflow_context_from_spark_conf()
        if from_spark:
            return from_spark

        # Those env vars are set by airflow before running the operator
        dag_id = os.environ.get("AIRFLOW_CTX_DAG_ID")
        execution_date = os.environ.get("AIRFLOW_CTX_EXECUTION_DATE")
        task_id = os.environ.get("AIRFLOW_CTX_TASK_ID")
        try:
            try_number = _get_try_number()
        except Exception:
            try_number = None

        if dag_id and task_id and execution_date:
            return AirflowTaskContext(
                dag_id=dag_id,
                execution_date=execution_date,
                task_id=task_id,
                try_number=try_number,
            )
        return None
    except Exception:
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
    if (
        not environ_config.environ_enabled("DBND__ENABLE__SPARK_CONTEXT_ENV")
        or _SPARK_ENV_FLAG not in os.environ
    ):
        return None

    if not _is_dbnd_spark_installed():
        return None
    try:
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
