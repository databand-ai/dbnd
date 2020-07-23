import logging
import sys

from types import FunctionType, ModuleType

from dbnd import dbnd_run_start, dbnd_run_stop, get_dbnd_project_config, task
from dbnd._core.decorator.func_task_decorator import _decorated_user_func
from dbnd._core.settings import CoreConfig
from dbnd_airflow.tracking.dbnd_spark_conf import (
    dbnd_wrap_spark_environment,
    get_dbnd_tracking_spark_conf_dict,
    spark_submit_with_dbnd_tracking,
)


logger = logging.getLogger(__name__)


def track_emr_add_steps_operator(operator):
    for step in operator.steps:
        args = step["HadoopJarStep"]["Args"]
        if args and "spark-submit" in args[0]:
            step["HadoopJarStep"]["Args"] = spark_submit_with_dbnd_tracking(args)


def track_databricks_submit_run_operator(operator):
    config = operator.json
    # passing env variables is only supported in new clusters
    if "new_cluster" in config:
        cluster = config["new_cluster"]
        if "spark_env_vars" not in cluster:
            cluster["spark_env_vars"] = {}
        spark_vars = cluster["spark_env_vars"]
        spark_vars["AIRFLOW_CTX_DAG_ID"] = "{{ dag.dag_id }}"
        spark_vars["AIRFLOW_CTX_EXECUTION_DATE"] = "{{ ts }}"
        spark_vars["AIRFLOW_CTX_TASK_ID"] = "{{ task.task_id }}"
        spark_vars["AIRFLOW_CTX_TRY_NUMBER"] = "{{ task_instance._try_number }}"
        try:
            if CoreConfig().databand_external_url:
                spark_vars[
                    "DBND__CORE__DATABAND_URL"
                ] = CoreConfig().databand_external_url
        except Exception:
            pass


def track_data_proc_pyspark_operator(operator):
    if operator.dataproc_properties is None:
        operator.dataproc_properties = dict()
    dbnd_conf = get_dbnd_tracking_spark_conf_dict()
    operator.dataproc_properties.update(dbnd_conf)


def track_spark_submit_operator(operator):
    if operator._conf is None:
        operator._conf = dict()
    dbnd_conf = get_dbnd_tracking_spark_conf_dict()
    operator._conf.update(dbnd_conf)

    if operator._env_vars is None:
        operator._env_vars = dict()
    dbnd_env_vars = dbnd_wrap_spark_environment()
    operator._env_vars.update(dbnd_env_vars)


def is_instance_by_class_name(obj, class_name):
    for cls in obj.__class__.mro():
        if cls.__name__ == class_name:
            return True
    return False


def track_python_operator(operator):
    if isinstance(operator.python_callable, _decorated_user_func):
        # function already has @task
        return

    operator.python_callable = task(operator.python_callable)


def _track_task(task):
    if is_instance_by_class_name(task, "SubDagOperator"):
        track_dag(task.subdag)
    elif is_instance_by_class_name(task, "EmrAddStepsOperator"):
        track_emr_add_steps_operator(task)
    elif is_instance_by_class_name(task, "DatabricksSubmitRunOperator"):
        track_databricks_submit_run_operator(task)
    elif is_instance_by_class_name(task, "DataProcPySparkOperator"):
        track_data_proc_pyspark_operator(task)
    elif is_instance_by_class_name(task, "SparkSubmitOperator"):
        track_spark_submit_operator(task)
    elif is_instance_by_class_name(task, "PythonOperator"):
        track_python_operator(task)


def _is_verbose():
    config = get_dbnd_project_config()
    return config.is_verbose()


def track_task(task):
    try:
        _track_task(task)
    except Exception:
        if _is_verbose():
            logger.exception("Failed to modify %s for tracking" % task.task_id)
        else:
            logger.info("Failed to modify %s for tracking" % task.task_id)


def track_dag(dag):
    """
    Modify operators in dag if necessary so we could track them.
    Supported operators: EmrAddStepsOperator, DataProcPySparkOperator, SparkSubmitOperator, PythonOperator.
    Other operators are not modified.

    More details on each operator:
    - EmrAddStepsOperator: modify args of spark-submit steps by adding dbnd variables using --conf
    - DataProcPySparkOperator: add dbnd variables to dataproc properties
    - SparkSubmitOperator: add dbnd variables to spark-submit command using --conf
    - PythonOperator: wrap python function with @task
    """
    try:
        for task in dag.tasks:
            track_task(task)
    except Exception:
        logger.exception("Failed to modify %s for tracking" % dag.dag_id)


def _is_function(obj):
    return isinstance(obj, FunctionType)


def _track_function(function):
    if not _is_function(function):
        return

    decorated_function = task(function)

    # We modify all modules since each module has its own pointers to local and imported functions.
    # If a module has already imported the function we need to change the pointer in that module.
    for module in sys.modules.copy().values():
        if not _is_module(module):
            continue

        for k, v in module.__dict__.items():
            if v is function:
                module.__dict__[k] = decorated_function


def track_functions(*args):
    """ Track functions by decorating them with @task """
    for arg in args:
        try:
            _track_function(arg)
        except Exception:
            logger.exception("Failed to track %s" % arg)


def _is_module(obj):
    return isinstance(obj, ModuleType)


def track_module_functions(module):
    """
    Track functions inside module by decorating them with @task.
    Only functions implemented in module will be tracked, imported functions won't be tracked.
    """
    try:
        if not _is_module(module):
            return

        module_objects = module.__dict__.values()
        module_functions = [i for i in module_objects if _is_module_function(i, module)]
        track_functions(*module_functions)
    except Exception:
        logger.exception("Failed to track %s" % module)


def track_modules(*args):
    """
    Track functions inside modules by decorating them with @task.
    Only functions implemented in module will be tracked, imported functions won't be tracked.
    """
    for arg in args:
        try:
            track_module_functions(arg)
        except Exception:
            logger.exception("Failed to track %s" % arg)


def _is_module_function(function, module):
    try:
        if not _is_function(function):
            return False

        if not hasattr(function, "__globals__"):
            return False

        return function.__globals__ is module.__dict__
    except Exception:
        logger.exception("Failed to track %s" % function)
        return False
