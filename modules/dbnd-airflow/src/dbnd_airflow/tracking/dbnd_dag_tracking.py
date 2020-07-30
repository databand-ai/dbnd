import logging

from types import FunctionType, ModuleType

from dbnd import task
from dbnd._core.configuration.environ_config import get_dbnd_project_config
from dbnd._core.decorator.dbnd_func_proxy import DbndFuncProxy
from dbnd._core.settings import CoreConfig
from dbnd._core.tracking.no_tracking import should_not_track
from dbnd_airflow.tracking.dbnd_spark_conf import (
    _get_databand_url,
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

        databand_url = _get_databand_url()
        if databand_url:
            spark_vars["DBND__CORE__DATABAND_URL"] = databand_url


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
    if isinstance(operator.python_callable, DbndFuncProxy):
        # function already has @task
        return

    operator.python_callable = task(operator.python_callable)


def _track_task(task):
    if should_not_track(task):
        return
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
        if should_not_track(dag):
            return

        for task in dag.tasks:
            track_task(task)
    except Exception:
        logger.exception("Failed to modify %s for tracking" % dag.dag_id)
