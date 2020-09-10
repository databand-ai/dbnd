import logging

from collections import OrderedDict

from dbnd import task
from dbnd._core.configuration.environ_config import get_dbnd_project_config
from dbnd._core.decorator.dbnd_func_proxy import DbndFuncProxy
from dbnd._core.tracking.no_tracking import should_not_track
from dbnd._core.utils.type_check_utils import is_instance_by_class_name
from dbnd_airflow.tracking.dbnd_airflow_conf import get_airflow_conf
from dbnd_airflow.tracking.dbnd_spark_conf import (
    dbnd_wrap_spark_environment,
    get_databricks_java_agent_conf,
    get_dbnd_tracking_spark_conf_dict,
    get_local_spark_java_agent_conf,
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
        cluster.setdefault("spark_env_vars", {})
        cluster["spark_env_vars"].update(get_airflow_conf())

        if "spark_jar_task" in config:
            cluster.setdefault("spark_conf", {})
            agent_conf = get_databricks_java_agent_conf()
            if agent_conf is not None:
                cluster["spark_conf"].update(agent_conf)


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

    if has_java_application(operator):
        agent_conf = get_local_spark_java_agent_conf()
        if agent_conf is not None:
            operator._conf.update(agent_conf)


def has_java_application(operator):
    return (
        operator._application.endswith(".jar")
        or operator._jars
        and operator._jars.ends_with(".jar")
    )


def track_python_operator(operator):
    if isinstance(operator.python_callable, DbndFuncProxy):
        # function already has @task
        return

    operator.python_callable = task(operator.python_callable)


def track_subdag(dag):
    track_dag(dag.subdag)


_TRACKING = OrderedDict(
    [
        ("SubDagOperator", track_subdag),
        ("EmrAddStepsOperator", track_emr_add_steps_operator),
        ("DatabricksSubmitRunOperator", track_databricks_submit_run_operator),
        ("DataProcPySparkOperator", track_data_proc_pyspark_operator),
        ("SparkSubmitOperator", track_spark_submit_operator),
        ("PythonOperator", track_python_operator),
    ]
)


def register_tracking(class_name, tracking_wrapper):
    _TRACKING[class_name] = tracking_wrapper


def get_tracking_wrapper(task):
    for class_name, tracking_wrapper in _TRACKING.items():
        if is_instance_by_class_name(task, class_name):
            return tracking_wrapper
    raise KeyError


def _track_task(task):
    if should_not_track(task):
        return

    try:
        tracking_wrapper = get_tracking_wrapper(task)
    except KeyError:
        return
    else:
        tracking_wrapper(task)


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
