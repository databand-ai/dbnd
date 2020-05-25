import logging
import os
import sys

from types import FunctionType, ModuleType

from dbnd import task
from dbnd._core.decorator.func_task_decorator import _decorated_user_func


logger = logging.getLogger(__name__)


def _filter_vars(key, bypass_dbnd, bypass_airflow, bypass_rest):
    if key.startswith("AIRFLOW_"):
        return True if bypass_airflow else False
    if key.startswith("DBND_"):
        return True if bypass_dbnd else False
    return True if bypass_rest else False


def dbnd_tracking_env(
    new_vars, bypass_dbnd=True, bypass_airflow=True, bypass_rest=False
):

    environment = {
        key: os.environ[key]
        for key in os.environ.keys()
        if _filter_vars(key, bypass_dbnd, bypass_airflow, bypass_rest)
    }
    environment.update(new_vars)
    return environment


# SparkSubmit operator handles existing environment, add additional databand variables only
def dbnd_wrap_spark_environment(environment=None):
    if not environment:
        environment = dict()
    environment.update({"DBND__ENABLE__SPARK_CONTEXT_ENV": "1"})
    return environment


def get_dbnd_tracking_spark_conf_dict(
    dag_id="{{dag.dag_id}}",
    task_id="{{task.task_id}}",
    execution_date="{{ts}}",
    try_number="{{task_instance._try_number}}",
):
    return {
        "spark.env.AIRFLOW_CTX_DAG_ID": dag_id,
        "spark.env.AIRFLOW_CTX_EXECUTION_DATE": execution_date,
        "spark.env.AIRFLOW_CTX_TASK_ID": task_id,
        "spark.env.AIRFLOW_CTX_TRY_NUMBER": try_number,
    }


def get_dbnd_tracking_spark_conf(
    dag_id="{{dag.dag_id}}",
    task_id="{{task.task_id}}",
    execution_date="{{ts}}",
    try_number="{{task_instance._try_number}}",
):
    """ This functions returns Airflow ids  as spark configuration properties. This is used to associate spark run with airflow dag/task.
        These properties are
            spark.env.AIRFLOW_CTX_DAG_ID  -  name of the Airflow DAG to associate a run with
            spark.env.AIRFLOW_CTX_EXECUTION_DATE - execution_date to associate a run with
            spark.env.AIRFLOW_CTX_TASK_ID" - name of the Airflow Task to associate a run with
            spark.env.AIRFLOW_CTX_TRY_NUMBER" - try number of the Airflow Task to associate a run with

        Example:
            >>> get_dbnd_tracking_spark_conf()
            ['--conf', 'spark.env.AIRFLOW_CTX_DAG_ID={{dag.dag_id}}', '--conf', 'spark.env.AIRFLOW_CTX_EXECUTION_DATE={{ts}}', '--conf', 'spark.env.AIRFLOW_CTX_TASK_ID={{task.task_id}}', '--conf', 'spark.env.AIRFLOW_CTX_TRY_NUMBER={{task._try_number}}']

        Args:
            dag_id: name of the Airflow DAG to associate a run. By default set to {{dag.dag_id}} Airflow template
            task_id: name of the Airflow task to associate a run. By default set to {{task.task_id}} Airflow template
            execution_date: execution date as a string. By default set to {{ts}} Airflow template
            try_number: try number of Airflow task. By default set to {{task_instance._try_number}} Airflow template

        Returns:
            List of Airflow Ids as spark properties. Ready to concatenate to spark-submit command

    """

    conf_as_dict = get_dbnd_tracking_spark_conf_dict(
        dag_id, task_id, execution_date, try_number
    )
    conf = []
    for key in conf_as_dict.keys():
        conf.extend(["--conf", key + "=" + conf_as_dict[key]])
    return conf


def spark_submit_with_dbnd_tracking(command_as_list, dbnd_context=None):
    """ This functions augments spark-submit command with dbnd tracking metadata aka dbnd_context. If context
    is not provided, a default Airflow templates are set for DAG Id, Task Id, and execution date

    Adds 3 configuration properties to spark-submit to associate spark run with airflow DAG that initiated this run.
    These properties are
        spark.env.AIRFLOW_CTX_DAG_ID  -  name of the Airflow DAG to associate a run with
        spark.env.AIRFLOW_CTX_EXECUTION_DATE - execution_date to associate a run with
        spark.env.AIRFLOW_CTX_TASK_ID" - name of the Airflow Task to associate a run with

    Example:
        >>> spark_submit_with_dbnd_tracking(["spark-submit","my_script.py","my_param"])
        ['spark-submit', '--conf', 'spark.env.AIRFLOW_CTX_DAG_ID={{dag.dag_id}}', '--conf', 'spark.env.AIRFLOW_CTX_EXECUTION_DATE={{ts}}', '--conf', 'spark.env.AIRFLOW_CTX_TASK_ID={{task.task_id}}', 'my_script.py', 'my_param']

    Args:
        command_as_list: spark-submit command line as a list of strings.
        dbnd_context: optional dbnd_context provided by a user. I

    Returns:
        An updated spark-submit command including dbnd context.

    """

    if not dbnd_context:
        dbnd_context = get_dbnd_tracking_spark_conf()

    index = next(
        (
            command_as_list.index(elm)
            for elm in command_as_list
            if "spark-submit" in elm
        ),
        -1,
    )
    if index == -1:
        raise Exception("Failed to find spark-submit in %s" % " ".join(command_as_list))

    return command_as_list[0 : index + 1] + dbnd_context + command_as_list[index + 1 :]


def track_emr_add_steps_operator(operator):
    for step in operator.steps:
        args = step["HadoopJarStep"]["Args"]
        if args and "spark-submit" in args[0]:
            step["HadoopJarStep"]["Args"] = spark_submit_with_dbnd_tracking(args)


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


def track_task(task):
    if is_instance_by_class_name(task, "EmrAddStepsOperator"):
        track_emr_add_steps_operator(task)
    elif is_instance_by_class_name(task, "DataProcPySparkOperator"):
        track_data_proc_pyspark_operator(task)
    elif is_instance_by_class_name(task, "SparkSubmitOperator"):
        track_spark_submit_operator(task)
    elif is_instance_by_class_name(task, "PythonOperator"):
        track_python_operator(task)


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
    for task in dag.tasks:
        try:
            track_task(task)
        except Exception:
            logger.exception("Failed to modify %s for tracking" % task.task_id)


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


if __name__ == "__main__":
    print(get_dbnd_tracking_spark_conf())
