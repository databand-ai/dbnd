import os


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
    dag_id="{{dag.dag_id}}", task_id="{{task.task_id}}", execution_date="{{ts}}"
):
    return {
        "spark.env.AIRFLOW_CTX_DAG_ID": dag_id,
        "spark.env.AIRFLOW_CTX_EXECUTION_DATE": execution_date,
        "spark.env.AIRFLOW_CTX_TASK_ID": task_id,
    }


def get_dbnd_tracking_spark_conf(
    dag_id="{{dag.dag_id}}", task_id="{{task.task_id}}", execution_date="{{ts}}"
):
    conf_as_dict = get_dbnd_tracking_spark_conf_dict(dag_id, task_id, execution_date)
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
