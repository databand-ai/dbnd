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
        conf.append(["--conf", key + "=" + conf_as_dict[key]])
    return conf


def spark_submit_with_dbnd_tracking(command_as_list, dbnd_context=None):
    if not dbnd_context:
        dbnd_context = get_dbnd_tracking_spark_conf()
    index = command_as_list.index("spark-submit")
    return command_as_list[0 : index + 1] + dbnd_context + command_as_list[index + 1 :]
