import logging
import os

from functools import partial
from typing import Dict

from dbnd._core.configuration.environ_config import ENV_DBND_SCRIPT_NAME
from dbnd_airflow.tracking.conf_operations import fix_keys, flat_conf
from dbnd_airflow.tracking.config import AirflowTrackingConfig
from dbnd_airflow.tracking.dbnd_airflow_conf import get_airflow_conf
from dbnd_airflow.utils import logger


def spark_env(key):
    return "spark.env." + key


add_spark_env_fields = partial(fix_keys, spark_env)


def _filter_vars(key, bypass_dbnd=True, bypass_airflow=True, bypass_rest=True):
    if key.startswith("AIRFLOW_"):
        return bypass_airflow
    elif key.startswith("DBND_"):
        return bypass_dbnd
    return bypass_rest


def dbnd_tracking_env(
    new_vars, bypass_dbnd=True, bypass_airflow=True, bypass_rest=False
):
    environment = {
        key: value
        for key, value in os.environ.items()
        if _filter_vars(key, bypass_dbnd, bypass_airflow, bypass_rest)
    }
    environment.update(new_vars)
    return environment


def dbnd_wrap_spark_environment(environment=None):
    if not environment:
        environment = dict()
    environment.update({"DBND__ENABLE__SPARK_CONTEXT_ENV": "1"})
    return environment


def with_dbnd_tracking_spark_conf(user_dict):
    dbnd_dict = get_dbnd_tracking_spark_conf()
    user_dict.update(dbnd_dict)
    return user_dict


def get_dbnd_tracking_spark_conf(**kwargs):
    return add_spark_env_fields(get_airflow_conf(**kwargs))


def get_dbnd_tracking_spark_flat_conf(**kwargs):
    return flat_conf(add_spark_env_fields(get_airflow_conf(**kwargs)))


def spark_submit_with_dbnd_tracking(command_as_list, dbnd_context=None):
    """This functions augments spark-submit command with dbnd tracking metadata aka dbnd_context. If context
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
    if dbnd_context:
        context = dbnd_context
    else:
        context = get_dbnd_tracking_spark_flat_conf()

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

    return command_as_list[0 : index + 1] + context + command_as_list[index + 1 :]


logger = logging.getLogger(__name__)


def get_spark_submit_java_agent_conf():
    config = AirflowTrackingConfig.from_databand_context()
    agent_jar = config.spark_submit_dbnd_java_agent
    if agent_jar is None:
        logger.warning("You are not using the dbnd java agent")
        return
    logger.debug("found agent_jar %s", agent_jar)
    if not os.path.exists(agent_jar):
        logger.warning(
            "The wanted dbnd java agent is not found: {agent_path}".format(
                agent_path=agent_jar
            )
        )
        return

    return create_spark_java_agent_conf(agent_jar)


def get_databricks_python_script_name(raw_script_path):
    # type: (str) -> Dict[str,str]
    try:
        script_name = os.path.basename(raw_script_path)
        if script_name:
            return {ENV_DBND_SCRIPT_NAME: script_name}
        else:
            logger.warning(
                "Unable to determine script name from path %s", raw_script_path
            )
            return {}
    except Exception as exc:
        logger.error(
            "Unable to determine script name from path %s, exception: %s",
            raw_script_path,
            exc,
        )
        return {}


def get_databricks_java_agent_conf():
    config = AirflowTrackingConfig.from_databand_context()
    agent_jar = config.databricks_dbnd_java_agent
    logger.debug("found agent_jar %s", agent_jar)
    if agent_jar is None:
        logger.warning("You are not using the dbnd java agent")
        return

    return create_spark_java_agent_conf(agent_jar)


def create_spark_java_agent_conf(agent_jar):
    return {
        "spark.driver.extraJavaOptions": "-javaagent:{agent_jar}".format(
            agent_jar=agent_jar
        )
    }
