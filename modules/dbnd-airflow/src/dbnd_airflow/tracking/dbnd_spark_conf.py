import logging
import os

from functools import partial

from dbnd_airflow.tracking.conf_operations import fix_keys, flat_conf
from dbnd_airflow.tracking.config import TrackingConfig
from dbnd_airflow.tracking.dbnd_airflow_conf import get_airflow_conf
from dbnd_airflow.utils import logger


def spark_env(key):
    return "spark.env." + key


add_spark_env_fields = partial(fix_keys, spark_env)


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


def dbnd_wrap_spark_environment(environment=None):
    if not environment:
        environment = dict()
    environment.update({"DBND__ENABLE__SPARK_CONTEXT_ENV": "1"})
    return environment


def with_dbnd_tracking_spark_conf(user_dict):
    dbnd_dict = get_dbnd_tracking_spark_conf_dict()
    user_dict.update(dbnd_dict)
    return user_dict


def get_dbnd_tracking_spark_conf_dict(**kwargs):
    return dict(add_spark_env_fields(get_airflow_conf(**kwargs)))


def get_dbnd_tracking_spark_conf(**kwargs):
    return flat_conf(add_spark_env_fields(get_airflow_conf(**kwargs)))


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


logger = logging.getLogger(__name__)


def get_local_spark_java_agent_conf():
    config = TrackingConfig()
    agent_jar = config.local_dbnd_java_agent
    logger.debug("found agent_jar %s", agent_jar)
    if agent_jar is None or not os.path.exists(agent_jar):
        logger.warning("The wanted agents jar doesn't exists: %s", agent_jar)
        return

    return create_spark_java_agent_conf(agent_jar)


def get_databricks_java_agent_conf():
    config = TrackingConfig()
    agent_jar = config.databricks_dbnd_java_agent
    logger.debug("found agent_jar %s", agent_jar)
    if agent_jar is None:
        logger.warning("No agent jar found")
        return

    return create_spark_java_agent_conf(agent_jar)


def create_spark_java_agent_conf(agent_jar):
    return {
        "spark.driver.extraJavaOptions": "-javaagent:{agent_jar}".format(
            agent_jar=agent_jar
        )
    }
