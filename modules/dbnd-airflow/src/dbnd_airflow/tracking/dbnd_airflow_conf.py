import logging

from itertools import islice

import six

from dbnd._core.configuration.environ_config import (
    DATABAND_AIRFLOW_CONN_ID,
    DBND_PARENT_TASK_RUN_ATTEMPT_UID,
    DBND_PARENT_TASK_RUN_UID,
    DBND_ROOT_RUN_TRACKER_URL,
    DBND_ROOT_RUN_UID,
)
from dbnd._core.settings import CoreConfig, TrackingConfig
from dbnd._core.utils.uid_utils import get_airflow_instance_uid


logger = logging.getLogger(__name__)


def get_airflow_conf(
    dag_id="{{dag.dag_id}}",
    task_id="{{task.task_id}}",
    execution_date="{{ts}}",
    try_number="{{task_instance._try_number}}",
):
    """
    These properties are
        AIRFLOW_CTX_DAG_ID - name of the Airflow DAG to associate a run with
        AIRFLOW_CTX_EXECUTION_DATE - execution_date to associate a run with
        AIRFLOW_CTX_TASK_ID - name of the Airflow Task to associate a run with
        AIRFLOW_CTX_TRY_NUMBER - try number of the Airflow Task to associate a run with
    """
    airflow_conf = {
        "AIRFLOW_CTX_DAG_ID": dag_id,
        "AIRFLOW_CTX_EXECUTION_DATE": execution_date,
        "AIRFLOW_CTX_TASK_ID": task_id,
        "AIRFLOW_CTX_TRY_NUMBER": try_number,
        "AIRFLOW_CTX_UID": get_airflow_instance_uid(),
    }
    airflow_conf.update(get_databand_url_conf())
    return airflow_conf


def _get_databand_url():
    try:
        external = TrackingConfig().databand_external_url
        if external:
            return external
        return CoreConfig().databand_url
    except Exception:
        pass


def get_databand_url_conf():
    databand_url = _get_databand_url()
    if databand_url:
        return {"DBND__CORE__DATABAND_URL": databand_url}
    return {}


def extract_airflow_tracking_conf(context):
    conf = extract_airflow_conf(context)
    conf.update(get_databand_url_conf())
    return conf


def extract_airflow_conf(context):
    task_instance = context.get("task_instance")
    if task_instance is None:
        return {}

    dag_id = task_instance.dag_id
    task_id = task_instance.task_id
    execution_date = str(task_instance.execution_date)
    try_number = str(task_instance.try_number)

    if dag_id and task_id and execution_date:
        return {
            "AIRFLOW_CTX_DAG_ID": dag_id,
            "AIRFLOW_CTX_EXECUTION_DATE": execution_date,
            "AIRFLOW_CTX_TASK_ID": task_id,
            "AIRFLOW_CTX_TRY_NUMBER": try_number,
            "AIRFLOW_CTX_UID": get_airflow_instance_uid(),
        }
    return {}


def get_tracking_information(context, task_run):
    info = extract_airflow_conf(context)
    return extend_airflow_ctx_with_dbnd_tracking_info(task_run, info)


def extend_airflow_ctx_with_dbnd_tracking_info(task_run, airflow_ctx_env):
    info = airflow_ctx_env.copy()

    info[DBND_ROOT_RUN_UID] = task_run.run.root_run_info.root_run_uid
    info[DBND_ROOT_RUN_TRACKER_URL] = task_run.run.root_run_info.root_run_url
    info[DBND_PARENT_TASK_RUN_UID] = task_run.task_run_uid
    info[DBND_PARENT_TASK_RUN_ATTEMPT_UID] = task_run.task_run_attempt_uid

    core = CoreConfig.current()
    info["DBND__CORE__DATABAND_URL"] = core.databand_url
    info["DBND__CORE__DATABAND_ACCESS_TOKEN"] = core.databand_access_token

    info = {n: str(v) for n, v in six.iteritems(info) if v is not None}
    return info


def get_xcoms(task_instance):
    from airflow.models.xcom import XCom

    execution_date = task_instance.execution_date
    task_id = task_instance.task_id
    dag_id = task_instance.dag_id

    results = XCom.get_many(execution_date, task_ids=task_id, dag_ids=dag_id)
    return [(xcom.key, xcom.value) for xcom in results]


def set_dbnd_config_from_airflow_connections():
    """
    Set Databand config from Extra section in Airflow dbnd_config connection.
    Read about setting DBND Connection at: https://dbnd.readme.io/docs/setting-up-configurations-using-airflow-connections
    """
    from airflow.exceptions import AirflowException
    from dbnd._core.configuration.dbnd_config import config
    from dbnd._core.configuration.config_value import ConfigValuePriority
    from dbnd_airflow.compat import BaseHook

    try:
        # Get connection from Airflow
        dbnd_conn_config = BaseHook.get_connection(DATABAND_AIRFLOW_CONN_ID)
        json_config = dbnd_conn_config.extra_dejson

        if not json_config:
            if dbnd_conn_config.extra:
                # Airflow failed to parse extra config as json
                logger.error(
                    "Extra config for {0} connection, should be formated as a valid json.".format(
                        DATABAND_AIRFLOW_CONN_ID
                    )
                )

            else:
                # Extra section in connection is empty
                logger.warning(
                    "No extra config provided to {0} connection.".format(
                        DATABAND_AIRFLOW_CONN_ID
                    )
                )

            return False

        config.set_values(
            config_values=json_config,
            priority=ConfigValuePriority.NORMAL,
            source="airflow_dbnd_connection",
        )
        logger.info(
            "Databand config was set using {0} connection.".format(
                DATABAND_AIRFLOW_CONN_ID
            )
        )
        return True

    except AirflowException as afe:
        # Probably dbnd_config is not set properly in Airflow connections.
        logger.info(afe)
        return False

    except Exception:
        logger.exception("Failed to extract dbnd config from airflow's connection.")
        return False
