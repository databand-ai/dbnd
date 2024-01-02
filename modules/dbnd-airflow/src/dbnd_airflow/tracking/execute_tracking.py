# © Copyright Databand.ai, an IBM Company 2022

import logging

from contextlib import contextmanager
from itertools import islice
from typing import Any, Dict, Optional

from dbnd import config, dbnd_bootstrap, log_metric, log_metrics
from dbnd._core.configuration.environ_config import is_dbnd_disabled
from dbnd._core.constants import TaskRunState
from dbnd._core.log.dbnd_log import dbnd_log_debug, dbnd_log_exception
from dbnd._core.task_run.task_run import TaskRun
from dbnd._core.task_run.task_run_error import TaskRunError
from dbnd._core.tracking.airflow_task_context import AirflowTaskContext
from dbnd._core.tracking.managers.callable_tracking import _log_result
from dbnd._core.tracking.script_tracking_manager import (
    dbnd_tracking_stop,
    tracking_start_base,
)
from dbnd._core.utils.basics.environ_utils import env
from dbnd._core.utils.basics.nested_context import safe_nested
from dbnd_airflow.raw_constants import MONITOR_DAG_NAME
from dbnd_airflow.tracking.config import AirflowTrackingConfig
from dbnd_airflow.tracking.dbnd_airflow_conf import (
    AIRFLOW_MONITOR_CONFIG_NAME,
    IS_SYNC_ENABLED_TRACKING_CONFIG_NAME,
    get_dbnd_config_dict_from_airflow_connections,
    get_sync_status_and_tracking_dag_ids_from_dbnd_conf,
    get_tracking_information,
    get_xcoms,
    set_dbnd_config_from_airflow_connections,
)
from dbnd_airflow.tracking.wrap_operators import (
    get_airflow_operator_handlers_config,
    wrap_operator_with_tracking_info,
)
from dbnd_airflow.utils import get_airflow_instance_uid


logger = logging.getLogger(__name__)


def is_monitor_dag(dag_id):
    return dag_id == MONITOR_DAG_NAME


def is_subdag_eligible_for_tracking(dag_id, tracking_list):
    for tracked_dag_id in tracking_list:
        if dag_id.startswith(f"{tracked_dag_id}."):
            return True

    return False


def is_dag_eligable_for_tracking(dag_id, tracking_list):
    # If return value is None, we track all dags. If it's empty list - we don't track anything.
    return (
        tracking_list is None
        or dag_id in tracking_list
        or is_subdag_eligible_for_tracking(dag_id, tracking_list)
        or is_monitor_dag(dag_id)
    )


def _try_to_start_airflow_operator_tracking(context):
    if is_dbnd_disabled():
        dbnd_log_debug("Databand is disabled via project config.")
        # we are not tracking if dbnd is disabled
        return None

    copied_operator = context["task_instance"].task
    # import pydevd_pycharm
    # pydevd_pycharm.settrace('localhost', port=8787, stdoutToServer=True, stderrToServer=True)

    dbnd_log_debug(
        "Running tracked .execute for %s (%s)",
        copied_operator.task_id,
        f"{copied_operator.__class__.__module__}.{copied_operator.__class__.__qualname__}",
    )

    dbnd_config_from_connection = get_dbnd_config_dict_from_airflow_connections()
    sync_enabled, tracking_list = get_sync_status_and_tracking_dag_ids_from_dbnd_conf(
        dbnd_config_from_connection
    )
    if not sync_enabled:
        dbnd_log_debug(
            f"DBND Tracking is disabled via Airflow connection`dbnd_config`"
            f" (at {AIRFLOW_MONITOR_CONFIG_NAME}.{IS_SYNC_ENABLED_TRACKING_CONFIG_NAME} ("
        )
        return None

    if not is_dag_eligable_for_tracking(
        context["task_instance"].dag_id, tracking_list=tracking_list
    ):
        dbnd_log_debug("DAG id is not tracked %s", context["task_instance"].dag_id)
        return None

    # Only here we kinda know that we will start tracking

    dbnd_bootstrap()

    # Set Databand config from Extra section in Airflow dbnd_config connection.
    set_dbnd_config_from_airflow_connections(
        dbnd_config_from_connection=dbnd_config_from_connection
    )

    task_context = extract_airflow_task_context(context)
    # start operator execute run with current airflow context
    task_run: Optional[TaskRun] = tracking_start_base(airflow_context=task_context)
    return task_run


def execute_with_dbnd_tracking(context):
    """
    This function replaces the operator's original `execute` function
    """

    # IMPORTANT!!: copied_operator:
    # ---------------------------------------
    # The task (=operator) is copied when airflow enters to TaskInstance._run_raw_task.
    # Then, only the copy_task (=copy_operator) is changed or called (render jinja, signal_handler,
    # pre_execute, execute, etc..).
    copied_operator = context["task_instance"].task

    dbnd_tracking_task_run = False
    try:
        dbnd_tracking_task_run = _try_to_start_airflow_operator_tracking(context)
    except Exception as e:
        dbnd_log_exception(
            "Exception caught while starting DBND tracking: {}".format(e)
        )

    if not dbnd_tracking_task_run:

        execute = get_execute_function(copied_operator)
        result = execute(copied_operator, context)
        return result

    from airflow.exceptions import AirflowRescheduleException

    # running the operator's original execute function
    try:
        operator_config = getattr(copied_operator, "dbnd_config", {})
        with config(operator_config, "airflow_operator_definition"):
            with af_tracking_context(dbnd_tracking_task_run, context, copied_operator):
                execute = get_execute_function(copied_operator)
                result = execute(copied_operator, context)

    # Check if this is sensor task that is retrying - normal behavior and not really an exception
    except AirflowRescheduleException:
        dbnd_tracking_stop(finalize_run=False)
        raise
    # catch if the original execute failed
    except Exception as ex:
        try:
            error = TaskRunError.build_from_ex(ex, dbnd_tracking_task_run)
            dbnd_tracking_task_run.set_task_run_state(
                state=TaskRunState.FAILED, error=error
            )
        except Exception:
            dbnd_log_exception("Failed to save failed state for the task")

        dbnd_tracking_stop()
        raise

    # if we have a task run here we want to log results and xcoms
    if dbnd_tracking_task_run:
        try:
            track_config = AirflowTrackingConfig.from_databand_context()
            if track_config.track_xcom_values:
                # reporting xcoms as metrix of the task
                log_xcom(context, track_config)

            if track_config.track_airflow_execute_result:
                # reporting the result
                log_operator_result(
                    dbnd_tracking_task_run,
                    result,
                    copied_operator,
                    track_config.track_xcom_values,
                )

        except Exception as e:
            dbnd_log_exception(
                "exception caught while logging airflow operator results {}".format(e)
            )

    # make sure we close and return the original results
    dbnd_tracking_stop()
    return result


def log_xcom(context, track_config):
    task_instance = context["task_instance"]
    xcoms = get_xcoms(task_instance)
    if xcoms:
        # get only the first xcoms
        xcoms_head = islice(xcoms, track_config.max_xcom_length)
        log_metrics(dict(xcoms_head))


def log_operator_result(task_run, result, operator, track_xcom):
    _log_result(task_run, result)

    # after airflow runs the operator it xcom_push the result, so we log it
    if track_xcom and operator.do_xcom_push and result is not None:
        from airflow.models import XCOM_RETURN_KEY

        log_metric(key=XCOM_RETURN_KEY, value=result)


@contextmanager
def af_tracking_context(task_run, airflow_context, operator):
    """
    Wrap the execution with handling the environment management
    """
    if not task_run:
        # aborting -  can't enter the context without task_run_executor
        yield
        return

    try:
        tracking_info = get_tracking_information(airflow_context, task_run)
        airflow_operator_handlers = get_airflow_operator_handlers_config(
            task_run.run.context.settings.tracking.airflow_operator_handlers
        )
        operator_wrapper = wrap_operator_with_tracking_info(
            tracking_info, operator, airflow_operator_handlers
        )

    except Exception as e:
        logger.error(
            "exception caught adding tracking context to operator execution {}"
            "continue without tracking context".format(e),
            exc_info=True,
        )
        yield
        return

    # wrap the execution with tracking info in the environment
    with safe_nested(env(**tracking_info), operator_wrapper):
        yield


def execute_with_dbnd_tracking_for_class(self, context):
    """
    Different wrapper for operator's class
    """
    return execute_with_dbnd_tracking(context)


def track_operator(operator):
    """
    Replace the operator's execute method with our `tracked execute` method
    """
    import inspect

    if inspect.isclass(operator):
        from airflow.models import BaseOperator

        # we only track operators which base on Airflow BaseOperator
        if not issubclass(operator, BaseOperator):
            return operator

        # the operator class is already tracked
        if (
            hasattr(operator, "_tracked_class")
            and operator.__name__ == operator._tracked_class
        ):
            return operator

        # this is the first time we encounter this class so we mark it
        operator._tracked_class = operator.__name__
        operator.__execute__ = operator.execute
        operator.execute = execute_with_dbnd_tracking_for_class
        return operator

    else:
        # the operator instance is already tracked
        if hasattr(operator, "_tracked_instance"):
            return operator

        # this is the first time we encounter this instance so we mark it
        operator._tracked_instance = True

        # if the operator's class is tracked, we can't used `operator.__class__.execute`
        # need to use the original execute (__execute__)
        if (
            hasattr(operator, "_tracked_class")
            and operator.__class__.__name__ == operator._tracked_class
        ):
            operator.__execute__ = operator.__class__.__execute__
        else:
            # this can be a problem when the operator class doesn't implement it's own execute
            operator.__execute__ = operator.__class__.execute

        operator.execute = execute_with_dbnd_tracking

    return operator


def get_execute_function(copied_operator):
    """
    After tracking operator we need to access the original execution function
    """
    if hasattr(copied_operator, "_tracked_instance"):
        return copied_operator.__execute__

    if hasattr(copied_operator, "_tracked_class"):
        return copied_operator.__class__.__execute__

    raise AttributeError("tracked failed to run original operator.execute")


def extract_airflow_task_context(
    airflow_context: Dict[str, Any]
) -> Optional[AirflowTaskContext]:
    """Create AirflowTaskContext for airflow_context dict"""

    task_instance = airflow_context.get("task_instance")
    if task_instance is None:
        return None

    dag_id = task_instance.dag_id
    task_id = task_instance.task_id
    execution_date = str(task_instance.execution_date)
    try_number = task_instance.try_number

    if dag_id and task_id and execution_date:
        return AirflowTaskContext(
            dag_id=dag_id,
            execution_date=execution_date,
            task_id=task_id,
            try_number=try_number,
            context=airflow_context,
            airflow_instance_uid=get_airflow_instance_uid(),
            is_subdag=airflow_context.get("dag").is_subdag,
        )

    logger.debug(
        "airflow context from inspect, at least one of those params is missing"
        "dag_id: {}, execution_date: {}, task_id: {}".format(
            dag_id, execution_date, task_id
        )
    )
    return None
