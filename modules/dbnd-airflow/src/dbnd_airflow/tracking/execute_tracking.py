import logging

from contextlib import contextmanager
from itertools import islice
from typing import Optional

from dbnd import get_dbnd_project_config, log_metric, log_metrics
from dbnd._core.constants import TaskRunState
from dbnd._core.errors.errors_utils import log_exception
from dbnd._core.task_run.task_run import TaskRun
from dbnd._core.task_run.task_run_error import TaskRunError
from dbnd._core.tracking.airflow_dag_inplace_tracking import extract_airflow_context
from dbnd._core.tracking.managers.callable_tracking import _log_result
from dbnd._core.tracking.script_tracking_manager import (
    dbnd_airflow_tracking_start,
    dbnd_tracking_stop,
)
from dbnd._core.utils.basics.environ_utils import env
from dbnd._core.utils.basics.nested_context import nested
from dbnd_airflow.raw_constants import MONITOR_DAG_NAME
from dbnd_airflow.tracking.config import AirflowTrackingConfig
from dbnd_airflow.tracking.dbnd_airflow_conf import get_tracking_information, get_xcoms
from dbnd_airflow.tracking.wrap_operators import wrap_operator_with_tracking_info


logger = logging.getLogger(__name__)


AIRFLOW_MONITOR_CONFIG_NAME = "airflow_monitor"
DAG_IDS_FOR_TRACKING_CONFIG_NAME = "dag_ids"


def get_tracking_dag_ids_from_airflow_json():
    try:
        from dbnd_airflow.tracking.dbnd_airflow_conf import (
            get_dbnd_json_config_from_airflow_connections,
        )

        json = get_dbnd_json_config_from_airflow_connections()

        if json:
            monitor_config = json.get(AIRFLOW_MONITOR_CONFIG_NAME, None)
            if monitor_config:
                dag_ids_config = monitor_config.get(
                    DAG_IDS_FOR_TRACKING_CONFIG_NAME, None
                )

                if dag_ids_config and isinstance(dag_ids_config, str):
                    return dag_ids_config.split(",")

                if dag_ids_config and isinstance(dag_ids_config, list):
                    return dag_ids_config
        return None
    except Exception as e:
        log_exception(
            "exception caught while running on dbnd new execute {}".format(e), e
        )
        return None


def is_monitor_dag(dag_id):
    return dag_id == MONITOR_DAG_NAME


def is_subdag_eligible_for_tracking(dag_id, tracking_list):
    for tracked_dag_id in tracking_list:
        if dag_id.startswith(f"{tracked_dag_id}."):
            return True

    return False


def is_dag_eligable_for_tracking(dag_id):
    # If return value is None, we track all dags. If it's empty list - we don't track anything.
    tracking_list = get_tracking_dag_ids_from_airflow_json()
    return (
        tracking_list is None
        or dag_id in tracking_list
        or is_subdag_eligible_for_tracking(dag_id, tracking_list)
        or is_monitor_dag(dag_id)
    )


def new_execute(context):
    """
    This function replaces the operator's original `execute` function
    """
    # IMPORTANT!!: copied_operator:
    # ---------------------------------------
    # The task (=operator) is copied when airflow enters to TaskInstance._run_raw_task.
    # Then, only the copy_task (=copy_operator) is changed or called (render jinja, signal_handler,
    # pre_execute, execute, etc..).
    copied_operator = context["task_instance"].task

    if not is_dag_eligable_for_tracking(context["task_instance"].dag_id):
        execute = get_execute_function(copied_operator)
        result = execute(copied_operator, context)
        return result

    try:
        # Set that we are in Airflow tracking mode
        get_dbnd_project_config().set_is_airflow_runtime()

        task_context = extract_airflow_context(context)
        # start operator execute run with current airflow context
        task_run = dbnd_airflow_tracking_start(
            airflow_context=task_context
        )  # type: Optional[TaskRun]

    except Exception as e:
        task_run = None
        logger.error(
            "exception caught while running on dbnd new execute {}".format(e),
            exc_info=True,
        )

    from airflow.exceptions import AirflowRescheduleException

    # running the operator's original execute function
    try:
        with af_tracking_context(task_run, context, copied_operator):
            execute = get_execute_function(copied_operator)
            result = execute(copied_operator, context)

    # Check if this is sensor task that is retrying - normal behavior and not really an exception
    except AirflowRescheduleException:
        dbnd_tracking_stop(finalize_run=False)
        raise
    # catch if the original execute failed
    except Exception as ex:
        if task_run:
            error = TaskRunError.build_from_ex(ex, task_run)
            task_run.set_task_run_state(state=TaskRunState.FAILED, error=error)

        dbnd_tracking_stop()
        raise

    # if we have a task run here we want to log results and xcoms
    if task_run:
        try:
            track_config = AirflowTrackingConfig.from_databand_context()
            if track_config.track_xcom_values:
                # reporting xcoms as metrix of the task
                log_xcom(context, track_config)

            if track_config.track_airflow_execute_result:
                # reporting the result
                log_operator_result(
                    task_run, result, copied_operator, track_config.track_xcom_values
                )

        except Exception as e:
            logger.error(
                "exception caught will tracking airflow operator {}".format(e),
                exc_info=True,
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
        # aborting -  can't enter the context without task_run
        yield
        return

    try:
        tracking_info = get_tracking_information(airflow_context, task_run)
        operator_wrapper = wrap_operator_with_tracking_info(tracking_info, operator)

    except Exception as e:
        logger.error(
            "exception caught adding tracking context to operator execution {}"
            "continue without tracking context".format(e),
            exc_info=True,
        )
        yield
        return

    # wrap the execution with tracking info in the environment
    with nested(env(**tracking_info), operator_wrapper):
        yield


def new_execute_for_class(self, context):
    """
    Different wrapper for operator's class
    """
    return new_execute(context)


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
        operator.execute = new_execute_for_class
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

        operator.execute = new_execute

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
