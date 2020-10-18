import logging

from dbnd._core.configuration.environ_config import get_dbnd_project_config
from dbnd._core.tracking.no_tracking import should_not_track
from dbnd._core.utils.type_check_utils import is_instance_by_class_name
from dbnd_airflow.tracking.execute_tracking import new_execute


logger = logging.getLogger(__name__)


def _track_task(task):
    if should_not_track(task):
        return

    if is_instance_by_class_name(task, "SubDagOperator"):
        # we do not track the execute of a SubDag, only its tasks
        track_dag(task.subdag)
    else:
        track_operator(task)


def new_execute_for_class(self, context):
    return new_execute(context)


def track_operator(operator):
    import inspect

    if inspect.isclass(operator):
        from airflow.models import BaseOperator
        from airflow.operators.subdag_operator import SubDagOperator

        # we only track operators which base on Airflow BaseOperator
        if not issubclass(operator, BaseOperator):
            return operator

        # we are not tracking sub dags through this mechanism
        if issubclass(operator, SubDagOperator):
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
