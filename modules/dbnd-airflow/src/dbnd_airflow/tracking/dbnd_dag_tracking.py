import logging

from dbnd._core.configuration.environ_config import get_dbnd_project_config
from dbnd._core.tracking.no_tracking import should_not_track
from dbnd._core.utils.type_check_utils import is_instance_by_class_name
from dbnd_airflow.tracking.execute_tracking import track_operator


logger = logging.getLogger(__name__)


def _track_task(task):
    from airflow.operators.subdag_operator import SubDagOperator

    if should_not_track(task):
        return

    if is_instance_by_class_name(task, SubDagOperator.__name__):
        # we do not track the execute of a SubDag, only its tasks
        track_dag(task.subdag)
    else:
        track_operator(task)


def _is_verbose():
    config = get_dbnd_project_config()
    return config.is_verbose()


def track_task(task):
    # `task` is Airflow Operator, although documentation states that it TaskInstance
    # Usually called from Dag -> add to dag bag
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
