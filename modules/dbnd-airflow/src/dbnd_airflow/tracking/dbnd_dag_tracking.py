import logging

from dbnd._core.log.dbnd_log import dbnd_log_exception, dbnd_log_init_msg
from dbnd._core.tracking.no_tracking import should_not_track
from dbnd._core.utils.type_check_utils import is_instance_by_class_name
from dbnd_airflow.tracking.execute_tracking import track_operator


logger = logging.getLogger(__name__)


def _track_task(task):
    from airflow.operators.subdag_operator import SubDagOperator

    if should_not_track(task):
        return

    track_operator(task)
    dbnd_log_init_msg("%s is tracked by dbnd" % task.task_id)
    if is_instance_by_class_name(task, SubDagOperator.__name__):
        # we also track the subdag's inner tasks
        track_dag(task.subdag)


def track_task(task):
    # `task` is Airflow Operator, although documentation states that it TaskInstance
    # Usually called from Dag -> add to dag bag
    try:
        _track_task(task)
    except Exception:
        dbnd_log_exception("Failed to modify %s for tracking" % task.task_id)


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
        dbnd_log_exception("Failed to modify %s for tracking" % dag.dag_id)
