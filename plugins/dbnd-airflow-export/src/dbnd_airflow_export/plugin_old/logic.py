import logging

from dbnd._core.utils.timezone import utcnow
from dbnd_airflow_export.dag_operations import get_dags, load_dags_models
from dbnd_airflow_export.datetime_utils import pendulum_max_dt
from dbnd_airflow_export.plugin_old.db_queries import (
    get_completed_task_instances_and_dag_runs,
    get_dag_runs_within_time_window,
    get_dag_runs_without_end_date,
    get_incomplete_task_instances_from_completed_dag_runs,
    get_task_instances_without_end_date,
)
from dbnd_airflow_export.plugin_old.metrics import measure_time
from dbnd_airflow_export.plugin_old.model import ExportData


def get_dags_list_only(session, dagbag, dag_ids):
    """
    This function returns all dags from Airflow but in their raw form - No tasks or source code are attached.
    Please do not use it in cases where either tasks or source code are required.
    """
    load_dags_models(session)
    dags_list = get_dags(
        dagbag=dagbag, include_task_args=False, dag_ids=dag_ids, raw_data_only=True
    )
    ed = ExportData(
        task_instances=[], dag_runs=[], dags=dags_list, since=str(utcnow()),
    )

    return ed


@measure_time
def get_complete_data(
    since,
    dag_ids,
    dagbag,
    quantity,
    include_logs,
    include_task_args,
    include_xcom,
    session,
):
    """
    This function returns the following data:
    1. The first task instances that ended after since (limited by quantity). Also get their dag runs.
    2. Dag runs that don't have task instances, but whose end_date is after since and before end_date of the most
    recent of the task instances.
    3. All Dags of the Dag runs that we found in steps 1 and 2
    """
    load_dags_models(session)

    logging.info("Trying to query completed task instances and dagruns from %s" % since)

    task_instances, dag_runs = get_completed_task_instances_and_dag_runs(
        since, dag_ids, quantity, dagbag, include_logs, include_xcom, session
    )
    logging.info("%d task instances were found." % len(task_instances))

    task_end_dates = [
        task.end_date for task in task_instances if task.end_date is not None
    ]
    if not task_end_dates or not quantity or len(task_instances) < quantity:
        dag_run_end_date = pendulum_max_dt
    else:
        dag_run_end_date = max(task_end_dates)

    dag_runs |= get_dag_runs_within_time_window(
        since, dag_run_end_date, dag_ids, quantity, session
    )
    logging.info("%d dag runs were found." % len(dag_runs))

    if not dag_ids:
        dag_ids = set(dag_run.dag_id for dag_run in dag_runs)

    dags_list = get_dags(
        dagbag=dagbag,
        include_task_args=include_task_args,
        dag_ids=dag_ids,
        raw_data_only=False,
    )

    logging.info(
        "Returning {} task instances, {} dag runs, {} dags".format(
            len(task_instances), len(dag_runs), len(dags_list)
        )
    )

    ed = ExportData(
        task_instances=task_instances, dag_runs=dag_runs, dags=dags_list, since=since,
    )

    return ed


@measure_time
def get_incomplete_data_type_1(
    since, dag_ids, dagbag, max_quantity, include_task_args, offset, session
):
    """
    This function returns the following data:
    1. Task instances with end_date=None from dag runs that finished running (their end_date is not None).
    It brings only the first dag runs (limited by max_quantity) that ended after since.
    2. All Dags of the Dag runs that we found in step 1
    """
    load_dags_models(session)

    logging.info(
        "Trying to query incomplete task instances from complete dagruns from %s"
        % since
    )

    task_instances, dag_runs = get_incomplete_task_instances_from_completed_dag_runs(
        since, dag_ids, dagbag, max_quantity, offset, session
    )

    if not dag_ids:
        dag_ids = set(dag_run.dag_id for dag_run in dag_runs)

    dags_list = get_dags(
        dagbag=dagbag,
        include_task_args=include_task_args,
        dag_ids=dag_ids,
        raw_data_only=False,
    )

    logging.info(
        "Returning {} task instances, {} dag runs, {} dags".format(
            len(task_instances), len(dag_runs), len(dags_list)
        )
    )

    ed = ExportData(
        task_instances=task_instances, dag_runs=dag_runs, dags=dags_list, since=since,
    )

    return ed


@measure_time
def get_incomplete_data_type_2(
    since, dag_ids, dagbag, quantity, include_task_args, incomplete_offset, session
):
    """
    This function returns the following data:
    1. Task instances with end_date=None from dag runs that are running (their end_date=None). Also their dag runs.
    2. All Dags of the Dag runs that we found in step 1
    Important: since pagination is used, the same parameters can return different results at different times.
    """
    load_dags_models(session)

    logging.info(
        "Trying to query incomplete task instances and dagruns from %s" % since
    )

    task_instances, dag_runs = get_task_instances_without_end_date(
        since=since,
        dag_ids=dag_ids,
        dagbag=dagbag,
        session=session,
        incomplete_offset=incomplete_offset,
        page_size=quantity,
    )
    logging.info(
        "Found {} task instances with no end_date from {} dag runs".format(
            len(task_instances), len(dag_runs)
        )
    )

    dag_runs_without_date = get_dag_runs_without_end_date(
        since=since,
        dag_ids=dag_ids,
        session=session,
        incomplete_offset=incomplete_offset,
        page_size=quantity,
    )
    logging.info("Found {} dag runs with no end_date".format(len(dag_runs)))

    dag_runs |= dag_runs_without_date

    if not dag_ids:
        dag_ids = set(dag_run.dag_id for dag_run in dag_runs)

    dags_list = get_dags(
        dagbag=dagbag,
        include_task_args=include_task_args,
        dag_ids=dag_ids,
        raw_data_only=False,
    )

    logging.info(
        "Returning {} task instances, {} dag runs, {} dags".format(
            len(task_instances), len(dag_runs), len(dags_list)
        )
    )

    ed = ExportData(
        task_instances=task_instances, dag_runs=dag_runs, dags=dags_list, since=since,
    )

    return ed
