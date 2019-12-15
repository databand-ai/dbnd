from __future__ import absolute_import, division, print_function, unicode_literals

import logging
import typing

from datetime import datetime

from airflow.models import DAG, DagPickle, DagRun, TaskInstance
from airflow.utils import timezone
from airflow.utils.db import provide_session
from airflow.utils.state import State
from sqlalchemy.orm import Session, make_transient

from dbnd import new_dbnd_context
from dbnd._core.utils.basics.pickle_non_pickable import ready_for_pickle
from dbnd_airflow.airflow_override import DbndAirflowTaskInstance
from dbnd_airflow.web.databand_versioned_dagbag import DAG_UNPICKABLE_PROPERTIES


if typing.TYPE_CHECKING:
    from dbnd._core.run.databand_run import DatabandRun

logger = logging.getLogger(__name__)


def create_pickled_dag(dag, execution_date, session):
    with ready_for_pickle(dag, DAG_UNPICKABLE_PROPERTIES) as pickable_dag:
        # now we are running under no_unpickable

        # pickle for user dag
        dp = DagPickle(dag=pickable_dag)
        session.add(dp)

        session.commit()
    dag.pickle_id = dp.id
    dag.last_pickled = timezone.utcnow()


@provide_session
def create_dagrun_from_dbnd_run(
    databand_run,
    af_dag,
    execution_date,
    state=State.RUNNING,
    external_trigger=False,
    conf=None,
    session=None,
):
    """
    Create new DagRun and all relevant TaskInstances
    """
    dag = af_dag
    create_pickled_dag(dag, execution_date, session=session)

    dagrun = (
        session.query(DagRun)
        .filter(DagRun.dag_id == dag.dag_id, DagRun.execution_date == execution_date)
        .first()
    )
    if dagrun is None:
        dagrun = DagRun(
            run_id=databand_run.run_id,
            execution_date=execution_date,
            start_date=af_dag.start_date,
            _state=state,
            external_trigger=external_trigger,
            dag_id=dag.dag_id,
            conf=conf,
        )
        session.add(dagrun)

    # DagStat.set_dirty(dag_id=dag.dag_id, session=session)
    # set required transient field
    dagrun.dag = dag
    dagrun.run_id = databand_run.run_id

    # update_af_dagrun_with_current_run_info(dagrun, databand_run)

    session.commit()

    copy_dag_id = dagrun.dag_id
    copy_execution_date = dagrun.execution_date
    copy_run_id = dagrun.run_id

    make_transient(dagrun)

    dagrun.dag_id = copy_dag_id
    dagrun.execution_date = copy_execution_date
    dagrun.run_id = copy_run_id

    create_task_instances_from_dbnd_run(
        databand_run=databand_run,
        dag=dag,
        execution_date=execution_date,
        session=session,
    )

    return dagrun


@provide_session
def create_task_instances_from_dbnd_run(
    databand_run, dag, execution_date, session=None
):
    # type: (DatabandRun, DAG, datetime, Session)-> None

    # # create the associated task instances
    # # state is None at the moment of creation
    # dagrun.verify_integrity(session=session)
    # fetches [TaskInstance] again
    # tasks_skipped = databand_run.tasks_skipped

    # we can find a source of the completion, but also,
    # sometimes we don't know the source of the "complete"
    # completed_by_run_id = find_task_run_instances(databand_run.tasks_completed)
    TI = DbndAirflowTaskInstance
    tis = (
        session.query(TI)
        .filter(TI.dag_id == dag.dag_id, TI.execution_date == execution_date)
        .all()
    )
    tis = {ti.task_id: ti for ti in tis}

    for af_task in dag.tasks:
        ti = tis.get(af_task.task_id)
        if ti is None:
            ti = DbndAirflowTaskInstance(af_task, execution_date=execution_date)
            ti.start_date = timezone.utcnow()
            ti.end_date = timezone.utcnow()
            session.add(ti)
        task_run = databand_run.get_task_run_by_af_id(af_task.task_id)

        executor_config = ti.executor_config
        executor_config["DatabandExecutor"] = {
            "dbnd_driver_dump": str(databand_run.driver_dump)
        }
        if dag.pickle_id:
            executor_config["DatabandExecutor"]["dag_pickle_id"] = dag.pickle_id

        # all tasks part of the backfill are scheduled to dagrun
        if task_run.is_reused:
            # this task is completed and we don't need to run it anymore
            ti.state = State.SUCCESS

    session.commit()


def create_trackable_dagrun_from_af_dag(
    run_id,
    state,
    af_dag,
    execution_date=None,
    external_trigger=False,
    conf=None,
    session=None,
):
    # let create a run that wraps this dag
    # we are in scheduling mode:
    with new_dbnd_context(name="scheduler") as dc:
        from dbnd_airflow.dbnd_task_executor.airflow_operator_as_dbnd import (
            AirflowDagAsDbndTask,
        )

        root_task = AirflowDagAsDbndTask.build_dbnd_task_from_dag(af_dag)

        from dbnd._core.run.databand_run import new_databand_run

        # if we run in scheduler - we don't have DatabandRun ( as we in multi run context)
        with new_databand_run(context=dc, task_or_task_name=root_task) as current_dr:
            # workaround for now - no constructor params
            # we actually can't run driver - airflow runs are all run by scheduler
            current_dr.run_id = run_id
            current_dr.execution_date = execution_date
            current_dr._init_without_run()

            # TODO: ensure AirflowTaskExecutor?
            task_executor = current_dr.task_executor
            with task_executor.prepare_run():
                ready_af_dag = task_executor.airflow_dag
                current_dr.save_run()

                return create_dagrun_from_dbnd_run(
                    databand_run=current_dr,
                    af_dag=ready_af_dag,
                    execution_date=execution_date,
                    session=session,
                    state=state,
                    external_trigger=external_trigger,
                    conf=conf,
                )
