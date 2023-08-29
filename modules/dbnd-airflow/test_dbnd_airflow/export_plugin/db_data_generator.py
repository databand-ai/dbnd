# © Copyright Databand.ai, an IBM Company 2022

from datetime import datetime

import attr

from airflow.models import DagModel, DagRun, Log, TaskInstance
from airflow.utils.db import provide_session

from dbnd._core.utils.timezone import utcnow
from dbnd_airflow.export_plugin.utils import AIRFLOW_VERSION_2


@attr.s
class FakeTask(object):
    dag_id = attr.ib(default=None)  # type: str
    task_id = attr.ib(default=None)  # type: str
    owner = attr.ib(default=None)  # type: str
    queue = attr.ib(default=None)  # type: str
    pool = attr.ib(default=1)  # type: int
    pool_slots = attr.ib(default=1)  # type: int
    priority_weight_total = attr.ib(default=None)  # type: str
    run_as_user = attr.ib(default=None)  # type: str
    retries = attr.ib(default=None)  # type: str
    executor_config = attr.ib(default=None)  # type: str
    task_type = attr.ib(default=None)  # type str


@attr.s
class FakeTaskInstance(object):
    dag_id = attr.ib(default=None)  # type: str
    task_id = attr.ib(default=None)  # type: str
    execution_date = attr.ib(default=None)  # type: datetime
    task = attr.ib(default=None)  # type: FakeTask
    map_index = attr.ib(default=None)  # airflow 2.3+


@provide_session
def insert_dag_runs(
    session,
    dag_id="plugin_test_dag",
    dag_runs_count=1,
    task_instances_per_run=0,
    state="success",
    with_log=False,
):
    for i in range(dag_runs_count):
        execution_date = utcnow()

        dag_run = DagRun()
        dag_run.dag_id = dag_id
        dag_run.execution_date = execution_date
        dag_run._state = state
        if AIRFLOW_VERSION_2:
            from airflow.utils.types import DagRunType

            dag_run.run_id = DagRun.generate_run_id(DagRunType.MANUAL, execution_date)
            dag_run.run_type = ""
        else:
            dag_run.run_id = f"manual__{execution_date.isoformat()}"

        session.add(dag_run)
        session.commit()
        if with_log:
            task_instance = FakeTaskInstance()
            task_instance.dag_id = dag_id
            task_instance.task_id = "task"
            task_instance.execution_date = execution_date
            task = FakeTask()
            task.owner = "Airflow"
            task_instance.task = task
            log = Log("success", task_instance)
            session.add(log)

        for j in range(task_instances_per_run):
            task = FakeTask(dag_id=dag_id, task_id="task{}".format(j))
            task_instance = TaskInstance(task, execution_date, state="success")
            session.add(task_instance)

    session.commit()


@provide_session
def set_dag_is_paused(session, is_paused):
    session.query(DagModel).update({DagModel.is_paused: is_paused})
    session.commit()
