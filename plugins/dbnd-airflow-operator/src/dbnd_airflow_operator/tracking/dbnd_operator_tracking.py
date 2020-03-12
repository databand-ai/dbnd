# PLEASE DO NOT MOVE/RENAME THIS FILE, IT'S SERIALIZED INTO AIRFLOW DB

from dbnd._core.current import try_get_databand_context
from dbnd._core.run.databand_run import DatabandRun, new_databand_run
from dbnd._core.utils.uid_utils import get_job_run_uid, get_task_run_uid
from dbnd_airflow_operator.airflow_dag_dbnd_operator import build_dag_task


def _dbnd_track_and_execute(task, airflow_op, context):
    dag = context["dag"]
    dag_id = dag.dag_id
    run_uid = get_job_run_uid(dag_id=dag_id, execution_date=context["execution_date"])
    task_run_uid = get_task_run_uid(run_uid, airflow_op.task_id)

    dag_task = build_dag_task(dag)
    dc = try_get_databand_context()
    # create databand run
    with new_databand_run(
        context=dc,
        task_or_task_name=dag_task,
        run_uid=run_uid,
        existing_run=False,
        job_name=dag.dag_id,
    ) as dr:  # type: DatabandRun
        root_task_run_uid = get_task_run_uid(run_uid, dag_id)
        dr._init_without_run(root_task_run_uid=root_task_run_uid)

        # self._start_taskrun(dr.driver_task_run)
        # self._start_taskrun(dr.root_task_run)

        tr = dr.create_dynamic_task_run(
            task, dr.local_engine, _uuid=task_run_uid, task_af_id=airflow_op.task_id
        )
        tr.runner.execute(airflow_context=context)
