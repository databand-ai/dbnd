# PLEASE DO NOT MOVE/RENAME THIS FILE, IT'S SERIALIZED INTO AIRFLOW DB
import sys

from airflow import DAG

from dbnd import Task
from dbnd._core.current import try_get_databand_context
from dbnd._core.run.databand_run import DatabandRun, new_databand_run
from dbnd._core.utils.basics.cmd_line_builder import list2cmdline
from dbnd._core.utils.uid_utils import get_job_run_uid, get_task_run_uid


def build_dag_task(dag):
    # type: (DAG) -> Task

    # create "root task" with default name as current process executable file name

    class InplaceTask(Task):
        _conf__task_family = dag.dag_id

    try:
        if dag.fileloc:
            dag_code = open(dag.fileloc, "r").read()
            InplaceTask.task_definition.task_source_code = dag_code
            InplaceTask.task_definition.task_module_code = dag_code
    except Exception:
        pass

    dag_task = InplaceTask(task_version="now", task_name=dag.dag_id)
    dag_task.task_is_system = True
    dag_task.task_meta.task_command_line = list2cmdline(sys.argv)
    dag_task.task_meta.task_functional_call = "bash_cmd(args=%s)" % repr(sys.argv)
    return dag_task


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
