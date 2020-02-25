# PLEASE DO NOT MOVE/RENAME THIS FILE, IT'S SERIALIZED INTO AIRFLOW DB
import itertools
import logging
import sys

from subprocess import list2cmdline
from typing import List

from airflow import DAG, settings
from airflow.models import BaseOperator
from airflow.utils.decorators import apply_defaults

from dbnd._core.current import try_get_databand_context
from dbnd._core.decorator.schemed_result import ResultProxyTarget
from dbnd._core.run.databand_run import DatabandRun, new_databand_run
from dbnd._core.task.task import Task
from dbnd._core.utils.json_utils import convert_to_safe_types
from dbnd._core.utils.uid_utils import get_job_run_uid, get_task_run_uid


logger = logging.getLogger(__name__)


def is_in_airflow_dag_build_context():
    from dbnd_airflow.dbnd_task_executor.airflow_operators_catcher import (
        DatabandOpCatcherDag,
    )

    return settings.CONTEXT_MANAGER_DAG and not isinstance(
        settings.CONTEXT_MANAGER_DAG, DatabandOpCatcherDag
    )


def build_task_at_airflow_dag_context(task_cls, call_args, call_kwargs):
    task = task_cls(*call_args, **call_kwargs)
    airflow_op = _build_airflow_dag_dbnd_operator_from_task(task)

    from airflow import settings

    dag = settings.CONTEXT_MANAGER_DAG

    for arg in itertools.chain(call_args, call_kwargs.values()):
        if isinstance(arg, StrXComResultWithOperator):
            upstream_task = dag.task_dict[arg.task_id]
            airflow_op.set_upstream(upstream_task)
    # we are in inline debug mode -> we are going to execute the task
    # we are in the band
    # and want to return result of the object
    if task.task_definition.single_result_output:
        if isinstance(task.result, ResultProxyTarget):
            return [StrXComResultWithOperator.build(task, n) for n in task.result.names]
        if task.result:
            return StrXComResultWithOperator.build(task, "result")
        # we have multiple outputs ( result, another output.. ) -> just return airflow operator
    return airflow_op


class AirflowDagDbndOperator(BaseOperator):
    """
    This is the Airflow operator that is created for every Databand Task


    it assume all tasks inputs coming from other airlfow tasks are in the format

    """

    ui_color = "#ffefeb"

    @apply_defaults
    def __init__(self, dbnd_task, dbnd_task_type, dbnd_task_id, **kwargs):
        template_fields = kwargs.pop("template_fields", None)
        super(AirflowDagDbndOperator, self).__init__(**kwargs)
        self._task_type = dbnd_task_type
        self.dbnd_task_id = dbnd_task_id

        # make a copy
        self.template_fields = list(self.template_fields)  # type: List[str]
        if template_fields:
            self.template_fields.extend(template_fields)
        self.dbnd_fields = list()

        user_params = dbnd_task._params.get_param_values(user_only=True)
        for p_def, p_value in user_params:
            p_name = p_def.name
            if p_name == "task_band":
                continue
            self.dbnd_fields.append(p_name)
            self.template_fields.append(p_name)
            setattr(self, p_name, str(p_value))

    @property
    def task_type(self):
        # we want to override task_type so we can have unique types for every Databand task
        v = getattr(self, "_task_type", None)
        if v:
            return v
        return BaseOperator.task_type.fget(self)

    def execute(self, context):
        logger.info("Running dbnd task from airflow operator %s", self.task_id)

        new_kwargs = {}
        for p_name in self.dbnd_fields:
            new_kwargs[p_name] = getattr(self, p_name, None)
        logger.error("Running with %s ", new_kwargs)
        dc = try_get_databand_context()

        dbnd_task = dc.task_instance_cache.get_task_by_id(self.dbnd_task_id)
        current_dbnd_task = dbnd_task.clone(**new_kwargs)
        current_dbnd_task._task_submit()

        logger.error("Finished to run %s", self)
        # this will go to xcom
        result = None
        if isinstance(current_dbnd_task.result, ResultProxyTarget):
            result = current_dbnd_task.result.as_dict()
        else:
            result = current_dbnd_task.result
        # if current_dbnd_task.task_definition.single_result_output:
        #     return result
        # we need to get all results here
        return {"result": result}

    def on_kill(self):
        from dbnd_airflow.dbnd_task_executor.dbnd_execute import dbnd_operator__kill

        return dbnd_operator__kill(self)


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


def _build_airflow_dag_dbnd_operator_from_task(task):
    # type: (Task)-> AirflowDagDbndOperator

    params = convert_to_safe_types(
        {p.name: value for p, value in task._params.get_param_values()}
    )
    op_kwargs = task.task_airflow_op_kwargs or {}

    op = AirflowDagDbndOperator(
        task_id=task.task_name,
        dbnd_task=task,
        dbnd_task_type=task.get_task_family(),
        dbnd_task_id=task.task_id,
        params=params,
        **op_kwargs
    )

    if task.task_retries is not None:
        op.retries = task.task_retries
        op.retry_delay = task.task_retry_delay

    task.ctrl.airflow_op = op
    # set_af_operator_doc_md(task_run, op)
    return op


class StrXComResultWithOperator(str):
    def __new__(cls, value, task_id):
        # explicitly only pass value to the str constructor
        obj = super(StrXComResultWithOperator, cls).__new__(cls, value)
        obj.task_id = task_id
        return obj

    @classmethod
    def build(cls, task, name):
        op = task.ctrl.airflow_op
        xcom_path = "{{task_instance.xcom_pull('%s')['%s']}}" % (op.task_id, name)
        return cls(xcom_path, op.task_id)


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
