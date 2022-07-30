# © Copyright Databand.ai, an IBM Company 2022

import typing

from dbnd._core.tracking.tracking_info_convertor import build_task_run_info
from dbnd._core.utils.json_utils import convert_to_safe_types
from dbnd.api.serialization.task import TaskRunInfoSchema
from dbnd_airflow_contrib.dbnd_operator import DbndOperator


if typing.TYPE_CHECKING:
    from dbnd._core.task_run.task_run import TaskRun


def build_dbnd_operator_from_taskrun(task_run):
    # type: (TaskRun)-> DbndOperator

    task = task_run.task
    params = convert_to_safe_types(
        {p.name: value for p, value in task._params.get_params_with_value()}
    )
    op_kwargs = task.task_airflow_op_kwargs or {}

    op = DbndOperator(
        task_id=task_run.task_af_id,
        dbnd_task_type=task.get_task_family(),
        dbnd_task_id=task.task_id,
        params=params,
        **op_kwargs
    )

    if task.task_retries is not None:
        op.retries = task.task_retries
        op.retry_delay = task.task_retry_delay

    task.ctrl.airflow_op = op
    set_af_operator_doc_md(task_run, op)
    return op


_task_run_info_schema = TaskRunInfoSchema()


def set_af_operator_doc_md(task_run, airflow_op):
    op = airflow_op
    op.doc_md = (
        "### Databand Info\n"
        "* **Tracker**: [{0}]({0})\n"
        "* **TaskRun UID**: {1}\n"
        "* **Run Name**: {2}\n"
        "* **Run UID**: {3}\n".format(
            task_run.task_tracker_url,
            task_run.task_run_uid,
            task_run.run.name,
            task_run.run.run_uid,
        )
    )
    op.dbnd_task_run_uid = task_run.task_run_uid
    op.dbnd_run_uid = task_run.run.run_uid
    op.dbnd_task_tracker_url = task_run.task_tracker_url
    op.dbnd_task_run_info = _task_run_info_schema.dump(
        build_task_run_info(task_run)
    ).data
