from dbnd._core.errors import DatabandConfigError, DatabandRuntimeError
from dbnd._core.errors.friendly_error.helpers import _run_name


def dataflow_pipeline_not_set(task):
    return DatabandRuntimeError(
        "dataflow_pipeline at {task} is None. Can't wait on dataflow job completion.".format(
            task=_run_name(task)
        ),
        help_msg="Please set task.pipeline first at '{task}' or change task.dataflow_wait_until_finish flag".format(
            task=_run_name(task)
        ),
    )
