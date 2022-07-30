# © Copyright Databand.ai, an IBM Company 2022

from dbnd._core.errors import DatabandError, DatabandRuntimeError
from dbnd._core.errors.friendly_error.helpers import _run_name
from dbnd._core.utils.console_utils import bold, cyan, red, underline


def dataflow_pipeline_not_set(task):
    return DatabandRuntimeError(
        "dataflow_pipeline at {task} is None. Can't wait on dataflow job completion.".format(
            task=_run_name(task)
        ),
        help_msg="Please set task.pipeline first at '{task}' or change task.dataflow_wait_until_finish flag".format(
            task=_run_name(task)
        ),
    )


def logger_format_for_databand_error(error):
    # type: (DatabandError) -> str

    title_error = red(underline(bold("Error: ")))
    title_exception = red(underline(bold("Nested exception: ")))
    title_helper = cyan(underline(bold("Help: ")))

    full_err_message = "\n\n {title_error}{msg_error} \n {title_exception}{msg_exception} \n {title_helper}\n{msg_helper}\n\n".format(
        title_error=title_error,
        msg_error=error,
        title_exception=title_exception,
        msg_exception=getattr(error, "nested_exceptions", ""),
        title_helper=title_helper,
        msg_helper=getattr(error, "help_msg", ""),
    )
    return full_err_message
