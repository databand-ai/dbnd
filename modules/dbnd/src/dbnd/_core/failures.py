# © Copyright Databand.ai, an IBM Company 2022

import logging
import subprocess
import sys

from dbnd._core.configuration.dbnd_config import config
from dbnd._core.context import bootstrap
from dbnd._core.errors import (
    DatabandConfigError,
    DatabandError,
    DatabandRunError,
    DatabandRuntimeError,
    DatabandSystemError,
    friendly_error,
    get_help_msg,
    show_exc_info,
)
from dbnd._core.errors.base import DatabandRunError
from dbnd._core.errors.errors_utils import (
    get_user_frame_info_str,
    nested_exceptions_str,
)
from dbnd._core.utils import console_utils, seven
from dbnd._core.utils.basics.format_exception import format_exception_as_str
from dbnd._core.utils.basics.helpers import indent
from dbnd._core.utils.string_utils import truncate_msg


ERROR_MESSAGE_HEAD_SIZE = 2048
ERROR_MESSAGE_TAIL_SIZE = 2048

logger = logging.getLogger(__name__)


def get_databand_error_message(ex, args=None, sys_exit=True):
    args = args or sys.argv
    please_report = False
    print_source = True

    if isinstance(ex, DatabandRunError):
        # we already printed all information!
        return (
            "There is an error! Your run has failed!",
            DatabandExitCodes.execution_failed,
        )

    if isinstance(ex, DatabandRuntimeError):
        exit_code = DatabandExitCodes.execution_failed
    elif isinstance(ex, DatabandConfigError):
        exit_code = DatabandExitCodes.configuration_error
    elif isinstance(ex, DatabandSystemError):
        exit_code = DatabandExitCodes.error
        please_report = True
    elif isinstance(ex, DatabandError):
        exit_code = DatabandExitCodes.error
    elif ex.__class__.__name__ == "NoCredentialsError":  # aws
        exit_code = DatabandExitCodes.configuration_error
        ex = friendly_error.config.no_credentials()
        print_source = False
    else:
        please_report = True
        exit_code = DatabandExitCodes.unknown_error

    msg = str(ex)

    extra_msg_lines = []

    nested_exceptions = nested_exceptions_str(ex)
    if nested_exceptions:
        extra_msg_lines.append("Caused by: \n%s\n" % indent(nested_exceptions, "\t"))

    help_msg = get_help_msg(ex)
    if help_msg:
        extra_msg_lines.append(" Help: \n%s\n" % indent(help_msg, "\t"))

    user_frame_info_str = get_user_frame_info_str(ex)
    if user_frame_info_str and print_source:
        extra_msg_lines.append("Source: \n%s\n" % indent(user_frame_info_str, "\t"))

    # if we crashed before finishing bootstrap we probably want to see the full trace, and we could have failed during config init so the verbose flag does nothing
    if (
        show_exc_info(ex)
        or config.getboolean("databand", "verbose")
        or not bootstrap._dbnd_bootstrap
    ):
        error_info = sys.exc_info()
        extra_msg_lines.append(format_exception_as_str(error_info))

    msg = truncate_msg(msg, ERROR_MESSAGE_HEAD_SIZE, ERROR_MESSAGE_TAIL_SIZE)

    if please_report:
        extra_msg_lines.append(
            " Please report it to support@databand.ai or appropriate slack channel!"
        )
    msg = (
        "There is an error! Your run has failed with {exc_type}\n"
        "{sep}\n"
        " Command line: {command_line}\n"
        " Failure:\n{msg}\n\n"
        "{extra_msg}\n"
        "{sep}\n"
        "".format(
            sep=console_utils.error_separator(),
            command_line=subprocess.list2cmdline(args or []),
            sep_small=console_utils.error_separator_small(),
            msg=console_utils.bold(indent(msg, "\t")),
            exc_type=ex.__class__.__name__,
            extra_msg="\n ".join(extra_msg_lines),
        )
    )
    return msg, exit_code


@seven.contextlib.contextmanager
def dbnd_handle_errors(exit_on_error=True):
    """Handles dbnd Exceptions."""
    try:
        yield
    except Exception as ex:
        if not hasattr(ex, "_dbnd_error_handled"):
            msg, code = get_databand_error_message(ex=ex, args=None)
            logger.error(msg)
            setattr(ex, "_dbnd_error_handled", True)
        if exit_on_error:
            sys.exit(1)
        raise


class DatabandExitCodes(object):
    unknown_error = 4
    error = 5
    databand_system = 6
    task_runtime = 7
    configuration_error = 8
    execution_failed = 10
