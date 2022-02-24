import logging
import os
import re
import sys

from dbnd._core.current import is_verbose
from dbnd._core.log.external_exception_logging import log_exception_to_server
from dbnd._core.utils.basics.text_banner import safe_string
from dbnd._core.utils.project.project_fs import databand_lib_path, project_path


logger = logging.getLogger(__name__)

_inner_call_functions_regexp = re.compile(
    "|".join(
        [
            r"errors/errors_utils",
            r"_core/task_build",
            r"_core/utils",
            r"_core/failures",
            r"contextlib.py",
        ]
    )
)


def safe_value(value):
    if value is None:
        return value

    return safe_string(str(value), 500)


def get_help_msg(cur_ex):
    if getattr(cur_ex, "help_msg", None):
        return cur_ex.help_msg
    return ""


def get_user_frame_info_str(cur_ex):
    if getattr(cur_ex, "user_frame_info", None):
        return frame_info_to_str(cur_ex.user_frame_info)
    return ""


def show_exc_info(cur_ex):
    if not cur_ex:
        return 0
    if hasattr(cur_ex, "show_exc_info"):
        return getattr(cur_ex, "show_exc_info")

    # By default, show exception
    return 1


def _nested_exceptions_messages(ex, limit=None, current=""):
    nested_exceptions = getattr(ex, "nested_exceptions", [])

    if limit:
        limit -= 1
    messages = []

    from dbnd._core.utils.basics.helpers import indent

    for idx, cur_ex in enumerate(nested_exceptions, start=1):
        if cur_ex is None:
            continue
        ex_id = "{current}{idx}".format(current=current, idx=idx)
        message = "{ex_id}. {ex_type}: {msg} ".format(
            ex_id=ex_id, ex_type=type(cur_ex).__name__, msg=str(cur_ex)
        )
        help_msg = get_help_msg(cur_ex)
        if help_msg:
            message += "\n{help_msg}".format(
                help_msg=indent(help_msg, " " * len(ex_id))
            )
        user_frame_info_str = get_user_frame_info_str(cur_ex)
        if user_frame_info_str:
            message += "\n{user_frame_info}".format(
                user_frame_info=indent(user_frame_info_str, " " * len(ex_id))
            )

        messages.append(message)
        if limit is None or limit:
            messages.extend(
                _nested_exceptions_messages(
                    cur_ex, limit=limit, current="(%s)-> " % ex_id
                )
            )
    return messages


def nested_exceptions_str(ex, limit=None):
    messages = _nested_exceptions_messages(ex, limit=limit)

    return "\n".join(messages)


def frame_info_to_str(frame_info):
    if not frame_info:
        return ""
    result = []
    if frame_info.function == "build_dbnd_task":
        return ""
    result.append(
        '  File "{}", line {}, in {}\n'.format(
            frame_info.filename, frame_info.lineno, frame_info.function
        )
    )
    if frame_info.code_context:
        line = "\n".join(l.rstrip() for l in frame_info.code_context)
        result.append("    {}\n".format(line))
    return "".join(result)


def user_side_code(context=5):
    try:
        fi = UserCodeDetector.build_code_detector().find_user_side_frame(
            context=context
        )
        return frame_info_to_str(fi)
    except Exception:
        return "UNKNOWN"


class UserCodeDetector(object):
    def __init__(self, code_dir=None, system_code_dirs=None):
        # The directory we're interested in.
        if not code_dir:
            code_dir = os.getcwd()

        self._system_code_dirs = system_code_dirs or []

        self._system_code_dir = [s + os.path.sep for s in self._system_code_dirs]
        self._code_dir = code_dir

    def is_user_file(self, file):
        """
        Decide whether the file in the traceback is one in our code_dir or not.
        """

        if "site-packages" in file or "dist-packages" in file:
            return False

        if "dbnd_examples" in file:
            return True

        for system_code_dir in self._system_code_dirs:
            if file.startswith(system_code_dir):
                return False
        if file.startswith(self._code_dir):
            return True

        if sys.platform != "win32" and not file.startswith("/"):
            return True

        return False

    def _is_user_frame(self, frame_info, user_side_only=True):
        if not frame_info.filename:
            return False

        if _inner_call_functions_regexp.search(frame_info.filename.replace("\\", "/")):
            return False

        if not user_side_only:
            return True

        return self.is_user_file(frame_info.filename)

    def find_user_side_frame(self, depth=1, context=1, user_side_only=False):
        from inspect import getframeinfo

        frame = sys._getframe(depth)
        while frame:
            frame_info = getframeinfo(frame, context)
            if self._is_user_frame(frame_info, user_side_only):
                return frame_info

            frame = frame.f_back
        return None

    @classmethod
    def build_code_detector(cls, system_code_dirs=None):
        # should be called withing databand environment

        if system_code_dirs is None:
            system_code_dirs = [databand_lib_path(), databand_lib_path("../targets")]
        code_dir = project_path()
        return cls(code_dir=code_dir, system_code_dirs=system_code_dirs)


def log_exception(msg, ex, logger_=None, verbose=None, non_critical=False):
    logger_ = logger_ or logger
    log_exception_to_server()

    from dbnd._core.errors.base import DatabandError

    if verbose is None:
        verbose = is_verbose()

    if verbose:
        # just show the exception
        logger_.exception(msg)
        return

    if non_critical:
        logger_.debug(msg + ": %s" % str(ex))
        return

    if isinstance(ex, DatabandError):
        # msg = "{msg}:{ex}".format(msg=msg, ex=ex)
        logger_.error(msg + ": %s" % str(ex))
    else:
        # should we? let's show the exception for now so we can debug
        logger_.exception(msg)
