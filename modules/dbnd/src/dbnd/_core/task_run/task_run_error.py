import sys
import traceback

import attr

from dbnd._core.errors import DatabandError, get_help_msg
from dbnd._core.errors.errors_utils import (
    frame_info_to_str,
    nested_exceptions_str,
    show_exc_info,
)
from dbnd._core.tracking.schemas.tracking_info_objects import ErrorInfo
from dbnd._core.utils.timezone import utcnow


def task_call_source_to_str(task_call_source):
    result = []
    for frame_info in reversed(task_call_source):
        result.append(frame_info_to_str(frame_info))

    return "".join(result).rstrip()


@attr.s
class TaskRunError(object):
    exc_info = attr.ib()  # type: Tuple
    task_run = attr.ib()  # type: TaskRun
    traceback = attr.ib()  # type: str
    exc_time = attr.ib(default=utcnow())  # type: Datetime
    msg = attr.ib(default=None)  # type: str
    airflow_context = attr.ib(default=None)  # type: Dict

    @property
    def exception(self):
        # type:()-> Exception
        return self.exc_info[1]

    @classmethod
    def build_from_ex(self, ex, task_run, exc_info=None):
        exc_info = exc_info or sys.exc_info()
        return TaskRunError(
            exc_info=exc_info,
            traceback=traceback.format_exc(),
            task_run=task_run,
            airflow_context=task_run.airflow_context,
        )

    @classmethod
    def build_from_message(cls, task_run, msg, help_msg):
        """
        Builds TaskRunError from string
        TODO: very ugly hack, we need to support TaskRunError without exc_info
        :param task_run:
        :param msg:
        :param help_msg:
        :return: TaskRunError
        """
        try:
            raise DatabandError(
                msg, show_exc_info=False, help_msg=help_msg,
            )
        except DatabandError as ex:
            return TaskRunError(
                exc_info=sys.exc_info(),
                traceback=traceback.format_exc(),
                task_run=task_run,
                airflow_context=task_run.airflow_context,
            )

    def as_error_info(self):
        ex = self.exception
        task = self.task_run.task
        isolate = not task.task_definition.full_task_family.startswith("databand.")
        return ErrorInfo(
            msg=str(ex),
            help_msg=get_help_msg(ex),
            exc_type=self.exc_info[0],
            databand_error=isinstance(ex, DatabandError),
            nested=nested_exceptions_str(ex, limit=1),
            traceback=self.traceback,
            user_code_traceback=task.settings.log.format_exception_as_str(
                exc_info=self.exc_info, isolate=isolate
            ),
            show_exc_info=bool(show_exc_info(ex)),
        )
