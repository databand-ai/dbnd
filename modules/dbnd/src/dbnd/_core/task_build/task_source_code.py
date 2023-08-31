# © Copyright Databand.ai, an IBM Company 2022

import inspect
import logging

from typing import Callable, Optional, Type

import attr

from dbnd._core.errors.errors_utils import UserCodeDetector
from dbnd._core.task.base_task import _BaseTask


logger = logging.getLogger(__name__)


def _hide_or_show_by_config(source):
    from dbnd._core.settings import TrackingConfig

    tracking_config = TrackingConfig.from_databand_context()
    if tracking_config.track_source_code:
        return source
    else:
        return None


@attr.s
class TaskSourceCode(object):
    _task_source_code = attr.ib(default=None)  # type: Optional[str]
    _task_module_code = attr.ib(default=None)  # type: Optional[str]
    _task_source_file = attr.ib(default=None)  # type: Optional[str]

    @property
    def task_source_code(self):
        return _hide_or_show_by_config(source=self._task_source_code)

    @task_source_code.setter
    def task_source_code(self, value):
        self._task_source_code = value

    @property
    def task_module_code(self):
        return _hide_or_show_by_config(source=self._task_module_code)

    @task_module_code.setter
    def task_module_code(self, value):
        self._task_module_code = value

    @property
    def task_source_file(self):
        return _hide_or_show_by_config(source=self._task_source_file)

    @property
    def task_source_file_for_internal_usage(self):
        return self._task_source_file

    @task_source_file.setter
    def task_source_file(self, value):
        self._task_source_file = value

    @classmethod
    def from_callable(cls, callable):
        # type: (Callable) -> TaskSourceCode
        task_source_code = _get_source_code(callable)
        task_module_code = _get_module_source_code(callable)
        task_source_file = _get_source_file(callable)

        return TaskSourceCode(
            task_source_code=task_source_code,
            task_module_code=task_module_code,
            task_source_file=task_source_file,
        )

    @classmethod
    def from_class(cls, class_obj):
        # type: (type) -> TaskSourceCode
        task_source_code = _get_source_code(class_obj)
        task_module_code = _get_module_source_code(class_obj)
        task_source_file = _get_source_file(class_obj.__class__)

        return TaskSourceCode(
            task_source_code=task_source_code,
            task_module_code=task_module_code,
            task_source_file=task_source_file,
        )

    @classmethod
    def from_task_class(cls, task_class):
        # type: (Type[_BaseTask]) -> TaskSourceCode
        # TODO: FIX TASK_DECORATOR
        if task_class._conf__track_source_code:
            if hasattr(task_class, "task_decorator") and task_class.task_decorator:
                # this means we are inside a task_class of decorated function
                return cls.from_callable(task_class.task_decorator.class_or_func)
            else:
                return cls.from_class(task_class)
        else:
            return NO_SOURCE_CODE

    @classmethod
    def from_callstack(cls):
        try:
            user_frame = UserCodeDetector.build_code_detector().find_user_side_frame(
                user_side_only=True
            )
            if user_frame:
                module_code = open(user_frame.filename).read()
                return TaskSourceCode(
                    task_module_code=module_code, task_source_file=user_frame.filename
                )
        except Exception as ex:
            logger.debug("Failed to find source code: %s", str(ex))
        return NO_SOURCE_CODE


NO_SOURCE_CODE = TaskSourceCode()


def _get_source_code(item):
    try:
        import inspect

        return inspect.getsource(item)
    except (TypeError, OSError):
        logger.debug("Failed to task source for %s", item)
    except Exception:
        logger.debug("Error while getting task source")
    return "Error while getting source code"


def _get_module_source_code(item):
    try:
        return inspect.getsource(inspect.getmodule(item))
    except TypeError:
        logger.debug("Failed to module source for %s", item)
    except Exception:
        logger.exception("Error while getting module source")
    return "Error while getting source code"


def _get_source_file(item):
    try:
        return inspect.getfile(item).replace(".pyc", ".py")
    except Exception:
        logger.warning("Failed find a path of source code for task {}".format(item))
    return None
