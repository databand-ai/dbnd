import inspect
import logging

from typing import Optional

import attr

from dbnd._core.errors.errors_utils import UserCodeDetector


logger = logging.getLogger(__name__)


@attr.s
class TaskSourceCode(object):
    task_source_code = attr.ib(default=None)  # type: Optional[str]
    task_module_code = attr.ib(default=None)  # type: Optional[str]
    task_source_file = attr.ib(default=None)  # type: Optional[str]

    @classmethod
    def from_callable(cls, callable):
        task_source_code = _get_source_code(callable)
        task_module_code = _get_module_source_code(callable)
        task_source_file = _get_source_file(callable)

        return TaskSourceCode(
            task_source_code=task_source_code,
            task_module_code=task_module_code,
            task_source_file=task_source_file,
        )

    @classmethod
    def from_class(cls, task_class):
        task_source_code = _get_source_code(task_class)
        task_module_code = _get_module_source_code(task_class)
        task_source_file = _get_source_file(task_class.__class__)

        return TaskSourceCode(
            task_source_code=task_source_code,
            task_module_code=task_module_code,
            task_source_file=task_source_file,
        )

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
