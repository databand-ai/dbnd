import logging

from dbnd._core.decorator.decorated_task import (
    DecoratedPipelineTask,
    DecoratedPythonTask,
)
from dbnd._core.decorator.task_cls_builder import _task_decorator
from dbnd._core.parameter.parameter_builder import parameter


logger = logging.getLogger(__name__)
_default_output = parameter.output.pickle[object]


def task(*args, **kwargs):
    kwargs.setdefault("_task_type", DecoratedPythonTask)
    kwargs.setdefault("_task_default_result", _default_output)
    return _task_decorator(*args, **kwargs)


def pipeline(*args, **kwargs):
    kwargs.setdefault("_task_type", DecoratedPipelineTask)
    kwargs.setdefault("_task_default_result", parameter.output)
    return _task_decorator(*args, **kwargs)


band = pipeline
