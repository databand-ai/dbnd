import itertools
import logging

from typing import Type

import luigi

from dbnd import Task, parameter
from dbnd._core.decorator.task_decorator_spec import build_task_decorator_spec
from dbnd._core.errors import TaskClassNotFoundException
from dbnd._core.task_build.task_metaclass import TaskMetaclass
from dbnd._core.task_build.task_registry import get_task_registry
from dbnd_luigi.luigi_params import extract_luigi_params
from dbnd_luigi.luigi_target import extract_targets


logger = logging.getLogger(__name__)


class _LuigiTask(Task):
    _luigi_task_completed = parameter(system=True, default=False)[bool]

    def _complete(self):
        return self._luigi_task_completed


def wrap_luigi_task(luigi_task):
    # type: ( luigi.Task)-> _LuigiTask
    """
    Wraps the original luigi task with the correct DBND Task
    """
    dbnd_task_cls = _get_task_cls(luigi_task)

    return dbnd_task_cls(
        task_name=luigi_task.get_task_family(), **luigi_task.param_kwargs
    )


def _get_task_cls(luigi_task):
    # type: (luigi.Task) -> Type[_LuigiTask]
    """
    Returns the right dbnd-luigi class wrapper base on existing relevant tracker or by creating new one
    """
    task_family = luigi_task.get_task_family()

    registry = get_task_registry()
    try:
        dbnd_task_cls = registry.get_task_cls(str(task_family))
    except TaskClassNotFoundException:
        dbnd_task_cls = _build_new_task_cls(luigi_task)
        logger.info("Creating new class %s", task_family)

    return dbnd_task_cls


def _build_new_task_cls(luigi_task):
    # type: (luigi.Task) -> Type[_LuigiTask]
    task_family = luigi_task.get_task_family()
    _classdict = _build_luigi_classdict(luigi_task)
    dbnd_task_cls = TaskMetaclass(str(task_family), (_LuigiTask,), _classdict)
    return dbnd_task_cls


def _build_luigi_classdict(luigi_task):
    """
    build a classdict as needed in the creation of TaskMetaclass
    """
    luigi_task_cls = luigi_task.__class__
    task_family = luigi_task.get_task_family()

    attributes = dict(
        _conf__task_family=task_family,
        _conf__decorator_spec=build_task_decorator_spec(
            class_or_func=luigi_task_cls,
            decorator_kwargs={},
            default_result=parameter.output.pickle[object],
        ),
        __doc__=luigi_task_cls.__doc__,
        __module__=luigi_task_cls.__module__,
    )
    attributes.update(extract_luigi_params(luigi_task))
    attributes.update(_extract_targets_dedup(luigi_task, attributes))
    return attributes


def _extract_targets_dedup(luigi_task, attributes):
    known_attribute = set(attributes.keys())
    for name, target in extract_targets(luigi_task):
        param_name = _get_available_name(
            name, lambda suggested: suggested not in known_attribute
        )
        known_attribute.add(param_name)
        yield param_name, target


def _get_available_name(original, cond):
    """
    Iterate over possibles names for original string, adding a different number as suffix at each iteration.
    The new name is available by checking if it pass the condition.
    """
    for c in itertools.chain([""], itertools.count()):
        suffix = str(c)
        suggested = original + suffix
        if cond(suggested):
            return suggested
