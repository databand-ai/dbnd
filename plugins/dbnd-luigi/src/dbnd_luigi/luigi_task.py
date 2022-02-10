import functools
import logging

from typing import Type

import luigi
import six

from dbnd._core.parameter.parameter_value import Parameters, ParameterValue
from dbnd._core.task.tracking_task import TrackingTask
from dbnd._core.task_build.task_definition import TaskDefinition
from dbnd._core.task_build.task_passport import TaskPassport
from dbnd._core.task_build.task_source_code import TaskSourceCode
from dbnd_luigi.luigi_params import extract_luigi_params


logger = logging.getLogger(__name__)


class _LuigiTask(TrackingTask):
    def __init__(self, **kwargs):
        super().__init__(**kwargs)
        self._completed = False

    def mark_completed(self):
        self._completed = True

    def _complete(self):
        return self._completed


def wrap_luigi_task(luigi_task):
    # type: ( luigi.Task)-> _LuigiTask
    """
    Wraps the original luigi task with the correct DBND Task
    Returns the right dbnd-luigi class wrapper base on existing relevant tracker or by creating new one
    """
    task_family = luigi_task.get_task_family()

    dbnd_tracking_task_definition = getattr(
        luigi_task, "_dbnd_tracking_task_definition", None
    )
    if not dbnd_tracking_task_definition:
        logger.info("Creating new class %s", task_family)
        dbnd_tracking_task_definition = _build_luigi_task_definition(
            luigi_task.__class__
        )
        setattr(
            luigi_task, "_dbnd_tracking_task_definition", dbnd_tracking_task_definition
        )

    param_values = []
    luigi_param_value_builder = functools.partial(
        ParameterValue, source="luigi_param", source_value=None
    )
    for name, param_definition in six.iteritems(
        dbnd_tracking_task_definition.task_param_defs
    ):
        pv = luigi_param_value_builder(
            parameter=param_definition,
            value=luigi_task.param_kwargs.get(param_definition.name),
        )
        param_values.append(pv)

    task_params = Parameters(source=luigi_task.task_id, param_values=param_values)
    dbnd_luigi_task = _LuigiTask(
        task_name=luigi_task.task_id,
        task_definition=dbnd_tracking_task_definition,
        task_params=task_params,
    )
    return dbnd_luigi_task


def _build_luigi_task_definition(luigi_task_cls):
    # type: (Type[luigi.Task]) -> Type[_LuigiTask]
    """
    build a classdict as needed in the creation of TaskMetaclass
    """
    task_family = luigi_task_cls.get_task_family()
    task_passport = TaskPassport.build_task_passport(
        task_family=task_family,
        task_namespace=luigi_task_cls.get_task_namespace(),
        cls_name=luigi_task_cls.__name__,
        module_name=luigi_task_cls.__module__,
    )
    task_source = TaskSourceCode.from_callable(luigi_task_cls)

    params_defs = extract_luigi_params(luigi_task_cls)
    td = TaskDefinition(
        task_passport=task_passport, source_code=task_source, classdict=params_defs
    )

    # we can't support outputs/inputs - they are dynamic per task
    # params_defs.update(_extract_targets_dedup(luigi_task_cls, attributes))

    return td
