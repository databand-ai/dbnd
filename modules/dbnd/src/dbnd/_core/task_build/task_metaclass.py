import abc
import logging
import typing

import six

from dbnd._core.configuration.dbnd_config import config
from dbnd._core.constants import TaskEssence
from dbnd._core.plugin.dbnd_airflow_operator_plugin import (
    build_task_at_airflow_dag_context,
    is_in_airflow_dag_build_context,
)
from dbnd._core.task_build.task_context import try_get_current_task
from dbnd._core.task_build.task_definition import TaskDefinition
from dbnd._core.task_build.task_factory import TaskFactory
from dbnd._core.task_build.task_registry import get_task_registry


if typing.TYPE_CHECKING:
    from dbnd._core.task.base_task import _BaseTask

logger = logging.getLogger(__name__)


class TaskMetaclass(abc.ABCMeta):
    """
    The Metaclass of :py:class:`Task`.
    """

    def __new__(mcs, classname, bases, classdict):
        """
        Custom class creation for namespacing.

        Also register all subclasses.

        When the set or inherited namespace evaluates to ``None``, set the task namespace to
        whatever the currently declared namespace is.
        """
        cls = super(TaskMetaclass, mcs).__new__(
            mcs, classname, bases, classdict
        )  # type: typing.Type[_BaseTask]

        # we are starting from "not clean" classdict ->
        # A. it's deserialization
        # B. it was calculated before
        if classdict.get("task_definition") is not None:
            return cls

        cls.task_definition = TaskDefinition.from_task_cls(
            task_class=cls, classdict=classdict
        )

        # now we will assign all calculated parameters
        # so instead of ParameterFactory, we will have ParameterDefinition
        for k, v in six.iteritems(cls.task_definition.task_param_defs):
            setattr(cls, k, v)

        # every time we see new implementation, we want it to have an priority over old implementation
        # we need to switch to dict() and store history else where
        r = get_task_registry()
        r.register_task(cls)

        return cls

    def _build_task_obj(cls, **kwargs):
        # called from TaskFactory to create object
        return super(TaskMetaclass, cls).__call__(**kwargs)

    def __call__(cls, *args, **kwargs):
        """
        Custom class instantiation utilizing instance cache.
        """
        _dbnd_disable_airflow_inplace = kwargs.pop(
            "_dbnd_disable_airflow_inplace", False
        )
        if (
            is_in_airflow_dag_build_context()
            and TaskEssence.is_task_cls(cls)
            and not _dbnd_disable_airflow_inplace
            and not getattr(cls, "_dbnd_decorated_task", False)
        ):
            kwargs = kwargs.copy()
            kwargs["_dbnd_disable_airflow_inplace"] = True
            return build_task_at_airflow_dag_context(
                task_cls=cls, call_args=args, call_kwargs=kwargs
            )

        task_definition = cls.task_definition
        # we need to have context initialized before we start to run all logic in config() scope

        # create new config layer, so when we are out of this process -> config is back to the previous value
        with config(
            config_values={},
            source=task_definition.task_passport.format_source_name("runtime"),
        ) as task_config:
            factory = TaskFactory(
                config=task_config,
                task_cls=cls,
                task_definition=cls.task_definition,
                task_args=args,
                task_kwargs=kwargs,
            )
            task_object = factory.build_task_object(cls)

        parent_task = try_get_current_task()
        if (
            parent_task
            and hasattr(task_object, "task_id")
            and (task_object.task_essence != TaskEssence.CONFIG)
        ):
            parent_task.descendants.add_child(task_object.task_id)

        return task_object
