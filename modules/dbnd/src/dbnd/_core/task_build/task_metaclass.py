import abc
import logging
import typing

import six

from dbnd._core.configuration.config_store import ConfigMergeSettings
from dbnd._core.configuration.dbnd_config import config
from dbnd._core.constants import _ConfigParamContainer, _TaskParamContainer
from dbnd._core.current import get_databand_context
from dbnd._core.plugin.dbnd_airflow_operator_plugin import (
    build_task_at_airflow_dag_context,
    is_in_airflow_dag_build_context,
)
from dbnd._core.task_build.task_context import (
    TaskContextPhase,
    task_context,
    try_get_current_task,
)
from dbnd._core.task_build.task_definition import TaskDefinition
from dbnd._core.task_build.task_meta_factory import (
    TaskMetaFactory,
    TrackedTaskMetaFactory,
)
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

        # we are starting from "not clean" classdict -> it's deserialization
        if classdict.get("task_definition") is not None:
            return cls

        td = cls.task_definition = TaskDefinition(cls, classdict)

        # now we will assign all params
        set_params = td.class_params if cls.is_tracking_mode else td.all_task_params
        for k, v in six.iteritems(set_params):
            setattr(cls, k, v)

        # every time we see new implementation, we want it to have an priority over old implementation
        # we need to switch to dict() and store history else where
        r = get_task_registry()
        r.register_task(cls)

        return cls

    def _build_task_obj(cls, task_meta):
        return super(TaskMetaclass, cls).__call__(task_meta=task_meta)

    def __call__(cls, *args, **kwargs):
        """
        Custom class instantiation utilizing instance cache.
        """
        _dbnd_disable_airflow_inplace = kwargs.pop(
            "_dbnd_disable_airflow_inplace", False
        )
        if (
            is_in_airflow_dag_build_context()
            and not _ConfigParamContainer.is_type_config(cls)
            and not _dbnd_disable_airflow_inplace
            and not getattr(cls, "_dbnd_decorated_task", False)
        ):
            kwargs = kwargs.copy()
            kwargs["_dbnd_disable_airflow_inplace"] = True
            return build_task_at_airflow_dag_context(
                task_cls=cls, call_args=args, call_kwargs=kwargs
            )

        return cls._create_task(args, kwargs)

    def _create_task(cls, args, kwargs):
        task_definition = cls.task_definition
        # we need to have context initialized before we start to run all logic in config() scope
        with config(
            config_values=task_definition.task_defaults_config_store,
            source=task_definition.task_passport.format_source_name("defaults"),
            merge_settings=ConfigMergeSettings.on_non_exists_only,
        ) as task_config:
            # update config with current class defaults
            # we apply them to config only if there are no values (this is defaults)
            return create_dbnd_task(
                config=task_config,
                new_task_factory=cls._build_task_obj,
                task_cls=cls,
                task_args=args,
                task_kwargs=kwargs,
            )

    @classmethod
    def disable_instance_cache(cls):
        """
        Disables the instance cache.
        """
        cls.__instance_cache = None

    @classmethod
    def clear_instance_cache(cls):
        """
        Clear/Reset the instance cache.
        """
        cls.__instance_cache = {}


def create_dbnd_task(config, new_task_factory, task_cls, task_args, task_kwargs):
    # type:(DbndConfig, Any, Type[_BaseTask], Any, Any, bool)->None
    tracking_mode = task_cls.is_tracking_mode

    task_meta_factory = TrackedTaskMetaFactory if tracking_mode else TaskMetaFactory
    factory = task_meta_factory(
        config=config, task_cls=task_cls, task_args=task_args, task_kwargs=task_kwargs,
    )

    task_meta = factory.create_dbnd_task_meta()

    # If a Task has already been instantiated with the same parameters,
    # the previous instance is returned to reduce number of object instances.
    tic = get_databand_context().task_instance_cache
    task = tic.get_task_obj_by_id(task_meta.obj_key.id)
    if not task or tracking_mode or hasattr(task, "_dbnd_no_cache"):
        task = new_task_factory(task_meta)
        tic.register_task_obj_instance(task)

        # now the task is created - all nested constructors will see it as parent
        with task_context(task, TaskContextPhase.BUILD):
            task._initialize()
            task._validate()
            task.task_meta.config_layer = config.config_layer

        tic.register_task_instance(task)

    parent_task = try_get_current_task()
    if (
        parent_task
        and hasattr(task, "task_id")
        and isinstance(task, _TaskParamContainer)
    ):
        parent_task.task_meta.add_child(task.task_id)

    return task
