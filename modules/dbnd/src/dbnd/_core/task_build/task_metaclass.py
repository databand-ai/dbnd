import abc
import logging

import six

from dbnd._core.task_build.task_definition import TaskDefinition
from dbnd._core.task_build.task_factory import TaskFactory
from dbnd._core.task_build.task_registry import get_task_registry


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
        cls = super(TaskMetaclass, mcs).__new__(mcs, classname, bases, classdict)

        # we are starting from "not clean" classdict -> it's deserialization
        if classdict.get("task_definition") is not None:
            return cls

        r = get_task_registry()
        cls.task_definition = TaskDefinition(
            cls, classdict, namespace_at_class_time=r.get_namespace(cls.__module__)
        )

        # now we will assign all params
        for k, v in six.iteritems(cls.task_definition.task_params):
            setattr(cls, k, v)

        # every time we see new implementation, we want it to have an priority over old implementation
        # we need to switch to dict() and store history else where
        r.register_task(cls)

        return cls

    def _build_task_obj(cls, task_meta):
        return super(TaskMetaclass, cls).__call__(task_meta=task_meta)

    def __call__(cls, *args, **kwargs):
        """
        Custom class instantiation utilizing instance cache.
        """
        tmb = TaskFactory(
            new_task_factory=cls._build_task_obj,
            task_cls=cls,
            task_args=args,
            task_kwargs=kwargs,
        )
        return tmb.create_dbnd_task()

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
