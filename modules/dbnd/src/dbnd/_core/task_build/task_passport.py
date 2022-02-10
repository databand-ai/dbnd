import typing

from typing import Type

import attr

from dbnd._core.task_build.task_const import _SAME_AS_PYTHON_MODULE
from dbnd._core.task_build.task_registry import get_task_registry
from dbnd._core.utils.basics.nothing import NOTHING, is_defined


if typing.TYPE_CHECKING:
    from dbnd._core.task import Task


def _short_name(name):
    # type: (str)->str
    """
    from my_package.sub_package  -> m.s
    """
    return ".".join(n[0] if n else n for n in name.split("."))


def format_source_suffix(name):
    return "[%s]" % name


@attr.s
class TaskPassport(object):
    """
    Task passport represents "name" identifiers for the Task/func/callable
    wrapped for Orchestration/Tracking by dbnd
    """

    full_task_family = attr.ib()
    full_task_family_short = attr.ib()
    task_family = attr.ib()
    task_config_section = attr.ib()

    @classmethod
    def from_task_cls(cls, task_class):
        # type: (Type[Task]) -> TaskPassport
        if task_class.task_decorator:
            return task_class.task_decorator.task_passport

        cls_name = task_class.__name__
        task_namespace = task_class.task_namespace
        module_name = task_class.__module__

        return cls.build_task_passport(
            cls_name=cls_name,
            module_name=module_name,
            task_namespace=task_namespace,
            task_family=task_class._conf__task_family,
        )

    @classmethod
    def from_module(cls, module):
        full_task_family_short = "%s" % (_short_name(module))

        return TaskPassport(
            full_task_family=module,
            full_task_family_short=full_task_family_short,
            task_family=module,
            task_config_section=None,
        )

    @classmethod
    def build_task_passport(
        cls, cls_name, module_name, task_namespace=NOTHING, task_family=None
    ):
        full_task_family = "%s.%s" % (module_name, cls_name)
        full_task_family_short = "%s.%s" % (_short_name(module_name), cls_name)

        if not is_defined(task_namespace):
            namespace_at_class_time = get_task_registry().get_namespace(module_name)
            if namespace_at_class_time == _SAME_AS_PYTHON_MODULE:
                task_namespace = module_name
            else:
                task_namespace = namespace_at_class_time

        if task_family:
            task_config_section = task_family
        elif task_namespace:
            task_family = "{}.{}".format(task_namespace, cls_name)
            task_config_section = task_family
        else:
            task_family = cls_name
            task_config_section = full_task_family

        return TaskPassport(
            full_task_family=full_task_family,
            full_task_family_short=full_task_family_short,
            task_family=task_family,
            task_config_section=task_config_section,
        )

    def format_source_name(self, name):
        return "%s%s" % (self.full_task_family_short, format_source_suffix(name))
