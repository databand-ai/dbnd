import copy
import logging
import sys
import typing

from typing import Type

import six

from dbnd._core.configuration.dbnd_config import config
from dbnd._core.current import get_databand_context
from dbnd._core.errors import friendly_error
from dbnd._core.plugin.dbnd_plugins import is_airflow_enabled
from dbnd._core.utils.basics.singleton_context import SingletonContext
from dbnd._core.utils.seven import contextlib
from dbnd._vendor.snippets.luigi_registry import get_best_candidate, module_parents


logger = logging.getLogger(__name__)

if typing.TYPE_CHECKING:
    from dbnd._core.task.task import Task
    from dbnd._core.task_build.task_definition import TaskDefinition


def _validate_no_recursion_in_config(task_name, config_task_type, param):
    if task_name == config_task_type:
        raise friendly_error.config.task_name_and_from_are_the_same(task_name, param)


class DbndTaskRegistry(SingletonContext):
    """
    Registry of all `dbnd` tasks
    """

    AMBIGUOUS_CLASS = object()

    def __init__(self):
        self._fqn_to_task_cls = {}  # map between full task class name and class
        self._task_family_to_task_cls = {}  # map between task_family name and class

        # namespace management

        self._namespace_map = {}

        # airflow support
        self._dag_bag = None

    def register_task(self, task_cls):
        td = task_cls.task_definition  # type: TaskDefinition

        self._fqn_to_task_cls[td.full_task_family] = task_cls
        if td.task_family in self._task_family_to_task_cls:
            task_cls = self.AMBIGUOUS_CLASS
        self._task_family_to_task_cls[td.task_family] = task_cls

    def _get_registered_task_cls(self, name):  # type: (str) -> Type[Task]
        """
        Returns an task class based on task_family, or full task class name
        We don't preload/check anything here
        """
        task_cls = self._fqn_to_task_cls.get(name)
        if task_cls:
            return task_cls

        return self._task_family_to_task_cls.get(name)

    # used for both tasks and configurations
    def _get_task_cls(self, task_name):
        from dbnd._core.utils.basics.load_python_module import load_python_module

        task_cls = self._get_registered_task_cls(task_name)
        if task_cls:
            return task_cls

        # we are going to check if we have override/definition in config
        config_task_type = config.get(task_name, "_type", None)
        if config_task_type:
            _validate_no_recursion_in_config(task_name, config_task_type, "_type")
            try:
                return self._get_task_cls(config_task_type)
            except Exception:
                logger.error(
                    "Failed to load type required by [%s] using _type=%s",
                    task_name,
                    config_task_type,
                )
                raise
        config_task_type = config.get(task_name, "_from", None)
        if config_task_type:
            _validate_no_recursion_in_config(task_name, config_task_type, "_from")
            return self._get_task_cls(config_task_type)

        if "." in task_name:
            parts = task_name.split(".")
            possible_root_task = parts.pop()
            possible_module = ".".join(parts)

            # Try to load module and check again for existance
            load_python_module(possible_module, "task name '%s'" % task_name)

            task_cls = self._get_registered_task_cls(task_name)
            if task_cls:
                return task_cls

            # Check if task exists but user forgot to decorate method with @task
            task_module = sys.modules.get(possible_module)
            if task_module and hasattr(task_module, possible_root_task):
                user_func = getattr(task_module, possible_root_task)
                if callable(user_func):
                    # Non-decorated function was found - decorate and return it
                    from dbnd._core.decorator import dbnd_func_proxy

                    decorated_task = dbnd_func_proxy.task(user_func)
                    setattr(task_module, possible_root_task, decorated_task)
                    logger.warning(
                        "Found non-decorated task: %s. "
                        "Please decorate this task with the proper symbol @pipeline \ @task.\n"
                        "Auto-decorating and treating it as @task ...",
                        task_name,
                    )
                    return decorated_task.task

        if is_airflow_enabled():
            from dbnd_airflow.dbnd_task_executor.airflow_operator_as_dbnd import (
                AirflowDagAsDbndTask,
            )

            dag = self._get_aiflow_dag(task_name)
            if dag:
                return AirflowDagAsDbndTask
        return None

    def get_task_cls(self, task_name):
        from dbnd._core.errors import friendly_error

        task_cls = self._get_task_cls(task_name)
        if task_cls == self.AMBIGUOUS_CLASS:
            raise friendly_error.ambiguous_task(task_name)
        if task_cls:
            return task_cls
        raise friendly_error.task_registry.task_not_exist(
            task_name=task_name,
            alternative_tasks=get_best_candidate(
                task_name, self._task_family_to_task_cls.keys()
            ),
        )

    def list_dbnd_task_classes(self):
        task_classes = []
        for task_name, task_cls in six.iteritems(self._task_family_to_task_cls):
            if task_cls == self.AMBIGUOUS_CLASS:
                continue
            td = task_cls.task_definition
            if td.hidden:
                continue
            if td.task_family.startswith("_"):
                continue

            task_classes.append(task_cls)

        task_classes = sorted(
            task_classes, key=lambda task_cls: task_cls.task_definition.full_task_family
        )
        return task_classes

    def build_dbnd_task(self, task_name, task_kwargs=None, expected_type=None):
        task_kwargs = task_kwargs or dict()
        task_kwargs.setdefault("task_name", task_name)

        task_cls = self.get_task_cls(task_name)  # type: Type[Task]
        if is_airflow_enabled():
            from dbnd_airflow.dbnd_task_executor.airflow_operator_as_dbnd import (
                AirflowDagAsDbndTask,
            )

            if issubclass(task_cls, AirflowDagAsDbndTask):
                # we are running old style dag
                dag = self._get_aiflow_dag(task_name)
                airflow_task = AirflowDagAsDbndTask.build_dbnd_task_from_dag(dag=dag)
                return airflow_task

        try:
            logger.debug("Building %s task", task_cls.task_definition.full_task_family)
            obj = task_cls(**task_kwargs)

        except Exception:
            exc = get_databand_context().settings.log.format_exception_as_str(
                sys.exc_info(), isolate=True
            )
            logger.error("Failed to build %s: \n\n%s", task_cls.get_task_family(), exc)
            raise
        if expected_type and not issubclass(task_cls, expected_type):
            raise friendly_error.task_registry.wrong_type_for_task(
                task_name, task_cls, expected_type
            )
        return obj

    def _get_aiflow_dag(self, dag_id):
        if not self._dag_bag:
            from airflow.models import DagBag

            self._dag_bag = DagBag()
        if dag_id in self._dag_bag.dags:
            return self._dag_bag.dags[dag_id]
        return None

    ########
    ## NAMESPACE MANAGEMENT
    def get_namespace(self, module_name):
        for parent in module_parents(module_name):
            #  if module is a.b.c ->  a.b can have namespace defined
            entry = self._namespace_map.get(parent)
            if entry:
                return entry
        # by default we don't have namespace
        return ""

    def register_namespace(self, scope, namespace):
        self._namespace_map[scope] = namespace


_REGISTRY = DbndTaskRegistry.try_instance()


def build_task_from_config(task_name, expected_type=None):
    tr = get_task_registry()
    return tr.build_dbnd_task(task_name=task_name, expected_type=expected_type)


def register_config_cls(config_cls):
    logger.debug("Registered config %s", config_cls)


def register_task(task):
    logger.debug("Registered task %s", task)


def get_task_registry():
    # type: ()-> DbndTaskRegistry
    return DbndTaskRegistry.get_instance()


@contextlib.contextmanager
def tmp_dbnd_registry():
    current = get_task_registry()

    new_tmp_registry = DbndTaskRegistry()
    # copy all old values
    new_tmp_registry._fqn_to_task_cls = copy.copy(current._fqn_to_task_cls)
    new_tmp_registry._task_family_to_task_cls = copy.copy(
        current._task_family_to_task_cls
    )
    new_tmp_registry._namespace_map = copy.copy(current._namespace_map)
    with DbndTaskRegistry.new_context(
        _context=new_tmp_registry, allow_override=True
    ) as r:
        # assign already known tasks
        yield r
