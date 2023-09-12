# © Copyright Databand.ai, an IBM Company 2022

import logging
import re
import sys
import typing

from types import FunctionType, ModuleType

from dbnd import task
from dbnd._core.tracking.no_tracking import should_not_track


logger = logging.getLogger(__name__)


def _is_function(obj):
    return isinstance(obj, FunctionType)


def _is_task(obj):
    """
    checks if obj is decorated func (dbnd generated object)
    """
    return hasattr(obj, "__is_dbnd_task__")


def _track_function(function):
    if not _is_function(function) or should_not_track(function) or _is_task(function):
        return

    decorated_function = task(function)

    # We modify all modules since each module has its own pointers to local and imported functions.
    # If a module has already imported the function we need to change the pointer in that module.
    for module in sys.modules.copy().values():
        if not _is_module(module):
            continue

        for k, v in module.__dict__.items():
            if v is function:
                module.__dict__[k] = decorated_function


def track_functions(*args):
    """Track functions by decorating them with @task."""
    for arg in args:
        try:
            _track_function(arg)
        except Exception:
            logger.exception("Failed to track %s" % arg)


def _is_module(obj):
    return isinstance(obj, ModuleType)


def track_module_functions(module):
    """
    Track functions inside module by decorating them with @task.

    Only functions implemented in module will be tracked, imported functions won't be tracked.
    """
    try:
        if not _is_module(module):
            return

        module_objects = module.__dict__.values()
        module_functions = [i for i in module_objects if _is_module_function(i, module)]
        track_functions(*module_functions)
    except Exception:
        logger.exception("Failed to track %s" % module)


def track_modules(*args):
    """
    Track functions inside modules by decorating them with @task.

    Only functions implemented in module will be tracked, imported functions won't be tracked.
    """
    for arg in args:
        try:
            track_module_functions(arg)
        except Exception:
            logger.exception("Failed to track %s" % arg)


def _is_module_function(function, module):
    try:
        if not _is_function(function):
            return False

        if not hasattr(function, "__globals__"):
            return False

        return function.__globals__ is module.__dict__
    except Exception:
        logger.exception("Failed to track %s" % function)
        return False


def _match_func_name(
    value: object,
    module: typing.Optional[str],
    prefixes: typing.List[typing.Union[str, re.Pattern]],
    exclude: typing.List[typing.Union[str, re.Pattern]],
):
    """
    match func to the function scope (module) and the name prefix (regexp or prefix)
    """
    # it should be callable
    if not _is_function(value) or should_not_track(value) or _is_task(value):
        return False
    # let's check scope (__main__ for example)
    if module is not None and not value.__module__.startswith(module):
        return False

    func_name = value.__name__

    if exclude:
        for exclude_filter in exclude:
            if isinstance(exclude_filter, str):
                if func_name.startswith(exclude_filter):
                    return False
            elif isinstance(exclude_filter, re.Pattern):
                if exclude_filter.match(func_name):
                    return False

    # if no prefixes filter  - it's a match
    if not prefixes:
        return True
    for prefix in prefixes:
        if isinstance(prefix, str):
            if func_name.startswith(prefix):
                return True
        elif isinstance(prefix, re.Pattern):
            if prefix.match(func_name):
                return True
    return False


def track_scope_functions(
    scope: typing.Dict[str, object] = None,
    prefixes: typing.List[typing.Union[typing.Pattern, str]] = None,
    exclude: typing.List[typing.Union[typing.Pattern, str]] = None,
    module="__main__",
):
    """
    Track all functions that match prefixes list within specific scope.

    This function especially useful for Jupyter Notebooks.
    You can start to track all functions from the current namespace, even after the function import
    """
    try:
        if scope is None:
            scope = globals()

        matched_functions = {}
        for key, value in list(scope.items()):
            if _match_func_name(
                value=value, module=module, prefixes=prefixes, exclude=exclude
            ):
                matched_functions[key] = value

        for name, func in matched_functions.items():
            scope[name] = task(func)
        return matched_functions
    except Exception as ex:

        logger.error("Failed to mark function as @dbnd.task: %s" % ex)
