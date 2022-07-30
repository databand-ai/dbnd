# © Copyright Databand.ai, an IBM Company 2022

import importlib
import logging
import os
import re
import sys

from dbnd._core.errors import DatabandError, friendly_error
from dbnd._core.utils.basics.memoized import cached


logger = logging.getLogger(__name__)

try:
    import_errors = (ImportError, ModuleNotFoundError)
except Exception:
    # we are python2
    import_errors = (ImportError,)


@cached()
def _load_module(module, description):
    try:
        try:
            return importlib.import_module(module)
        except import_errors:

            # in some cases it will not help
            # like "tests" package.
            # it too late to fix it as tests already loaded from site-packages..
            if os.getcwd() in sys.path:
                raise

            # we'll try to load current folder to PYTHONPATH, just in case
            logger.info(
                "Databand has failed to load module '%s', "
                "it will retry with cwd at PYTHONPATH." % module
            )
            sys.path.insert(0, os.getcwd())
            m = importlib.import_module(module)
            logger.info(
                "We have managed to load module after adding %s to PYTHONPATH, "
                "please consider using 'pip install -e . ' with your project"
                % os.getcwd()
            )
            return m
    except import_errors as ex:
        logger.warning(
            "Failed to load module '%s' %s: cwd='%s', sys.path=\n\t%s",
            module,
            friendly_error.dbnd_module_not_found_tip(module),
            os.getcwd(),
            "\n\t".join(sys.path),
        )
        raise friendly_error.failed_to_import_user_module(
            ex, module=module, description=description
        )


def load_python_module(module, module_source):
    logger.info("Loading modules '%s' from %s.", module, module_source)
    for m in module.split(","):
        _load_module(m, module_source)


def load_python_attr_from_module(attr_path):
    m = re.match(r"^(\S+)\.(\S+)", attr_path)
    if not m:
        raise friendly_error.config.wrong_func_attr_format(attr_path)

    module_path, attr_name = m.group(1), m.group(2)
    module = _load_module(module_path, description="")

    if not hasattr(module, attr_name):
        raise DatabandError("Failed to import symbol %s" % attr_path)

    attr = getattr(module, attr_name)
    return attr


def load_python_callable(callable_path):
    callable_attr = load_python_attr_from_module(callable_path)
    if not callable(callable_attr):
        raise DatabandError("The `%s` is not `callable`" % callable_attr)
    return callable_attr


def run_user_func(callable_path):
    if not callable_path:
        return None
    f = load_python_callable(callable_path=callable_path)
    try:
        return f()
    except Exception:
        logger.warning("Failed to run user function %s", callable_path)
        raise
