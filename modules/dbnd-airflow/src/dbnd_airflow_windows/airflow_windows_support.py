import getpass
import os
import sys

from airflow.utils import configuration, timeout

from dbnd_airflow_windows import timeout as timeout_patch
from dbnd_airflow_windows.configuration import tmp_configuration_copy
from dbnd_airflow_windows.getuser import find_runnable_getuser_function


__win_patches = (
    (timeout, "timeout", timeout_patch.timeout),
    (configuration, "tmp_configuration_copy", tmp_configuration_copy),
    (getpass, "getuser", find_runnable_getuser_function()),
    (sys, "maxint", sys.maxsize),
)


def patch_module_attr(module, name, value):
    original_name = "original_" + name
    if hasattr(module, original_name):
        return
    setattr(module, original_name, getattr(module, name, None))
    setattr(module, name, value)


def _patch_windows_compatible():
    for module, name, value in __win_patches:
        patch_module_attr(module, name, value)


def enable_airflow_windows_support():
    if os.name == "nt":
        _patch_windows_compatible()
