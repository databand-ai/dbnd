import getpass
import os
import sys

import airflow

from airflow.utils import configuration

from dbnd_airflow_windows import timeout
from dbnd_airflow_windows.configuration import tmp_configuration_copy
from dbnd_airflow_windows.getuser import find_runnable_getuser_function


__win_patches = (
    (airflow.utils.timeout, "timeout", timeout.timeout),
    (airflow.models, "timeout", timeout.timeout),
    (configuration, "tmp_configuration_copy", tmp_configuration_copy),
    (getpass, "getuser", find_runnable_getuser_function()),
    (sys, "maxint", sys.maxsize),
)


def patch_module_attr(module, name, value):
    original_name = "original_" + name
    if getattr(module, original_name):
        return
    setattr(module, original_name, getattr(module, name))
    setattr(module, name, value)


def _patch_windows_compatible():
    for module, name, value in __win_patches:
        patch_module_attr(module, name, value)


def enable_airflow_windows_support():
    if os.name == "nt":
        _patch_windows_compatible()
