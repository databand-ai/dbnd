"""
All code related to "getting" file from project based on databand library
"""

import os

from dbnd._core.configuration.environ_config import ENV_DBND_HOME, ENV_DBND_SYSTEM


def abs_join(*path):
    return os.path.abspath(os.path.normpath(os.path.join(*path)))


def relative_path(file_in_path, *path):
    _current_dir = os.path.dirname(os.path.abspath(file_in_path))
    return abs_join(_current_dir, *path)


_lib_home_default = relative_path(__file__, "..", "..", "..")

_home_default = abs_join(_lib_home_default, "..")
_system_default = abs_join(_lib_home_default, ".dbnd")

PROJECT_HOME = None
DBND_SYSTEM = None


def set_project_fs(root=None, dbnd_system=None):
    global PROJECT_HOME, DBND_SYSTEM
    PROJECT_HOME = root or os.environ.get(ENV_DBND_HOME, _home_default)
    if dbnd_system:
        os.environ[ENV_DBND_SYSTEM] = dbnd_system


def get_project_home():
    return PROJECT_HOME


def databand_lib_path(*path):
    return abs_join(_lib_home_default, *path)


def databand_config_path(*path):
    return databand_lib_path("conf", *path)


def databand_system_path(*path):
    dbnd_system = os.environ.get(ENV_DBND_SYSTEM, _system_default)
    return abs_join(dbnd_system, *path)


def project_path(*path):
    return abs_join(PROJECT_HOME, *path)


# we have eager call on project root
set_project_fs()
