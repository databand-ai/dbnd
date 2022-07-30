# © Copyright Databand.ai, an IBM Company 2022

"""
All code related to "getting" file from project based on databand library
Deprecated in favor for dbnd_project_config at dbnd._core.configuration.environ_config
"""

from dbnd._core.configuration.environ_config import get_dbnd_project_config
from dbnd._core.utils.basics.path_utils import (
    abs_join,
    relative_path,
    relative_path_directory,
)


def databand_lib_path(*path):
    """
    Deprecated function.

    Look at dbnd_project_config at dbnd._core.configuration.environ_config.
    """
    return get_dbnd_project_config().dbnd_lib_path(*path)


def databand_config_path(*path):
    return get_dbnd_project_config().dbnd_config_path(*path)


def databand_system_path(*path):
    """
    Deprecated function.

    Look at dbnd_project_config at dbnd._core.configuration.environ_config.
    """
    return get_dbnd_project_config().dbnd_system_path(*path)


def project_path(*path):
    """
    Gets a local path from databand directory and returns its absolute path.

    Deprecated. Look at dbnd_project_config at dbnd._core.configuration.environ_config
    """
    return get_dbnd_project_config().dbnd_project_path(*path)


__all__ = [
    "abs_join",
    "relative_path_directory",
    "relative_path",
    "databand_lib_path",
    "databand_config_path",
    "databand_system_path",
    "project_path",
]
