# © Copyright Databand.ai, an IBM Company 2022

import logging
import os
import threading
import typing

from dbnd._core.errors import DatabandRuntimeError
from dbnd._core.utils.seven import Callable
from targets.fs.local import LocalFileSystem


logger = logging.getLogger(__name__)

if typing.TYPE_CHECKING:
    from targets import FileSystem


class FileSystems(object):
    local = "local"
    s3 = "s3"
    gcs = "gs"


KNOWN_FILE_SYSTEMS = {}
CUSTOM_FILE_SYSTEM_MATCHERS = []


def register_file_system(prefix, file_system_factory):
    # type: (str,Callable[[], FileSystem]) -> None
    KNOWN_FILE_SYSTEMS[prefix] = file_system_factory


def register_file_system_name_custom_resolver(resolver):
    # type: (Callable[[str], str])->None
    CUSTOM_FILE_SYSTEM_MATCHERS.append(resolver)


# in memory is implemented as part of InMemory target
# register_file_system("memory", )
register_file_system(FileSystems.local, LocalFileSystem)
register_file_system("file", LocalFileSystem)

_cached_fs = {}


def reset_fs_cache():
    global _cached_fs
    _cached_fs = {}


def get_file_system(fs_name):
    current_thread_id = threading.current_thread().ident
    fs = _cached_fs.get((fs_name, current_thread_id))
    if fs:
        return fs

    if fs_name not in KNOWN_FILE_SYSTEMS:
        raise Exception("Unknown file system '%s' " % fs_name)

    fs_builder = KNOWN_FILE_SYSTEMS[fs_name]
    _cached_fs[(fs_name, current_thread_id)] = fs = fs_builder()
    return fs


def get_file_system_name(path):
    fs_prefix = None
    if ":" in path:
        fs_prefix = path.split(":")[0]
        if fs_prefix in KNOWN_FILE_SYSTEMS:
            return fs_prefix
        if os.name == "nt" and fs_prefix.lower() in get_windows_drives():
            return FileSystems.local

    for cfs in CUSTOM_FILE_SYSTEM_MATCHERS:
        found_fs_name = cfs(path)
        if found_fs_name:
            return found_fs_name

    if path.startswith("/"):
        return FileSystems.local

    if fs_prefix:
        # TODO: it would be nice to provide more useful information
        raise DatabandRuntimeError(
            "Can't find file system '%s'" % fs_prefix,
            help_msg="Please check that you have registered required schema with"
            " `register_file_system` or relevant plugin is installed",
        )
    return FileSystems.local


def get_windows_drives():
    import string

    from ctypes import windll

    drives = []
    bitmask = windll.kernel32.GetLogicalDrives()
    for letter in string.ascii_lowercase:
        if bitmask & 1:
            drives.append(letter)
        bitmask >>= 1

    return drives
