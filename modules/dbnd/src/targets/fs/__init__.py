import typing
from collections import Callable

from targets.fs.local import LocalFileSystem

if typing.TYPE_CHECKING:
    from targets import FileSystem


class FileSystems(object):
    local = "local"
    s3 = "s3"
    gcs = "gcs"


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

_cached_fs = {}


def get_file_system(fs_name):
    fs = _cached_fs.get(fs_name)
    if fs:
        return fs

    fs_builder = KNOWN_FILE_SYSTEMS[fs_name]
    _cached_fs[fs_name] = fs = fs_builder()
    return fs


def get_file_system_name(path):
    if ":" in path:
        fs_prefix = path.split(":")[0]
        if fs_prefix in KNOWN_FILE_SYSTEMS:
            return fs_prefix

    for cfs in CUSTOM_FILE_SYSTEM_MATCHERS:
        found_fs_name = cfs(path)
        if found_fs_name:
            return found_fs_name

    if path.startswith("/"):
        return FileSystems.local
    return FileSystems.local
