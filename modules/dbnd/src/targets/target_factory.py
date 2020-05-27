import os
import re

from dbnd._core.errors import DatabandError
from targets.file_target import FileTarget
from targets.fs.local import LocalFileSystem
from targets.multi_target import MultiTarget
from targets.target_config import extract_target_config_from_path
from targets.utils.path import trailing_slash


_GLOB_PATH_REGEX = re.compile(r".*{.*,.*}.*")


def _is_glob_path(path):
    match = _GLOB_PATH_REGEX.match(path)
    return match is not None


def target(*path, **kwargs):
    """
    autoresolving function
    :param path:
    :param kwargs:
    :return: FileTarget
    """

    path = [str(p) for p in path]
    path = os.path.join(*path)
    if not path:
        raise DatabandError("Can not convert empty string '%s' to Target" % path)
    if not _is_glob_path(path):
        if "," in path:
            return MultiTarget(targets=[target(p, **kwargs) for p in path.split(",")])

    fs = kwargs.pop("fs", None)
    config = kwargs.pop("config", None)
    config = extract_target_config_from_path(path, config=config)
    if path.endswith("[noflag]"):
        config = config.with_flag(None)
        path = path[:-8]

    if config.folder and not trailing_slash(path):
        path = "%s%s" % (path, os.path.sep if isinstance(fs, LocalFileSystem) else "/")

    if config.target_factory:
        return config.target_factory(path, fs=fs, config=config)

    if config.folder:
        from targets.dir_target import DirTarget

        return DirTarget(path, fs=fs, config=config)
    return FileTarget(path=path, fs=fs, config=config, **kwargs)
