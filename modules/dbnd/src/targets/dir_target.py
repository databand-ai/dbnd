import logging
import os

from dbnd._core.utils.basics.nothing import NOTHING, is_not_defined
from targets.file_target import FileTarget
from targets.fs import FileSystems
from targets.target_factory import target
from targets.utils.open_multiple import MultiTargetOpen


logger = logging.getLogger(__name__)

DEFAULT_FLAG_FILE_NAME = "_SUCCESS"


class DirTarget(FileTarget):
    def __init__(self, path, fs, config=None, io_pipe=None, source=None):
        """
         If `flag` Defines a target directory with a flag-file (defaults to `_SUCCESS`) used
    to signify job success.

        This checks for two things:

        * the path exists (just like the S3Target)
        * the _SUCCESS file exists within the directory.

        Because Hadoop outputs into a directory and not a single file,
        the path is assumed to be a directory.

        :param path:
        :param fs:
        :param config:
        :param io_pipe:
        :param flag:
        :param source:
        """

        super(DirTarget, self).__init__(
            path=path, fs=fs, config=config, io_pipe=io_pipe, source=source
        )
        if path[-1] != "/" and path[-1] != "\\":
            raise ValueError(
                "%s requires the path to be to a "
                "directory.  It must end with a slash ( / )." % self.__class__.__name__
            )
        flag = config.flag
        if flag is True:
            flag = DEFAULT_FLAG_FILE_NAME  # default value, otherwise override it

        self.flag_target = target(path, flag, fs=fs) if flag else None

        self._auto_partition_count = 0
        self._write_target = self.partition("part-0000")

        self.meta_files = config.meta_files

    def exists(self):
        path = self.path
        if self.flag_target:
            return self.flag_target.exists()

        return super(DirTarget, self).exists()

    def folder_exists(self):
        return self.fs.exists(self.path)

    def is_meta_file(self, path):
        base_name = os.path.basename(path)
        if base_name.startswith(".") or base_name.startswith("_"):
            return True
        if base_name in self.meta_files:
            return True
        return False

    def list_partitions(self):
        all_files = self.fs.listdir(self.path)
        all_files = sorted(all_files)
        all_files = filter(lambda x: not self.is_meta_file(x), all_files)
        if self.flag_target:
            all_files = filter(lambda x: x != str(self.flag_target), all_files)
        return [
            target(
                p,
                fs=self._fs,
                config=self.config.as_file(),
                properties=self.properties,
                source=self.source,
            )
            for p in all_files
        ]

    def open(self, mode="r"):
        if "r" in mode:
            return MultiTargetOpen(targets=self.list_partitions(), mode=mode)
        elif "w" in mode:
            return self._write_target.open(mode)
        else:
            raise ValueError("Unsupported open mode '{}'".format(mode))

    def partition(self, name=NOTHING, extension=NOTHING, config=NOTHING, **kwargs):
        """
        :param config:
        :param name: file name of the partition. if not provided - "part-%04d" % ID
        :param extension: extension. if not provided -> default extension will be used
        :return: FileTarget that represents the partition.
        """
        if is_not_defined(name):
            name = "part-%04d" % self._auto_partition_count
            self._auto_partition_count += 1
        if is_not_defined(config):
            # only if it's a call not from file,folder - we set it as file
            config = self.config.as_file()

        if is_not_defined(extension):
            extension = config.get_ext()
        if extension:
            name += extension
        return target(self.path, name, config=config, fs=self._fs, **kwargs)

    def mkdir(self):
        self.fs.mkdir(self.path)

    def mark_success(self):
        self.mkdir()
        if self.flag_target:
            self.flag_target.touch()

    def is_local(self):
        return self.fs_name == FileSystems.local

    def move_from(self, from_path, raise_if_exists=False):
        self.fs.move_dir(from_path, self.path, raise_if_exists)
        self.mark_success()

    def make_tmp(self):
        return self.fs.make_tmp_dir()
