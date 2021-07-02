import errno
import io
import logging
import os
import random
import shutil
import zipfile

from targets.errors import FileAlreadyExists, MissingParentDirectory, NotADirectory
from targets.fs.file_system import FileSystem
from targets.pipes.base import FileWrapper
from targets.utils.atomic import AtomicLocalFile


logger = logging.getLogger(__name__)


class LocalFileSystem(FileSystem):
    """
    Wrapper for access to file system operations.

    Work in progress - add things as needed.
    """

    name = "local"
    support_direct_access = True
    local = True

    def copy(self, old_path, new_path, raise_if_exists=False):
        if raise_if_exists and os.path.exists(new_path):
            raise RuntimeError("Destination exists: %s" % new_path)
        if os.path.isfile(old_path):
            d = os.path.dirname(new_path)
            if d and not os.path.exists(d):
                self.mkdir(d)
            shutil.copy(old_path, new_path)
        else:
            shutil.copytree(old_path, new_path)

    def exists(self, path):
        return os.path.exists(path)

    def mkdir(self, path, parents=True, raise_if_exists=False):
        if self.exists(path):
            if raise_if_exists:
                raise FileAlreadyExists()
            elif not self.isdir(path):
                raise NotADirectory()
            else:
                return

        if parents:
            try:
                os.makedirs(path)
            except OSError as err:
                # somebody already created the path
                if err.errno != errno.EEXIST:
                    raise
        else:
            if not os.path.exists(os.path.dirname(path)):
                raise MissingParentDirectory()
            os.mkdir(path)

    def mkdir_parent(self, path):
        """
        Create all parent folders if they do not exist.
        """
        parentfolder = os.path.dirname(os.path.normpath(path))
        if parentfolder:
            return self.mkdir(parentfolder)

    def isdir(self, path):
        return os.path.isdir(path)

    def listdir(self, path):
        for dir_, _, files in os.walk(path):
            assert dir_.startswith(path)
            for name in files:
                yield os.path.join(dir_, name)

    def remove(self, path, recursive=True):
        if recursive and self.isdir(path):
            shutil.rmtree(path)
        else:
            os.remove(path)

    def move(self, old_path, new_path, raise_if_exists=False):
        """
        Move file atomically. If source and destination are located
        on different filesystems, atomicity is approximated
        but cannot be guaranteed.
        """
        if raise_if_exists and os.path.exists(new_path):
            raise FileAlreadyExists("Destination exists: %s" % new_path)
        d = os.path.dirname(new_path)
        if d and not os.path.exists(d):
            self.mkdir(d)
        try:
            os.rename(old_path, new_path)
        except OSError as err:
            if err.errno == errno.EXDEV:
                new_path_tmp = "%s-%09d" % (new_path, random.randint(0, 999999999))
                shutil.copy(old_path, new_path_tmp)
                os.rename(new_path_tmp, new_path)
                os.remove(old_path)
            elif err.errno == errno.EEXIST:
                shutil.move(old_path, new_path)
            else:
                raise err

    def copy_from_local(self, local_path, dest, **kwargs):
        self.copy(local_path, dest, **kwargs)

    def move_from_local(self, local_path, dest, **kwargs):
        self.move(local_path, dest)

    def rename_dont_move(self, path, dest):
        """
        Rename ``path`` to ``dest``, but don't move it into the ``dest``
        folder (if it is a folder). This method is just a wrapper around the
        ``move`` method of LocalTarget.
        """
        self.move(path, dest, raise_if_exists=True)

    def open_read(self, path, mode="r"):
        file_io = io.FileIO(path, mode)
        return FileWrapper(io.BufferedReader(file_io))

    def open_write(self, path, mode="w", **kwargs):
        return AtomicLocalFile(path, fs=self, mode=mode, **kwargs)

    def move_dir(self, path, dest, raise_if_exists=False):
        if not self.exists(dest):
            self.mkdir(dest)

        elif not self.isdir(dest):
            raise NotADirectory(dest)

        for file in self.listdir(path):
            old_path = os.path.join(path, file)
            new_path = os.path.join(dest, file)
            self.move(old_path, new_path, raise_if_exists)

    def make_tmp(self):
        from tempfile import mkstemp

        _, tmp = mkstemp()
        return tmp

    def make_tmp_dir(self):
        from tempfile import mkdtemp

        return mkdtemp()
