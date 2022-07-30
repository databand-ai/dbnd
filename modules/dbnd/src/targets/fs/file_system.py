# © Copyright Databand.ai, an IBM Company 2022

import abc
import errno
import logging
import os
import warnings

import six

from targets.errors import FileAlreadyExists
from targets.utils.atomic import AtomicLocalFile


logger = logging.getLogger(__name__)


@six.add_metaclass(abc.ABCMeta)
class FileSystem(object):
    """
    FileSystem abstraction used in conjunction with :py:class:`FileTarget`.

    Typically, a FileSystem is associated with instances of a :py:class:`FileTarget`. The
    instances of the py:class:`FileTarget` will delegate methods such as
    :py:meth:`FileTarget.exists` and :py:meth:`FileTarget.remove` to the FileSystem.

    Methods of FileSystem raise :py:class:`FileSystemException` if there is a problem completing the
    operation.
    """

    name = None
    support_direct_access = False
    _exist_after_write_consistent = True
    local = False

    @classmethod
    def exist_after_write_consistent(cls):
        return cls._exist_after_write_consistent

    @abc.abstractmethod
    def exists(self, path):
        """
        Return ``True`` if file or directory at ``path`` exist, ``False`` otherwise

        :param str path: a path within the FileSystem to check for existence.
        """

    @abc.abstractmethod
    def remove(self, path, recursive=True, skip_trash=True):
        """Remove file or directory at location ``path``

        :param str path: a path within the FileSystem to remove.
        :param bool recursive: if the path is a directory, recursively remove the directory and all
                               of its descendants. Defaults to ``True``.
        """

    def mkdir(self, path, parents=True, raise_if_exists=False):
        """
        Create directory at location ``path``

        Creates the directory at ``path`` and implicitly create parent
        directories if they do not already exist.

        :param str path: a path within the FileSystem to create as a directory.
        :param bool parents: Create parent directories when necessary. When
                             parents=False and the parent directory doesn't
                             exist, raise databand.target.MissingParentDirectory
        :param bool raise_if_exists: raise databand.target.FileAlreadyExists if
                                     the folder already exists.
        """
        raise NotImplementedError(
            "mkdir() not implemented on {0}".format(self.__class__.__name__)
        )

    def mkdir_parent(self, path):
        pass

    def isdir(self, path):
        """
        Return ``True`` if the location at ``path`` is a directory. If not, return ``False``.

        :param str path: a path within the FileSystem to check as a directory.

        *Note*: This method is optional, not all FileSystem subclasses implements it.
        """
        raise NotImplementedError(
            "isdir() not implemented on {0}".format(self.__class__.__name__)
        )

    def listdir(self, path):
        """Return a list of files rooted in path.

        This returns an iterable of the files rooted at ``path``. This is intended to be a
        recursive listing.

        :param str path: a path within the FileSystem to list.

        *Note*: This method is optional, not all FileSystem subclasses implements it.
        """
        raise NotImplementedError(
            "listdir() not implemented on {0}".format(self.__class__.__name__)
        )

    def move(self, path, dest):
        """
        Move a file, as one would expect.
        """
        raise NotImplementedError(
            "move() not implemented on {0}".format(self.__class__.__name__)
        )

    def rename_dont_move(self, path, dest):
        """
        Potentially rename ``path`` to ``dest``, but don't move it into the
        ``dest`` folder (if it is a folder).  This relates to :ref:`AtomicWrites`.

        This method has a reasonable but not bullet proof default
        implementation.  It will just do ``move()`` if the file doesn't
        ``exists()`` already.
        """
        warnings.warn(
            "File system {} client doesn't support atomic mv.".format(
                self.__class__.__name__
            )
        )
        if self.exists(dest):
            raise FileAlreadyExists()
        self.move(path, dest)

    def rename(self, *args, **kwargs):
        """
        Alias for ``move()``
        """
        self.move(*args, **kwargs)

    def copy(self, path, dest, **kwargs):
        """
        Copy a file or a directory with contents.
        Currently, LocalFileSystem and MockFileSystem support only single file
        copying but S3Client copies either a file or a directory as required.
        """
        raise NotImplementedError(
            "copy() not implemented on {0}".format(self.__class__.__name__)
        )

    def move_from_local(self, local_path, dest, **kwargs):
        self.copy_from_local(local_path, dest, **kwargs)
        os.remove(local_path)

    def copy_from_local(self, local_path, dest, **kwargs):
        if os.path.isdir(local_path):
            for path, subdirs, files in os.walk(local_path):
                for file in files:
                    # construct the full local path
                    local_file_path = os.path.join(local_path, path, file)
                    relative_path = os.path.relpath(local_file_path, local_path)
                    remote_path = os.path.join(dest, relative_path)
                    self.copy_from_local_file(local_file_path, remote_path, **kwargs)
        else:
            self.copy_from_local_file(local_path, dest, **kwargs)

    def copy_from_local_file(self, local_path, dest, **kwargs):
        raise NotImplementedError(
            "copy_from_local_file() not implemented on {0}".format(
                self.__class__.__name__
            )
        )

    def download(self, path, location, **kwargs):
        logger.info("download %s to %s", path, location)
        parent_location = os.path.dirname(location)

        # parent_location can be empty string if `location` has no dirname, e.g -
        # "some_file_name.txt" or "some_whole_dir"
        if parent_location and not os.path.exists(parent_location):
            try:
                os.makedirs(parent_location)
            except OSError as err:
                # somebody already created the path
                if err.errno != errno.EEXIST:
                    raise

        if self.isdir(path):
            if not os.path.exists(location):
                os.mkdir(location)
            for f in self.listdir(path):
                relative_path = os.path.relpath(f, path)
                local_path = os.path.join(location, relative_path)
                # prevent reference to the folder itself (.)
                if f != path:
                    self.download(f, local_path, **kwargs)
        else:
            self.download_file(path, location, **kwargs)

    def download_file(self, path, location, **kwargs):
        raise NotImplementedError(
            "download_file() not implemented on {0}".format(self.__class__.__name__)
        )

    def open_read(self, path, mode="r"):
        raise NotImplementedError(
            "open_read() not implemented on {0}".format(self.__class__.__name__)
        )

    def open_write(self, path, mode="w", **kwargs):
        return AtomicLocalFile(path, self, mode=mode, **kwargs)

    def move_dir(self, path, dest):
        raise NotImplementedError(
            "move_dir() not implemented on {0}".format(self.__class__.__name__)
        )

    def make_tmp(self):
        raise NotImplementedError(
            "make_tmp() not implemented on {0}".format(self.__class__.__name__)
        )

    def make_tmp_dir(self):
        raise NotImplementedError(
            "make_tmp_dir() not implemented on {0}".format(self.__class__.__name__)
        )
