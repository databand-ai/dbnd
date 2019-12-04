import io
import os

from targets.config import get_local_tempfile


class AtomicLocalFile(io.BufferedWriter):
    """Class to create a Target that creates
    a temporary file in the local filesystem before
    moving it to its final destination.

    This class is just for the writing part of the Target. See
    :class:`targets.file.FileTarget` for example
    """

    def __init__(self, path, fs, mode="w"):
        self.__tmp_path = self.generate_tmp_path(path)
        self.path = path
        self.fs = fs
        file_io = io.FileIO(self.__tmp_path, mode)

        super(AtomicLocalFile, self).__init__(file_io)

    def close(self):
        super(AtomicLocalFile, self).close()
        self.move_to_final_destination()

    def generate_tmp_path(self, path):
        return get_local_tempfile(os.path.basename(path))

    def move_to_final_destination(self):
        self.fs.move_from_local(self.tmp_path, self.path)

    def __del__(self):
        # this is required on Windows, otherwise os.remove fails
        if not self.closed:
            super(AtomicLocalFile, self).close()
        if os.path.exists(self.tmp_path):
            os.remove(self.tmp_path)

    @property
    def tmp_path(self):
        return self.__tmp_path

    def __exit__(self, exc_type, exc, traceback):
        " Close/commit the file if there are no exception "
        if exc_type:
            return
        return super(AtomicLocalFile, self).__exit__(exc_type, exc, traceback)


class _DeleteOnCloseFile(io.FileIO):
    def close(self):
        super(_DeleteOnCloseFile, self).close()
        try:
            os.remove(self.name)
        except OSError:
            # Catch a potential threading race condition and also allow this
            # method to be called multiple times.
            pass

    def readable(self):
        return True

    def writable(self):
        return False

    def seekable(self):
        return True
