class MultiTargetOpen(object):
    """FileInput([files[, inplace[, backup[, bufsize[, mode[, openhook]]]]]])

    Class FileInput is the implementation of the module; its methods
    filename(), lineno(), fileline(), isfirstline(), isstdin(), fileno(),
    nextfile() and close() correspond to the functions of the same name
    in the module.
    In addition it has a readline() method which returns the next
    input line, and a __getitem__() method which implements the
    sequence behavior. The sequence must be accessed in strictly
    sequential order; random access and readline() cannot be mixed.
    """

    def __init__(self, targets=None, mode="r"):

        self._targets = targets

        self._filename = None

        self._startlineno = 0
        self._filelineno = 0

        self._file_idx = 0
        self._file = None
        # restrict mode argument to reading modes
        if mode not in ("r", "rU", "U", "rb"):
            raise ValueError(
                "FileInput opening mode must be one of " "'r', 'rU', 'U' and 'rb'"
            )
        self._mode = mode

    def __del__(self):
        self.close()

    def close(self):
        try:
            self.nextfile()
        finally:
            self._file_idx = 0

    def __iter__(self):
        return self

    def __next__(self):
        while 1:
            line = self._readline()
            if line:
                self._filelineno += 1
                return line
            if not self._file:
                raise StopIteration
            self.nextfile()
            # repeat with next file

    def next(self):  # python 2.7
        return self.__next__()

    def __getitem__(self, i):
        if i != self.lineno():
            raise RuntimeError("accessing lines out of order")
        try:
            return self.next()
        except StopIteration:
            raise IndexError("end of input reached")

    def nextfile(self):
        """
        close current file, that all
        """
        file = self._file
        self._file = None
        try:
            del self._readline  # restore FileInput._readline
        except AttributeError:
            pass
        if file:
            file.close()

    def readline(self):
        while 1:
            line = self._readline()
            if line:
                self._filelineno += 1
                return line
            if not self._file:
                return line
            self.nextfile()
            # repeat with next file

    def _readline(self):
        """
        first iteration runs here
        """
        if self._file_idx >= len(self._targets):
            return None

        self._filename = self._targets[self._file_idx]
        self._file_idx += 1

        self._startlineno = self.lineno()
        self._filelineno = 0

        # This may raise IOError
        self._file = self._filename.open(self._mode)

        self._readline = self._file.readline  # hide FileInput._readline
        return self._readline()

    def filename(self):
        return self._filename

    def lineno(self):
        return self._startlineno + self._filelineno

    def filelineno(self):
        return self._filelineno

    def fileno(self):
        if self._file:
            try:
                return self._file.fileno()
            except ValueError:
                return -1
        else:
            return -1

    def isfirstline(self):
        return self._filelineno == 1

    def read(self):
        raise NotImplementedError(
            "We don't support reading 'blob' from multiple files, use readlines function"
        )

    def __enter__(self):
        return self

    def __exit__(self, *exc_info):
        self.close()

    def readlines(self):
        return [l for l in self]
