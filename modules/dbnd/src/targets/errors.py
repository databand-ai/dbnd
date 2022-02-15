class TargetError(Exception):
    """
    Base exception.
    """


class FileSystemException(TargetError):
    """
    Base class for generic file system exceptions.
    """


class FileAlreadyExists(FileSystemException):
    """
    Raised when a file system operation can't be performed because
    a directory exists but is required to not exist.
    """


class FailedToCopy(FileSystemException):
    """
    Raised when a copy operation failed
    """


class MissingParentDirectory(FileSystemException):
    """
    Raised when a parent directory doesn't exist.
    (Imagine mkdir without -p)
    """


class NotADirectory(FileSystemException):
    """
    Raised when a file system operation can't be performed because
    an expected directory is actually a file.
    """


class FileNotFoundException(FileSystemException):
    pass


class NotSupportedValue(TargetError):
    pass


class InvalidDeleteException(FileSystemException):
    pass
